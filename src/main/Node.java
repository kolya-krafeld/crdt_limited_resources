package main;

import com.sun.net.httpserver.Authenticator;
import main.crdt.LimitedResourceCrdt;
import main.jobs.CrdtMerger;
import main.jobs.MessageProcessor;
import main.jobs.MessageReceiver;
import main.utils.Message;
import main.utils.MessageHandler;

import main.failure_detector.FailureDetector;
import main.utils.Message;
import main.utils.LogicalClock;
import main.utils.MessageType;

import javax.swing.text.html.Option;
import java.net.DatagramSocket;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.*;


/**
 * Node in the network. Connects to other nodes and clients.
 * Responsible for its own CRDT.
 */
public class Node {

    /**
     * UDP socket for receiving and sending messages.
     */
    public DatagramSocket socket;

    /**
     * Utils object that handels sending messages.
     */
    public final MessageHandler messageHandler;
    /**
     * Flag to indicate if the node is currently in lease coordination phase.
     */
    boolean inCoordinationPhase = false;

    /**
     * Is set when we have less resources than processes, so every lease needs a coordination phase.
     */
    boolean finalResources = false; //todo might not be needed

    boolean outOfResources = false;

    /**
     * Queue of messages to be processed outside of coordination phase. Using concurrent queue to make it thread safe.
     */
    public LinkedBlockingDeque<Message> operationMessageQueue = new LinkedBlockingDeque<>();

    /**
     * Queue of messages to be processed. Using concurrent queue to make it thread safe.
     */
    public Queue<Message> coordiantionMessageQueue = new ConcurrentLinkedQueue<>();


    private int ownPort;
    private int leaderPort;

    /**
     * Index of the node in the network.
     */
    private int ownIndex;
    /**
     * List of all ports of the nodes in the network.
     */
    private List<Integer> nodesPorts;

    /**
     * Size of a quorum with the current amount of nodes.
     */
    private int quorumSize;

    /**
     * CRDT that only allows access to limited ressources.
     */
    private LimitedResourceCrdt crdt;

    /**
     * CRDT that was accepted in the last coordination phase.
     */
    private Optional<LimitedResourceCrdt> acceptedCrdt = Optional.empty();

    

    private Config config;
    private ScheduledExecutorService clockExecutor;
    private FailureDetector failureDetector;
    public LogicalClock logicalClock;

    public Node(int port, List<Integer> nodesPorts, Config config) {
        this.ownPort = port;
        this.nodesPorts = nodesPorts;
        this.crdt = new LimitedResourceCrdt(nodesPorts.size());

        this.quorumSize = (nodesPorts.size() / 2) + 1;

        this.config = config;
        this.logicalClock = new LogicalClock();
        this.clockExecutor = Executors.newSingleThreadScheduledExecutor();
        this.failureDetector = new FailureDetector(this, this.ownPort, nodesPorts, config);

        // Get own index in port list
        int ownIndex = nodesPorts.indexOf(port);
        if (ownIndex == -1) {
            throw new IllegalArgumentException("Port not in list of nodes.");
        }
        this.ownIndex = ownIndex;

        // Open UDP port
        try {
            socket = new DatagramSocket(port);
            this.messageHandler = new MessageHandler(this, socket, port);
        } catch (Exception e) {
            System.out.println("Could not open socket!");
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts the node.
     */
    public void init() {

        ScheduledExecutorService clockExecutor = Executors.newScheduledThreadPool(1);
        clockExecutor.scheduleAtFixedRate(() -> logicalClock.tick(), 0, config.tick(), TimeUnit.MILLISECONDS);
        failureDetector.start();
        
        MessageReceiver messageReceiver = new MessageReceiver(this);
        messageReceiver.start();

        MessageProcessor messageProcessor = new MessageProcessor(this, messageHandler);
        messageProcessor.start();

        CrdtMerger merger = new CrdtMerger(this, messageHandler);

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(merger, 10, 10, TimeUnit.SECONDS);
    }

    //TODO: entfernen: nur zu testzwecken
    public int numberOfConnectedNodes() {
        return this.failureDetector.numberOfConnectedNodes();
    }

    //TODO: entfernen: nur zu testzwecken
    public boolean isConnectedToQuorum() {
        return this.failureDetector.isConnectedToQuorum();
    }


    public synchronized int getTime() {
        return this.logicalClock.getTime();
    }
    /**
     * Deserializes the CRDT from the message and merges it with the current CRDT.
     */
    public void mergeCrdts(String crdtString) {
        LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(crdtString);
        crdt.merge(mergingCrdt);
        System.out.println("Merged crdt: " + crdt);
    }


    public LimitedResourceCrdt getCrdt() {
        return crdt;
    }

    // GETTERS & SETTERS

    public boolean isLeader() {
        return ownPort == leaderPort;
    }

    public void setLeaderPort(int leaderPort) {
        this.leaderPort = leaderPort;
    }

    public void setInCoordinationPhase(boolean inCoordinationPhase) {
        this.inCoordinationPhase = inCoordinationPhase;
    }

    public boolean isInCoordinationPhase() {
        return inCoordinationPhase;
    }

    public boolean isFinalResources() {
        return finalResources;
    }

    public int getOwnPort() {
        return ownPort;
    }

    public List<Integer> getNodesPorts() {
        return nodesPorts;
    }

    public int getLeaderPort() {
        return leaderPort;
    }

    public int getOwnIndex() {
        return ownIndex;
    }

    public void setOutOfResources(boolean outOfResources) {
        this.outOfResources = outOfResources;
    }

    public void setFinalResources(boolean finalResources) {
        this.finalResources = finalResources;
    }

    public boolean isOutOfResources() {
        return outOfResources;
    }

    public Optional<LimitedResourceCrdt> getAcceptedCrdt() {
        return acceptedCrdt;
    }

    public void setAcceptedCrdt(LimitedResourceCrdt acceptedCrdt) {
        if (acceptedCrdt == null) {
            this.acceptedCrdt = Optional.empty();
            return;
        }

        this.acceptedCrdt = Optional.of(acceptedCrdt);
    }

    public int getQuorumSize() {
        return quorumSize;
    }
}