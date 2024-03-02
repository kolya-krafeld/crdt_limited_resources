package main;

import main.crdt.LimitedResourceCrdt;
import main.jobs.CrdtMerger;
import main.jobs.MessageProcessor;
import main.jobs.MessageReceiver;
import main.utils.*;


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

    public Logger logger = new Logger(Logger.LogLevel.DEBUG, this);

    /**
     * Persists state of node to local file, to save state after a restart.
     */
    public Persister persister;

    /**
     * UDP socket for receiving and sending messages.
     */
    public DatagramSocket socket;

    /**
     * Utils object that handels sending messages.
     */
    private final MessageHandler messageHandler;

    /**
     * Flag to indicate if the node is currently in lease coordination phase.
     */
    boolean inCoordinationPhase = false;

    /**
     * Flag to indicate if the node is currently in restart phase. To get out of this phase, the node needs to receive a <decide> or <accept-sync> message.
     */
    boolean inRestartPhase = false;

    /**
     * Is set when we have less resources than processes, so every lease needs a coordination phase.
     */
    boolean finalResources = false;

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

    private int leaderPort = -1;

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

    /**
     * Round number is the number of the current/latest coordination round.
     */
    private int roundNumber = 0;

    /**
     * Round number of the last decided coordination phase.
     */
    private int lastDecideRoundNumber = 0;

    /**
     * Flag used for testing. If set to true, the node will add a delay to every message it sends.
     */
    private boolean addMessageDelay = false;

    // JOBS
    private MessageReceiver messageReceiver;
    private MessageProcessor messageProcessor;
    private CrdtMerger crdtMerger;
    private ScheduledExecutorService executor;

    public Node(int port, List<Integer> nodesPorts) {
        this.ownPort = port;
        this.nodesPorts = nodesPorts;
        this.crdt = new LimitedResourceCrdt(nodesPorts.size());
        this.persister = new Persister(this);

        this.quorumSize = (nodesPorts.size() / 2) + 1;

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
            logger.warn("Could not open socket!");
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts the node.
     */
    public void init() {
        // Persist state at beginning
        persister.persistState(false, 0, crdt, Optional.empty());

        messageReceiver = new MessageReceiver(this);
        messageReceiver.start();

        messageProcessor = new MessageProcessor(this, messageHandler);
        messageProcessor.start();

        crdtMerger = new CrdtMerger(this, messageHandler);

        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(crdtMerger, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Kills all node processes.
     */
    public void kill() {
        logger.warn("Killed!");

        messageReceiver.interrupt();
        messageReceiver.stopReceiver();
        messageProcessor.interrupt();
        crdtMerger.interrupt();
        executor.shutdown();

        socket.close();

    }

    /**
     * Restarts the node after it failed.
     */
    public void restart() {
        logger.info("Restarted!");
        inRestartPhase = true;
        // Reload state
        persister.loadState();

        // Open up socket again
        try {
            socket = new DatagramSocket(ownPort);
            messageHandler.setSocket(socket);
        } catch (Exception e) {
            logger.warn("Could not open socket!");
            throw new RuntimeException(e);
        }

        // Start all jobs again
        init();

        // Send message to all nodes that we are back. Only the leader will respond.
        // Either with a <decide> if we are still in the same coordination phase as before or with <accept-sync> otherwise.
        // Resend the message until we leave restart phase. This is required, in case the leader fails between receiving and processing message.
        while (inRestartPhase) {
            messageHandler.broadcast(MessageType.REQUEST_SYNC.getTitle() + ":" + roundNumber);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * Deserializes the CRDT from the message and merges it with the current CRDT.
     */
    public void mergeCrdts(String crdtString) {
        LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(crdtString);
        crdt.merge(mergingCrdt);
        logger.debug("Merged crdt: " + crdt);
    }

    // GETTERS & SETTERS

    public LimitedResourceCrdt getCrdt() {
        return crdt;
    }

    public void setCrdt(LimitedResourceCrdt crdt) {
        this.crdt = crdt;
    }

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

    public boolean isInRestartPhase() {
        return inRestartPhase;
    }

    public int getRoundNumber() {
        return roundNumber;
    }

    public void setRoundNumber(int roundNumber) {
        this.roundNumber = roundNumber;
    }

    public void setInRestartPhase(boolean inRestartPhase) {
        this.inRestartPhase = inRestartPhase;
    }

    public int getLastDecideRoundNumber() {
        return lastDecideRoundNumber;
    }

    public void setLastDecideRoundNumber(int lastDecideRoundNumber) {
        this.lastDecideRoundNumber = lastDecideRoundNumber;
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

    public void setAddMessageDelay(boolean addMessageDelay) {
        this.addMessageDelay = addMessageDelay;
    }

    public boolean isAddMessageDelay() {
        return addMessageDelay;
    }
}
