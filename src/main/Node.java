package main;

import main.ballot_leader_election.BallotLeaderElection;
import main.crdt.Crdt;
import main.crdt.LimitedResourceCrdt;
import main.crdt.ORSet;
import main.crdt.PNCounter;
import main.jobs.CrdtMerger;
import main.jobs.MessageProcessor;
import main.jobs.MessageReceiver;
import main.utils.*;

import main.failure_detector.FailureDetector;
import main.utils.Message;
import main.utils.LogicalClock;
import main.utils.MessageType;

import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.*;


/**
 * Node in the network. Connects to other nodes and clients.
 * Responsible for its own CRDT.
 */
public class Node {

    public Logger logger = new Logger(Logger.LogLevel.ERROR, this);

    /**
     * Flag to indicate if the node should coordinate for every resource. Used for benchmarking.
     */
    private boolean coordinateForEveryResource = false;

    /**
     * CRDT that only allows access to limited ressources.
     */
    private LimitedResourceCrdt limitedResourceCrdt;

    /**
     * Map of all monotonic CRDTs. Maps a unique identifier to a CRDT.
     */
    private Map<String, Crdt> monotonicCrdts = new HashMap<>();

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
    public final MessageHandler messageHandler;
    /**
     * Flag to indicate if the node is currently in lease coordination phase.
     */
    boolean inCoordinationPhase = false;

    /**
     * Flag to indicate if the node is currently in restart phase. To get out of this phase, the node needs to receive a <decide> or <accept-sync> message.
     */
    boolean inRestartPhase = false;

    /**
     * Flag to indicate that a new leader is in prepare phase.
     */
    boolean inPreparePhase = false;

    /**
     * Is set when we have less resources than processes, so every lease needs a coordination phase.
     */
    boolean finalResources = false;

    /**
     * Is set when our node and the leader node are out of resources. This is used to deny resources to clients directly.
     */
    boolean outOfResources = false;

    /**
     *Is set when node was leader and missed the election and itself is not the leader anymore.
     */
    public boolean isSearchingForLeader = false;

    /**
     * Queue of merge messages for monotonic CRDTs.
     */
    public LinkedBlockingDeque<Message> monotonicCrdtMessageQueue = new LinkedBlockingDeque<>();

    /**
     * Queue of messages to be processed outside of coordination phase. Using concurrent queue to make it thread safe.
     */
    public LinkedBlockingDeque<Message> operationMessageQueue = new LinkedBlockingDeque<>();

    /**
     * Queue of messages to be processed. Using concurrent queue to make it thread safe.
     */
    public Queue<Message> coordinationMessageQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue of messages prepare phase messages to be processed. Used only by a new leader that was freshly elected.
     */
    public Queue<Message> preparePhaseMessageQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue of messages with topics heartbeat and leader election to be processed.
     * Using concurrent queue to make it thread safe.
     */
    public Queue<Message> heartbeatAndElectionMessageQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue of messages with topics find leader to be processed.
     */
    public Queue<Message> findLeaderMessageQueue = new ConcurrentLinkedQueue<>();

    /**
     * Stores the answers of the find leader messages in the form of <leader port, Integer[votes, leaderBallotNumber]>
     * When the majority of the nodes have voted for a leader, the node knows again which node is the leader
     */
    public Map<Integer, Integer[]> findLeaderAnswers = new HashMap<>();

    private int ownPort;

    private int leaderPort = -1;
    public int leaderBallotNumber = 0;
    /**
     * Own ballot number of the node
     */
    public int ballotNumber = 0;
    public boolean leaderElectionInProcess=false;

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
     * Flag to indicate if the node is connected to a majority of nodes.
     */
    private boolean isQuorumConnected = true;

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

    private Config config;
    private ScheduledExecutorService clockExecutor;
    public FailureDetector failureDetector;
    public LogicalClock logicalClock;
    public BallotLeaderElection ballotLeaderElection;

    public Node(int port, List<Integer> nodesPorts, Config config) {
        this.ownPort = port;
        this.nodesPorts = nodesPorts;
        this.limitedResourceCrdt = new LimitedResourceCrdt(nodesPorts.size());
        this.persister = new Persister(this);

        this.quorumSize = (nodesPorts.size() / 2) + 1;

        this.config = config;
        this.logicalClock = new LogicalClock();
        this.clockExecutor = Executors.newSingleThreadScheduledExecutor();
        this.failureDetector = new FailureDetector(this, nodesPorts, config);
        this.ballotLeaderElection = new BallotLeaderElection(this, config.electionTimeout());

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
    public void init(boolean startFailureDetector) {
        // Persist state at beginning
        persister.persistState(false, 0, limitedResourceCrdt, Optional.empty(), this.leaderBallotNumber);

        ScheduledExecutorService clockExecutor = Executors.newScheduledThreadPool(1);
        clockExecutor.scheduleAtFixedRate(() -> logicalClock.tick(), 0, config.tickLength(), TimeUnit.MILLISECONDS);
        if (startFailureDetector) {
            failureDetector.start();
        }

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

        failureDetector.interrupt();
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

        // Start all jobs again, except failure detector
        init(false);

        // Send message to all nodes that we are back. Only the leader will respond.
        // Either with a <decide> if we are still in the same coordination phase as before or with <accept-sync> otherwise.
        // Resend the message until we leave restart phase. This is required, in case the leader fails between receiving and processing message.
        while (inRestartPhase) {
            messageHandler.broadcast(MessageType.REQUEST_SYNC.getTitle() + ":" + roundNumber);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        failureDetector.start();
    }

    public synchronized int getTime() {
        return this.logicalClock.getTime();
    }

    public void startLeaderElection() {
        //maybe we have to set inCoordinationPhase = true, but as leaderElection happens on another layer, it should work
        this.ballotLeaderElection.start();
    }

    /**
     * Deserializes the CRDT from the message and merges it with the current CRDT.
     */
    public void mergeCrdts(String crdtString) {
        LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(crdtString);
        limitedResourceCrdt.merge(mergingCrdt);
        logger.debug("Merged crdt: " + limitedResourceCrdt);
    }

    /**
     * Add a new positive-negative counter CRDT to the node.
     */
    public void addPNCounterCrdt(String identifier) {
        Crdt pnCounter = new PNCounter(this.nodesPorts.size());
        monotonicCrdts.put(identifier, pnCounter);
    }

    /**
     * Add a new ORSet CRDT to the node.
     */
    public void addORSetCrdt(String identifier) {
        Crdt orSet = new ORSet();
        monotonicCrdts.put(identifier, orSet);
    }

    // GETTERS & SETTERS

    public LimitedResourceCrdt getLimitedResourceCrdt() {
        return limitedResourceCrdt;
    }

    public void setLimitedResourceCrdt(LimitedResourceCrdt limitedResourceCrdt) {
        this.limitedResourceCrdt = limitedResourceCrdt;
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

    public boolean isQuorumConnected() {
        return this.isQuorumConnected;
    }

    public void setQuorumConnected(boolean quorumConnected) {
        this.isQuorumConnected = quorumConnected;
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

    public boolean isInPreparePhase() {
        return inPreparePhase;
    }

    public void setInPreparePhase(boolean inPreparePhase) {
        this.inPreparePhase = inPreparePhase;
    }

    public Map<String, Crdt> getMonotonicCrdts() {
        return monotonicCrdts;
    }

    public boolean isCoordinateForEveryResource() {
        return coordinateForEveryResource;
    }

    public void setCoordinateForEveryResource(boolean coordinateForEveryResource) {
        this.coordinateForEveryResource = coordinateForEveryResource;
    }
}
