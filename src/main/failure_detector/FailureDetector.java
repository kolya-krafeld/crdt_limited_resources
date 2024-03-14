package main.failure_detector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.Config;
import main.utils.Logger;
import main.utils.MessageType;
import main.Node;

public class FailureDetector extends Thread {
    private final Logger logger;
    private final List<Integer> allNodes;
    private final Node node;
    /**
     * List of nodes that are suspected to be dead.
     */
    private List<Integer> suspectedNodes;
    /**
     * List of nodes that are not suspected to be dead.
     */
    private List<Integer> unsuspectedNodes;
    /**
     * Map of nodes and the time a heartbeat ping was sent to them.
     */
    private Map<Integer, Integer> sendHeartbeatPingAtTime;
    /**
     * Map of nodes and the time they sent a heartbeat pong.
     */
    private Map<Integer, Integer> gotHeartbeatPongAtTime;
    /**
     * Time in ticks that a node has to respond to a heartbeat ping.
     */
    private final long heartbeatTimeout;
    private final int tickLength;

    /**
     * Flag whether failure detector should currently be active
     */
    private boolean active = true;

    /**
     * Thread responsible for detecting node failures and starting leader election.
     * Leader elections are also happening in this thread, as they are started from here.
     * When a node receives a leader election message, the failure detector keeps running, but it won't start a new leader election, for example if it also detects that the leader is dead.
     * It can still happen that nodes start the leader election concurrently if nodes suspect the leader being dead in a short period of time.
     */
    public FailureDetector(Node node, List<Integer> allNodes, Config config) {
        this.node = node;
        this.logger = node.logger;
        this.allNodes = allNodes;
        this.suspectedNodes = new ArrayList<>(allNodes);
        this.unsuspectedNodes = new ArrayList<>();
        this.sendHeartbeatPingAtTime = new HashMap<>();
        this.gotHeartbeatPongAtTime = new HashMap<>();
        this.heartbeatTimeout = config.heartbeatTimeout();
        this.tickLength = config.tickLength();

    }

    /**
     * Sends a heartbeat request to allNodes every tick and puts responses in the gotHeartbeatPongAtTime map.
     * Checks every heartbeatTimeout ticks if a node has not responded to a heartbeat request. If this is the case, the node is suspected to be dead.
     * If the leader is suspected to be dead, a leader election is started.
     * If the leader is set to -1, a leader election is started. (usually after the startup)
     * If the leader is not connected to the quorum, a leader election is started.
     */
    public void run() {
        try {
            while (true) {
                if (active) {
                    //logger.info("Checking heartbeats...");
                    sendHeartbeatRequest();
                    if (node.getTime() % this.heartbeatTimeout == 0) {
                        checkHeartbeats();
                    }
                    Thread.sleep(tickLength);
                } else {
                    synchronized (node.fdLock) {
                        node.fdLock.wait();
                    }
                }
            }

        } catch (InterruptedException e) {
            logger.info("FailureDetector was interrupted!");
        }
    }

    /**
     * Sends a heartbeat request to all nodes.
     * The heartbeat request contains the current ballot number and if the node is connected to the quorum.
     * The time the heartbeat request was sent is put in the sendHeartbeatPingAtTime map, together with the time it was sent.
     */
    private void sendHeartbeatRequest() {
        for (int receiverNode : this.allNodes) {
            if (receiverNode == this.node.getOwnPort()) {
                // Don't send a heartbeat to yourself
                continue;
            }

            HeartbeatMessage heartBeatMessage = new HeartbeatMessage(this.node.isQuorumConnected(), node.ballotNumber);
            String message = MessageType.HB_REQUEST.getTitle() + ":" + heartBeatMessage;
            node.messageHandler.send(message, receiverNode);
            sendHeartbeatPingAtTime.put(receiverNode, node.getTime());
        }
    }

    /**
     * Checks if the timestamps of each node in sendHeartbeatPingAtTime and gotHeartbeatPongAtTime are below the heartbeatTimeout.
     * Starts leader election if the leader is suspected to be dead or the leader is set to -1.
     */
    private void checkHeartbeats() {
        List<Integer> toBeSuspected = new ArrayList<>();
        List<Integer> toBeUnsuspected = new ArrayList<>();

        for (int node : this.allNodes) {
            if (node == this.node.getOwnPort()) {
                // Don't check yourself
                continue;
            }

            int send = sendHeartbeatPingAtTime.getOrDefault(node, -1);
            int received = gotHeartbeatPongAtTime.getOrDefault(node, -1);
            if (received < 0 || send - received > this.heartbeatTimeout) {
                logger.debug(this.node.getOwnPort() + ": " + node + " is suspected to be dead! Send: " + send + " Received: " + received);
                toBeSuspected.add(node);
            } else {
                toBeUnsuspected.add(node);
            }
        }
        this.suspectedNodes = toBeSuspected;
        this.unsuspectedNodes = toBeUnsuspected;
        unsuspectedNodes.add(node.getOwnPort()); // Add yourself to the unsuspected nodes
        if (this.unsuspectedNodes.size() >= this.node.getQuorumSize()) {
            this.node.setQuorumConnected(true);
        } else {
            logger.warn("Not connected to quorum");
            this.node.setQuorumConnected(false);
        }

        if (this.suspectedNodes.contains(this.node.getLeaderPort()) && !node.leaderElectionInProcess) {
            logger.warn("Leader is suspected to be dead, starting leader election");
            this.node.startLeaderElection();
        }

        if (this.node.getLeaderPort() == -1 && !node.leaderElectionInProcess) {
            logger.warn("Leader is set to -1, starting leader election");
            this.node.startLeaderElection();
        }

    }

    /**
     * Handles the HB_REPLY message by putting it in the gotHeartbeatPongAtTime map, together with the current time
     * If the leader is not connected to the quorum anymore, a leader election is started.
     * If the node itself is the leader, but with a lower ballot number, than the node missed a leader election, and it asks for the new leader
     */
    public void updateNodeStatus(int node, HeartbeatMessage message) {
        if (this.node.getLeaderPort() == node && !message.isQourumConnected && !this.node.leaderElectionInProcess) {
            logger.warn("Leader is not connected to quorum anymore, starting leader election");
            this.node.startLeaderElection();
        } else if (this.node.isLeader() && message.ballotNumber > this.node.ballotNumber && !this.node.isSearchingForLeader) {
            logger.warn("Hasn't updated to the new leader, starting search for new leader");
            this.node.isSearchingForLeader = true;
            findNewLeader();
        }
        addTimeWhenWeReceivedLastMessageFromNode(node);
    }

    public void addTimeWhenWeReceivedLastMessageFromNode(int nodePort) {
        this.gotHeartbeatPongAtTime.put(nodePort, this.node.getTime());
    }

    /*
    * Asks nodes for the leader by sending a FIND_LEADER_REQUEST to all nodes.
    * Replies to that message are put in the findLeaderAnswers map.
    * If a node has more than a quorum of confirmations being the leader, the outdated node updates its leader and leaderBallotNumber.
     */
    private void findNewLeader() {
        node.messageHandler.broadcast(MessageType.FIND_LEADER_REQUEST.getTitle());
        while (true) {
            for (int node : this.allNodes) {
                if (this.node.findLeaderAnswers.containsKey(node)) {
                    int votesForLeader = this.node.findLeaderAnswers.get(node)[0];
                    int leaderBallotNumber = this.node.findLeaderAnswers.get(node)[1];
                    if (votesForLeader >= this.node.getQuorumSize()) {
                        this.node.leaderBallotNumber = leaderBallotNumber;
                        this.node.setLeaderPort(node);
                        this.node.isSearchingForLeader = false;
                        return;
                    }
                }
            }
        }
    }

    public void sendHeartbeatReply(int port) {
        HeartbeatMessage message = new HeartbeatMessage(node.isQuorumConnected(), node.ballotNumber);
        String messageStr = MessageType.HB_REPLY.getTitle() + ":" + message;
        node.messageHandler.send(messageStr, port);
    }

    //only for test purposes
    public synchronized int numberOfConnectedNodes() {
        return this.unsuspectedNodes.size();
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}

