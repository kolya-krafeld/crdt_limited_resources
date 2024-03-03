package main.failure_detector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import main.Config;
import main.utils.Logger;
import main.utils.MessageHandler;
import main.utils.LogicalClock;
import main.utils.MessageType;
import main.Node;

public class FailureDetector extends Thread {
    private final int nodePort;
    private final Logger logger;
    private final List<Integer> allNodes;
    private Node node;
    private List<Integer> suspectedNodes;
    private List<Integer> unsuspectedNodes;
    private Map<Integer, Integer> sendHeartbeatPingAtTime;
    private Map<Integer, Integer> gotHeartbeatPongAtTime;
    private MessageHandler messageHandler;
    private LogicalClock logicalClock;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledExecutorService checkExecutor;
    private ScheduledFuture<?> checkTask;
    private final long heartbeatTimeout;
    private final int tickLength;

    public FailureDetector(Node node, int nodePort, List<Integer> allNodes, Config config) {
        this.node = node;
        this.logger = node.logger;
        this.nodePort = nodePort;
        this.allNodes = allNodes;
        this.suspectedNodes = new ArrayList<>(allNodes);
        this.unsuspectedNodes = new ArrayList<>();
        this.sendHeartbeatPingAtTime = new HashMap<>();
        this.gotHeartbeatPongAtTime = new HashMap<>();
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.checkExecutor = Executors.newSingleThreadScheduledExecutor();
        this.heartbeatTimeout = config.heartbeatTimeout();
        this.tickLength = config.tickLength();

    }


    public void run() {
        try {
            while (true) {
                sendHeartbeatRequest();
                if (node.getTime() % this.heartbeatTimeout == 0) {
                    checkHeartbeats();
                }
                Thread.sleep(tickLength);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();

        }
    }

    private void sendHeartbeatRequest() {
        for (int receiverNode : this.allNodes) {
            HeartbeatMessage heartBeatMessage = new HeartbeatMessage(this.node.isQuorumConnected(), node.ballotNumber);
            String message = MessageType.HBRequest.getTitle() + ":" + heartBeatMessage.toString();
            node.messageHandler.send(message, receiverNode);
            sendHeartbeatPingAtTime.put(receiverNode, node.getTime());
        }
    }

    private void checkHeartbeats() {
        List<Integer> toBeSuspected = new ArrayList<>();
        List<Integer> toBeUnsuspected = new ArrayList<>();
        for (int node : this.allNodes) {
            int send = sendHeartbeatPingAtTime.getOrDefault(node, -1);
            int received = gotHeartbeatPongAtTime.getOrDefault(node, -1);
            if (received < 0 || send - received > this.heartbeatTimeout) {
                toBeSuspected.add(node);
            } else {
                toBeUnsuspected.add(node);
            }
        }
        this.suspectedNodes = toBeSuspected;
        this.unsuspectedNodes = toBeUnsuspected;
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
        if(this.node.getLeaderPort()==-1 && !node.leaderElectionInProcess){
            logger.warn("Leader is set to -1, starting leader election");
            this.node.startLeaderElection();
        }

    }

    public void updateNodeStatus(int node, HeartbeatMessage message) {
        if (this.node.getLeaderPort() == node && !message.isQourumConnected && !this.node.leaderElectionInProcess) {
            logger.warn("Leader is not connected to quorum anymore, starting leader election");
            this.node.startLeaderElection();
        }
        this.gotHeartbeatPongAtTime.put(node, this.node.getTime());
    }

    //only for test purposes
    public synchronized int numberOfConnectedNodes() {
        return this.unsuspectedNodes.size();
    }

    public void sendHeartbeatReply(int port) {
        HeartbeatMessage message = new HeartbeatMessage(node.isQuorumConnected(), node.ballotNumber);
        String messageStr = MessageType.HBReply.getTitle() + ":" + message.toString();
        node.messageHandler.send(messageStr, port);
    }
}

