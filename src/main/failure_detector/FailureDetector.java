package main.failure_detector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import main.Config;
import main.utils.MessageHandler;
import main.utils.LogicalClock;
import main.utils.MessageType;
import main.Node;

public class FailureDetector {
    private final int nodePort;
    private final List<Integer> allNodes;
    private Node node;
    private List<Integer> suspectedNodes;
    private List<Integer> unsuspectedNodes;
    private Map<Integer, Integer> sendHeartbeatPingAtTime;
    private Map<Integer, Integer> gotHeartbeatPongAtTime;
    private MessageHandler messageHandler;
    private LogicalClock logicalClock;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService checkExecutor;
    private final int sendHeartbeatInterval;
    private final int heartbeatTimeout;

    public FailureDetector(Node node, int nodePort, List<Integer> allNodes, Config config) {
        this.node = node;
        this.nodePort = nodePort;
        this.allNodes = allNodes;
        this.suspectedNodes = new ArrayList<>(allNodes);
        this.unsuspectedNodes = new ArrayList<>();
        this.sendHeartbeatPingAtTime = new HashMap<>();
        this.gotHeartbeatPongAtTime = new HashMap<>();
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.checkExecutor = Executors.newSingleThreadScheduledExecutor();
        this.sendHeartbeatInterval = config.sendHeartbeatInterval();
        this.heartbeatTimeout = config.heartbeatTimeout();
    }


    public void start() {
        this.heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeatPing, 0, this.sendHeartbeatInterval, TimeUnit.MILLISECONDS);
        this.checkExecutor.scheduleAtFixedRate(this::checkHeartbeats, 0, this.heartbeatTimeout, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        this.heartbeatExecutor.shutdown();
        this.checkExecutor.shutdown();
    }

    private void sendHeartbeatPing() {
        for (int receiverNode : this.allNodes) {
            String message = MessageType.HEARTBEAT_PING.getTitle() + ":" + this.nodePort;
            node.messageHandler.send(message, receiverNode);
            sendHeartbeatPingAtTime.put(receiverNode, node.getTime());
        }
    }

    private void checkHeartbeats() {
        List<Integer> toBeSuspected = new ArrayList<>();
        for (int node : this.unsuspectedNodes) {
            int send = sendHeartbeatPingAtTime.getOrDefault(node, 0);
            int received = gotHeartbeatPongAtTime.getOrDefault(node, 0);
            if (received == 0 || received - send > this.heartbeatTimeout) {
                toBeSuspected.add(node);
            }
        }
        this.unsuspectedNodes.removeAll(toBeSuspected);
        this.suspectedNodes.addAll(toBeSuspected);
    }

    public void updateNodeStatus(int node) {
        this.gotHeartbeatPongAtTime.put(node, this.node.getTime());
    }

    public synchronized int numberOfConnectedNodes() {
        return this.unsuspectedNodes.size();
    }

    public synchronized boolean isConnectedToQuorum() {
        return this.unsuspectedNodes.size() > this.suspectedNodes.size();
    }


    public void sendHeartbeatPong(int port) {
        node.messageHandler.send(MessageType.HEARTBEAT_PONG.getTitle(), port);
    }
}