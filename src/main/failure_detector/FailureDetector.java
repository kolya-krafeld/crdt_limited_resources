package main.failure_detector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import main.Config;
import main.MessageHandler;
import main.utils.MessageType;

public class FailureDetector {
    private int nodePort;
    private final List<Integer> allNodes;
    private List<Integer> suspectedNodes;
    private List<Integer> unsuspectedNodes;
    private Map<Integer, Long> lastHeartbeat;
    private MessageHandler messageHandler;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService checkExecutor;
    private final int heartbeatTimeout;
    private final int sendHeartbeatInterval;

    public FailureDetector(int nodePort, List<Integer> allNodes, Config config) {
        this.nodePort = nodePort;
        this.allNodes = allNodes;
        this.suspectedNodes = new ArrayList<>(allNodes);
        this.unsuspectedNodes = new ArrayList<>();
        this.lastHeartbeat = new HashMap<>();
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.checkExecutor = Executors.newSingleThreadScheduledExecutor();
        this.heartbeatTimeout = config.heartbeatTimeout();
        this.sendHeartbeatInterval = config.sendHeartbeatInterval();
        this.messageHandler = new MessageHandler(nodePort);
        
    }

    public void start() {
        this.heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat, 0, this.sendHeartbeatInterval, TimeUnit.MILLISECONDS);
        this.checkExecutor.scheduleAtFixedRate(this::checkHeartbeats, 0, this.heartbeatTimeout, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        this.heartbeatExecutor.shutdown();
        this.checkExecutor.shutdown();
    }

    private void sendHeartbeat() {
        for (int receiverNode : this.allNodes) {
            String message = MessageType.HEARTBEAT.getTitle() + ":" + this.nodePort;
            messageHandler.send(message, receiverNode, new ConcurrentHashMap<>());
        }
    }

    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        List<Integer> toBeSuspected = new ArrayList<>();
        for (int node : this.unsuspectedNodes) {
            if (now - this.lastHeartbeat.getOrDefault(node, 0L) > this.heartbeatTimeout) {
                toBeSuspected.add(node);
            }
        }
        this.unsuspectedNodes.removeAll(toBeSuspected);
        this.suspectedNodes.addAll(toBeSuspected);
    }

    public void updateNodeStatus(int node) {
        this.lastHeartbeat.put(node, System.currentTimeMillis());
        this.suspectedNodes.remove(node);
        if (!this.unsuspectedNodes.contains(node)) {
            this.unsuspectedNodes.add(node);
        }
    }

    public synchronized int numberOfConnectedNodes() {
        return this.unsuspectedNodes.size();
    }

    public synchronized boolean isConnectedToQuorum() {
        return this.unsuspectedNodes.size() > this.suspectedNodes.size();
    }




}