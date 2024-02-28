package failure_detector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import config.Config;
import node.Node;

public class FailureDetector {
    private Node ownerNode;
    private final List<Node> allNodes;
    private List<Node> suspectedNodes;
    private List<Node> unsuspectedNodes;
    private Map<Node, Long> lastHeartbeat;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService checkExecutor;
    private final int heartbeatTimeout;
    private final int sendHeartbeatInterval;

    public FailureDetector(Node node, List<Node> allNodes, Config config) {
        this.ownerNode = node;
        this.allNodes = allNodes;
        this.suspectedNodes = new ArrayList<>(allNodes);
        this.unsuspectedNodes = new ArrayList<>();
        this.lastHeartbeat = new HashMap<>();
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.checkExecutor = Executors.newSingleThreadScheduledExecutor();
        this.heartbeatTimeout = config.heartbeatTimeout();
        this.sendHeartbeatInterval = config.sendHeartbeatInterval();
        
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
        for (Node receiverNode : this.allNodes) {
            receiverNode.receiveHeartbeat(this.ownerNode);
        }
    }

    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        List<Node> toBeSuspected = new ArrayList<>();
        for (Node node : this.unsuspectedNodes) {
            if (now - this.lastHeartbeat.getOrDefault(node, 0L) > this.heartbeatTimeout) {
                toBeSuspected.add(node);
            }
        }
        this.unsuspectedNodes.removeAll(toBeSuspected);
        this.suspectedNodes.addAll(toBeSuspected);
    }

    public void updateNodeStatus(Node node) {
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