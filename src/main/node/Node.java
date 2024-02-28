package node;

import java.util.List;

import config.Config;
import failure_detector.FailureDetector;



public class Node {
    private FailureDetector failureDetector;
    private final int id;

    public Node(int id, List<Node> allNodes, Config config) {
        this.id = id;
        this.failureDetector = new FailureDetector(this, allNodes, config);
    }

    public void startFailureDetector() {
        this.failureDetector.start();
    }

    public void stopFailureDetector() {
        this.failureDetector.stop();
    }

    public void receiveHeartbeat(Node node) {
        System.out.println("Node " + this.id + " received heartbeat from " + node.id);
        this.failureDetector.updateNodeStatus(node);
    }

    public int getId() {
        return this.id;
    }

    //TODO: entfernen: nur zu testzwecken
    public int numberOfConnectedNodes() {
        return this.failureDetector.numberOfConnectedNodes();
    }

    //TODO: entfernen: nur zu testzwecken
    public boolean isConnectedToQuorum() {
        return this.failureDetector.isConnectedToQuorum();
    }
}



