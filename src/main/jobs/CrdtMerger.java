package main.jobs;

import main.utils.MessageHandler;
import main.Node;

/**
 * Thread responsible for broadcasting our current CRDT state to all nodes in the network.
 */
public class CrdtMerger implements Runnable {

    private Node node;
    private MessageHandler messageHandler;

    public CrdtMerger(Node node, MessageHandler messageHandler) {
        this.node = node;
        this.messageHandler = messageHandler;
    }

    public void run() {
        if (!node.isInCoordinationPhase()) {
            System.out.println("Broadcasting merge.");
            String crdtString = node.getCrdt().toString();
            String message = "merge:" + crdtString;
            messageHandler.broadcast(message);
        }
    }
}
