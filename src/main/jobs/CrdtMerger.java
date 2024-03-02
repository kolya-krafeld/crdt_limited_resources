package main.jobs;

import main.utils.MessageHandler;
import main.Node;

/**
 * Thread responsible for broadcasting our current CRDT state to all nodes in the network.
 */
public class CrdtMerger extends Thread {

    private Node node;
    private MessageHandler messageHandler;

    public CrdtMerger(Node node, MessageHandler messageHandler) {
        this.node = node;
        this.messageHandler = messageHandler;
    }

    public void run() {
        if (!node.isInCoordinationPhase() && !node.isInRestartPhase()) {
            String crdtString = node.getCrdt().toString();
            String message = "merge:" + crdtString;
            messageHandler.broadcast(message);
        }
    }
}
