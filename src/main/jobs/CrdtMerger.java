package main.jobs;

import main.crdt.Crdt;
import main.utils.MessageHandler;
import main.Node;

import java.util.Map;

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
        // Only send merge for limited resource CRDT if we are not in the coordination phase or restart phase
        if (!node.isInCoordinationPhase() && !node.isInRestartPhase()) {
            String crdtString = node.getLimitedResourceCrdt().toString();
            String message = "merge:" + crdtString;
            messageHandler.broadcast(message);
        }

        // Loop over monitoring CRDTs and send merge messages for each
        for (Map.Entry<String, Crdt> entry : node.getMonotonicCrdts().entrySet()) {
            messageHandler.broadcast(generateMonotonicMergeMessage(entry));
        }
    }

    /**
     * Generate a merge message for a monotonic CRDT.
     * Structure: merge-monotonic:<id>:<type>:<crdt-state>
     */
    public static String generateMonotonicMergeMessage(Map.Entry<String, Crdt> entry) {
        String id = entry.getKey();
        Crdt crdt = entry.getValue();
        String type = crdt.getClass().getSimpleName().toLowerCase();
        return String.format("merge-monotonic:%s:%s:%s", id, type, crdt.toString());
    }
}
