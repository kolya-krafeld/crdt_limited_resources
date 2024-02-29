package main.jobs;

import main.utils.MessageHandler;
import main.Node;
import main.crdt.LimitedResourceCrdt;
import main.utils.Message;
import main.utils.MessageType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Thread responsible for processing messages from the node's message queues.
 */
public class MessageProcessor extends Thread {

    private final Node node;
    private final MessageHandler messageHandler;

    /**
     * CRDT that is only used by the leader to merge all CRDTs they get from the State calls.
     */
    private LimitedResourceCrdt leaderMergedCrdt = null;

    /**
     * Set of all nodes (ports) that we have received a state from.
     */
    private Set<Integer> statesReceivedFrom = new HashSet<>();

    /**
     * Number of accepted messages received from followers in the current coordination phase.
     */
    private int numberOfAccepted = 0;

    /**
     * Indicates which nodes have sent us the latest request for lease.
     * In one coordination phase multiple lease requests can be received.
     */
    private List<Integer> leaseRequestFrom = new ArrayList<>();

    /**
     * Timestamp when the last request for state/accept was broadcasted.
     * Used to see whether we need to wait for more messages.
     */
    private long lastRequestStateSent = 0;
    private long lastAcceptSent = 0;

    private final long messageWaitTime = 100;

    public MessageProcessor(Node node, MessageHandler messageHandler) {
        this.node = node;
        this.messageHandler = messageHandler;
    }


    /**
     * Processes messages from the coordination and operation message queues.
     * Prioritize coordination messages.
     * Don't process operation messages if we are in the coordination phase.
     */
    public void run() {
        while (true) {
            if (!node.coordiantionMessageQueue.isEmpty()) {
                matchCoordinationMessage(node.coordiantionMessageQueue.poll());

            } else if (!node.operationMessageQueue.isEmpty() && !node.isInCoordinationPhase()) {
                // Only process operation messages if we are not in the coordination phase
                matchOperationMessage(node.operationMessageQueue.poll());
            }
        }
    }

    /**
     * Matches coordination message to the appropriate method.
     */
    private void matchCoordinationMessage(Message message) {
        switch (message.getType()) {
            case REQL:
                receiveRequestLease(message);
                break;
            case REQS:
                receiveRequestState();
                break;
            case STATE:
                receiveState(message);
                break;
            case ACCEPT:
                receiveAccept(message);
                break;
            case ACCEPTED:
                receivedAccepted(message.getContent());
                break;
            case DECIDE:
                receiveDecide(message);
                break;
            default:
                System.out.println("Unknown message: " + message);
        }
    }

    /**
     * Matches operation message to the appropriate method.
     */
    private void matchOperationMessage(Message message) {
        switch (message.getType()) {
            case INC:
                node.getCrdt().increment(node.getOwnIndex());
                break;
            case DEC:
                receiveDecrement(message);
                break;
            case MERGE:
                node.mergeCrdts(message.getContent());
                break;
            default:
                System.out.println("Unknown message: " + message.getContent());
        }
    }


    // --------------------------------------------------------------------------------------
    // ----------------- COORDINATION MESSAGE HANDLING --------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Leader receives request lease from follower.
     * 1. Start coordination phase
     * 2. Send request state to all other nodes
     */
    private void receiveRequestLease(Message message) {
        if (node.isLeader()) {

            // We have been in operation phase before
            if (!node.isInCoordinationPhase()) {
                node.setInCoordinationPhase(true);
                leaderMergedCrdt = node.getCrdt(); // set leader crdt first
                leaseRequestFrom.clear();

                // Send Request State to all other nodes
                this.lastRequestStateSent = System.currentTimeMillis();
                messageHandler.broadcastWithIgnore(MessageType.REQS.getTitle(), node.getNodesPorts(), leaseRequestFrom);
            }

            leaseRequestFrom.add(message.getPort());
            // Receiving request-lease is treated like receiving a state message
            receiveState(message);
        }
    }

    /**
     * Follower receives request state from leader.
     * 1. Start coordination phase
     * 2. Send state to leader
     */
    private void receiveRequestState() {
        node.setInCoordinationPhase(true);
        String outMessage = MessageType.STATE.getTitle() + ":" + node.getCrdt().toString();
        messageHandler.sendToLeader(outMessage);
    }

    /**
     * Leader receives state message from follower.
     * 1. Merge state with leaders-merged-crdt
     *
     * If we have received state from quorum:
     * 1. Reassign leases
     * 2. Propose state to all nodes
     */
    private void receiveState(Message message) {
        if (node.isLeader()) {
            LimitedResourceCrdt state = new LimitedResourceCrdt(message.getContent());
            leaderMergedCrdt.merge(state);
            statesReceivedFrom.add(message.getPort());

            if (isReadyToProcessNextCoordinationPhase(statesReceivedFrom.size(), lastRequestStateSent)) {
                // Reassign leases
                reassignLeases();
                System.out.println("Leader proposes state: " + leaderMergedCrdt);


                // Send ACCEPT to all nodes
                this.lastAcceptSent = System.currentTimeMillis();
                String outMessage = MessageType.ACCEPT.getTitle() + ":" + leaderMergedCrdt.toString();
                messageHandler.broadcast(outMessage);

                // Reset state variables
                statesReceivedFrom.clear();
            }
        }
    }

    /**
     * Decides whether we are ready to process next coordination phase: e.g. after request state or accept.
     * Case 1: We have received STATE/ACCEPTED message from all nodes.
     * Case 2: We have received STATE/ACCEPTED message from a quorum of nodes and we have passed the wait time.
     */
    private boolean isReadyToProcessNextCoordinationPhase(int messagesReceivedFrom, long lastMessageSent) {
        // +1 is for leader
        if (messagesReceivedFrom + 1 == node.getNodesPorts().size()) {
            System.out.println("Received messages from all nodes.");
            return true;
        }
        // +1 is for leader
        if (messagesReceivedFrom + 1 >= node.getQuorumSize()
                && System.currentTimeMillis() > messageWaitTime + lastMessageSent) {
            System.out.println("Received messages from quorum of nodes after wait time had passed.");
            return true;
        }

        return false;
    }

    /**
     * Follower receives accept from leader.
     * 1. Set state as accepted state
     * 2. Send accepted to leader
     */
    private void receiveAccept(Message message) {
        node.setAcceptedCrdt(new LimitedResourceCrdt(message.getContent()));
        messageHandler.sendToLeader(MessageType.ACCEPTED.getTitle());
    }

    /**
     * Leader receives accepted from follower.
     * Wait for quorum of accepted messages.
     * 1. Send decide to all nodes
     * 2. End coordination phase
     */
    private void receivedAccepted(String messageStr) {
        numberOfAccepted++;
        if (isReadyToProcessNextCoordinationPhase(numberOfAccepted, lastAcceptSent)) {
            String message = MessageType.DECIDE.getTitle() + ":" + leaderMergedCrdt.toString();
            messageHandler.broadcast(message);

            // End coordination phase
            node.setInCoordinationPhase(false);
            numberOfAccepted = 0;
            statesReceivedFrom.clear();
        }
    }

    /**
     * Follower receives decide from leader.
     * 1. Merge state with own crdt
     */
    private void receiveDecide(Message message) {
        LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(message.getContent());

        // Check to see how many resources are left
        int assignedResourcesToMe = mergingCrdt.queryProcess(node.getOwnIndex());
        int resourcesLeftForLeader = mergingCrdt.queryProcess(node.getNodesPorts().indexOf(node.getLeaderPort()));

        if (resourcesLeftForLeader == 0 && assignedResourcesToMe == 0) {
            // Leader has no resources left. Therefore, we are out of resources now!
            node.setOutOfResources(true);
        } else if (assignedResourcesToMe == 0) {
            // No resources assigned to me. We are working on the final resources now.
            node.setFinalResources(true);
        }

        node.getCrdt().merge(mergingCrdt);
        System.out.println("Received decided state: " + message.getContent());
        node.setInCoordinationPhase(false); // End of coordination phase
    }

    /**
     * Reassigns leases to all nodes that we got a state message from.
     * We can only take leases from nodes that we have received a state from.
     *
     * BASE CASE: We have more available resources than nodes that we got a state from.
     * We can assign the resources to the nodes directly.
     *
     * CASE (less resources than nodes): We need to coordinate for every lease.
     * We assign the remaining leases to the leader now and the followers need to send a new <REQL> for each new lease
     * that they need.
     *
     * E.g. 12 resources left -> 4 resources per node
     * <10, 10, 10> -> <14, 14, 14>
     * <10, 8,  0>     <10, 10, 10>
     */
    public void reassignLeases() {

        // Only get the available resources from nodes that we got a state from. We assume that all other nodes have
        // already given away all their resources.
        int availableResources = 0;
        for (int i = 0; i < node.getNodesPorts().size(); i++) {
            // If we are the leader or we have received a state from this node
            if (i == node.getOwnIndex() || statesReceivedFrom.contains(node.getNodesPorts().get(i))) {
                availableResources += leaderMergedCrdt.queryProcess(i);
            }
        }

        System.out.println("Available resources: " + availableResources);

        // We will set the lower bound for all active processes to highest lower bound
        int highestLowerBound = leaderMergedCrdt.getLowerCounter().stream().max(Integer::compare).get();

        int amountOfStates = statesReceivedFrom.size() + 1; // +1 for the leader

        if (availableResources >= amountOfStates) {
            // BASE CASE: We have more resources than nodes. We can assign the resources to the nodes directly.

            int resourcesPerNode = availableResources / amountOfStates;
            int resourcesLeft = availableResources % amountOfStates;

            for (int i = 0; i < node.getNodesPorts().size(); i++) {
                if (i == node.getOwnIndex() || statesReceivedFrom.contains(node.getNodesPorts().get(i))) {
                    int additional = resourcesLeft > 0 ? 1 : 0; // Add one additional resource to the first nodes
                    leaderMergedCrdt.setUpper(i, highestLowerBound + resourcesPerNode + additional);
                    leaderMergedCrdt.setLower(i, highestLowerBound);
                    resourcesLeft--;
                }
            }
        } else {
            // We have less resources than nodes that we got a state from. We need to coordinate from now on for every lease.
            // We assign the remaining leases to the leader for this time.

            // Set upper and lower count to same value for all nodes that we got a state from
            int highestUpperCounter = leaderMergedCrdt.getUpperCounter().stream().max(Integer::compare).get();
            for (int i = 0; i < node.getNodesPorts().size(); i++) {
                if (i == node.getOwnIndex() || statesReceivedFrom.contains(node.getNodesPorts().get(i))) {
                    leaderMergedCrdt.setUpper(i, highestUpperCounter);
                    leaderMergedCrdt.setLower(i, highestUpperCounter);
                }
            }

            if (!node.isFinalResources()) {
                // We are just getting in the final resource mode now
                node.setFinalResources(true);

                // Assign the remaining resources to the leader
                leaderMergedCrdt.setUpper(node.getOwnIndex(), highestUpperCounter + availableResources);
            } else {
                // We are already in final resource mode. This means one node has asked for one resource. We will assign them one and the rest to the leader.
                // If we have resources left, give one to the requester and rest to the leader
                if (availableResources > 0) {
                    // Get index of first requester
                    int indexOfRequester = node.getNodesPorts().indexOf(leaseRequestFrom.get(0));
                    // Give requesting process one lease
                    leaderMergedCrdt.setUpper(indexOfRequester, highestUpperCounter + 1);
                    availableResources--;

                    // Assign the remaining resources to the leader
                    leaderMergedCrdt.setUpper(node.getOwnIndex(), highestUpperCounter + availableResources);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------
    // ----------------- OPERATION MESSAGE HANDLING --------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Receive decrement message from client.
     */
    private void receiveDecrement(Message message) {

        // Out of resources: deny request straight away.
        if (node.isOutOfResources()) {
            System.out.println("Out of resources. Cannot decrement counter.");

            // Notify client about unsuccessful decrement
            messageHandler.send(MessageType.DENYR.getTitle(), message.getPort());
            return;
        }


        int ownResourcesLeft = node.getCrdt().queryProcess(node.getOwnIndex());

        if (ownResourcesLeft == 0 && node.isFinalResources()) {
            // We are in final resource mode and have no resources assigned. We need to ask the leader for every new lease.

            node.setInCoordinationPhase(true);
            String outMessage = MessageType.REQL.getTitle() + ":" + node.getCrdt().toString();
            messageHandler.sendToLeader(outMessage);

            // Put message right back at the front of the queue, so it will be processed next.
            node.operationMessageQueue.addFirst(message);

        } else if (node.isFinalResources()) {
            // We are in final resource mode but got resources assigned. We can decrement the counter now.

            node.getCrdt().decrement(node.getOwnIndex());
            // Notify client about successful decrement
            messageHandler.send(MessageType.APPROVER.getTitle(), message.getPort());
        } else {
            // BASE CASE: we have resources assigned

            boolean successful = node.getCrdt().decrement(node.getOwnIndex());
            if (!successful) {
                System.out.println("Could not decrement counter.");
                // todo send request for lease
            } else {
                // Notify client about successful decrement
                messageHandler.send(MessageType.APPROVER.getTitle(), message.getPort());

                // Query CRDT and request leases if we have no leases left
                ownResourcesLeft = node.getCrdt().queryProcess(node.getOwnIndex());
                if (ownResourcesLeft == 0) {
                    System.out.println("No resources left.");
                    node.setInCoordinationPhase(true);
                    String outMessage = MessageType.REQL.getTitle() + ":" + node.getCrdt().toString();
                    messageHandler.sendToLeader(outMessage);
                }
            }
        }
    }

    public void setStatesReceivedFrom(Set<Integer> statesReceivedFrom) {
        this.statesReceivedFrom = statesReceivedFrom;
    }
}
