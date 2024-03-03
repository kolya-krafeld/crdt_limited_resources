package main.jobs;

import main.Node;
import main.ballot_leader_election.ElectionReplyMessage;
import main.crdt.LimitedResourceCrdt;
import main.failure_detector.HeartbeatMessage;
import main.utils.*;

import java.util.*;


/**
 * Functional interface for triggering a function after a certain time.
 */
@FunctionalInterface
interface Function {
    void apply();
}


/**
 * Thread responsible for processing messages from the node's message queues.
 */
public class MessageProcessor extends Thread {

    private final Logger logger;

    private final Node node;
    private final MessageHandler messageHandler;
    private final Persister persister;

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

    /**
     * Round when the last accepted/decide was send. Prevents sending accepted messages multiple times in one round.
     */
    private int lastAcceptRoundSend = 0;
    private int lastDecideRoundSend = 0;

    private final long messageWaitTime = 100;

    public MessageProcessor(Node node, MessageHandler messageHandler) {
        this.node = node;
        this.logger = node.logger;
        this.messageHandler = messageHandler;
        this.persister = node.persister;
    }


    /**
     * Processes messages from the coordination and operation message queues.
     * Prioritize coordination messages.
     * Don't process operation messages if we are in the coordination phase.
     */
    public void run() {
        while (true) {
            if (!node.heartbeatAndElectionMessageQueue.isEmpty()) {
                matchHeartbeatAndElectionMessage(node.heartbeatAndElectionMessageQueue.poll());
            }
            if (!node.coordiantionMessageQueue.isEmpty()) {
                matchCoordinationMessage(node.coordiantionMessageQueue.poll());

            } else if (!node.operationMessageQueue.isEmpty() && !node.isInCoordinationPhase() && !node.isInRestartPhase()) {
                // Only process operation messages if we are not in the coordination or restart phase
                matchOperationMessage(node.operationMessageQueue.poll());
            }
        }
    }

    /**
     * Matches coordination message to the appropriate method.
     */
    private void matchCoordinationMessage(Message message) {
        if (message == null) {
            // We need to filter out null messages, because we can get them during node failures
            return;
        }
        switch (message.getType()) {
            case REQL:
                receiveRequestLease(message);
                break;
            case REQS:
                receiveRequestState(message);
                break;
            case STATE:
                receiveState(message);
                break;
            case ACCEPT:
                receiveAccept(message);
                break;
            case ACCEPTED:
                receivedAccepted(message);
                break;
            case DECIDE:
                receiveDecide(message);
                break;
            case REQUEST_SYNC:
                receiveRequestSync(message);
                break;
            case ACCEPT_SYNC:
                receiveAcceptSync(message);
                break;
            default:
                logger.warn("Unknown message: " + message);
        }
    }

    /**
     * Matches operation message to the appropriate method.
     */
    private void matchOperationMessage(Message message) {
        if (message == null) {
            // We need to filter out null messages, because we can get them during node failures
            return;
        }
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
                logger.warn("Unknown message: " + message);
        }
    }

    void matchHeartbeatAndElectionMessage(Message message) {
        switch (message.getType()) {
            case HBRequest:
                node.failureDetector.sendHeartbeatReply(message.getPort());
                break;
            case HBReply:
                node.failureDetector.updateNodeStatus(message.getPort(), HeartbeatMessage.fromString(message.getContent()));
                break;
            case ELECTIONREQUEST:
                handleElectionRequest(message);
                break;
            case ELECTIONREPLY:
                node.ballotLeaderElection.handleElectionReply(message);
                break;
            case ELECTIONRESULT:
                handleElectionResult(message.getContent());
                break;
            default:
                logger.info("Unknown message from :"+message.getPort()+" : Type: " + message.getType() + " Content: " + message.getContent());
        }
    }




    // --------------------------------------------------------------------------------------
    // ----------------- COORDINATION MESSAGE HANDLING --------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Leader receives request-sync from a follower that has just restarted.
     */
    private void receiveRequestSync(Message message) {
        if (node.isLeader()) {

            int followerRoundNumber = Integer.parseInt(message.getContent());
            if (node.isInCoordinationPhase() && followerRoundNumber == node.getRoundNumber()) {
                // We are still in the same coordination phase as before. We can ignore this. At the end of the phase,
                // we will send a DECIDE message to all processes. And the follower will receive it and end the restart phase.
            } else {
                // We are not in the same coordination phase as before. Send accept-sync to follower.
                String messageStr = MessageType.ACCEPT_SYNC.getTitle() + ":" + node.getLastDecideRoundNumber() + ":" + node.getCrdt().toString();
                messageHandler.send(messageStr, message.getPort());
            }
        }
    }

    /**
     * Receive accept-sync from leader. Format of message: <accept-sync>:<round-number>:<crdt>
     */
    private void receiveAcceptSync(Message message) {
        if (!node.isLeader()) {

            String[] messageParts = message.getContent().split(":");
            int leaderRoundNumber = Integer.parseInt(messageParts[0]);
            String crdtString = messageParts[1];
            node.mergeCrdts(crdtString);
            node.setRoundNumber(leaderRoundNumber);
            node.setLastDecideRoundNumber(leaderRoundNumber);

            // Persist newly loaded state
            persister.persistState(false, node.getLastDecideRoundNumber(), node.getCrdt(), Optional.empty(), node.leaderBallotNumber);

            node.setLeaderPort(message.getPort());
            node.setInRestartPhase(false);
        }
    }

    /**
     * Leader receives request lease from follower.
     * 1. Start coordination phase
     * 2. Send request state to all other nodes
     */
    private void receiveRequestLease(Message message) {
        if (node.isLeader()) {

            // We have been in operation phase before
            if (!node.isInCoordinationPhase()) {
                // Increase round number
                node.setRoundNumber(node.getRoundNumber() + 1);

                persister.persistState(true, node.getRoundNumber(), node.getCrdt(), Optional.empty(), node.leaderBallotNumber);
                node.setInCoordinationPhase(true);
                leaderMergedCrdt = node.getCrdt(); // set leader crdt first
                leaseRequestFrom.clear();

                leaseRequestFrom.add(message.getPort());


                // Send Request State to all other nodes
                this.lastRequestStateSent = System.currentTimeMillis();
                String messageStr = MessageType.REQS.getTitle() + ":" + node.getRoundNumber();
                messageHandler.broadcastWithIgnore(messageStr, node.getNodesPorts(), leaseRequestFrom, false);
            } else {
                // We are already in coordination phase.
                leaseRequestFrom.add(message.getPort());
            }

            // Receiving request-lease is treated like receiving a state message
            String internalStateMessage = MessageType.STATE.getTitle() + ":" + node.getRoundNumber() +  ":" + message.getContent();
            receiveState(new Message(message.getAddress(), message.getPort(), internalStateMessage));
        }
    }

    /**
     * Follower receives request state from leader.
     * 1. Start coordination phase
     * 2. Send state to leader
     */
    private void receiveRequestState(Message message) {
        if (!node.isInRestartPhase()) { // In restart phase we only deliver <accept-sync> and <decide> messages

            int roundNumber = Integer.parseInt(message.getContent());
            node.setRoundNumber(roundNumber);

            persister.persistState(true, node.getRoundNumber(), node.getCrdt(), Optional.empty(), node.leaderBallotNumber);
            node.setInCoordinationPhase(true);

            // Delay message send
            delaysMessageSent();

            String outMessage = MessageType.STATE.getTitle() + ":" + node.getRoundNumber() +  ":" + node.getCrdt().toString();
            messageHandler.sendToLeader(outMessage);
        }
    }

    /**
     * Method used to simulate delays in message sending.
     */
    private void delaysMessageSent() {
        int delayInSeconds = 5;
        if (node.isAddMessageDelay()) {
            try {
                Thread.sleep(delayInSeconds * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Leader receives state message from follower.
     * 1. Merge state with leaders-merged-crdt
     * <p>
     * If we have received state from quorum:
     * 1. Reassign leases
     * 2. Propose state to all nodes
     */
    private void receiveState(Message message) {
        if (node.isLeader()) {
            String[] messageParts = message.getContent().split(":");
            int roundNumber = Integer.parseInt(messageParts[0]);
            String crdtString = messageParts[1];

            // Check if message is from an older coordination round
            if (isOldMessage(roundNumber)) {
                logger.debug("Received old state message.");
                sendDecideForOldMessage(message);
                return;
            }

            LimitedResourceCrdt state = new LimitedResourceCrdt(crdtString);
            leaderMergedCrdt.merge(state);
            statesReceivedFrom.add(message.getPort());

            Function processStateMajority = () -> {
                if (lastAcceptRoundSend >= node.getRoundNumber()) {
                    // We have already sent an accepted message in this round. We can ignore this.
                    logger.debug("We have already sent a accept message in this round.");
                    return;
                }

                reassignLeases();
                logger.debug("Leader proposes state: " + leaderMergedCrdt);

                // Send ACCEPT to all nodes
                this.lastAcceptSent = System.currentTimeMillis();
                this.lastAcceptRoundSend = node.getRoundNumber();
                String outMessage = MessageType.ACCEPT.getTitle() +":" + node.getRoundNumber() + ":" + leaderMergedCrdt.toString();
                messageHandler.broadcast(outMessage);

                // Reset state variables
                statesReceivedFrom.clear();
            };

            triggerNextCoordinationStateWhenReady(statesReceivedFrom.size(), lastRequestStateSent, processStateMajority);
        }
    }

    /**
     * Decides whether we are ready to process next coordination phase: e.g. after request state or accept.
     * Case 1: We have received STATE/ACCEPTED message from all nodes.
     * Case 2: We have received STATE/ACCEPTED message from a quorum of nodes and we have passed the wait time.
     * Case 3: We have received STATE/ACCEPTED message from a quorum of nodes but we have not passed the wait time yet.
     * -> Start a new thread that waits for the remaining time and then triggers the function.
     */
    private void triggerNextCoordinationStateWhenReady(int messagesReceivedFrom, long lastMessageSent, Function function) {
        // +1 is for leader
        if (messagesReceivedFrom + 1 == node.getNodesPorts().size()) {
            logger.debug("Received messages from all nodes.");

            // Trigger function
            function.apply();
        }
        // +1 is for leader
        if (messagesReceivedFrom + 1 >= node.getQuorumSize()
                && System.currentTimeMillis() > messageWaitTime + lastMessageSent) {
            logger.debug("Received messages from quorum of nodes after wait time had passed.");
            // Trigger function
            function.apply();
        } else if (messagesReceivedFrom + 1 >= node.getQuorumSize()) {
            logger.debug("Received messages from quorum of nodes but wait time has not passed yet.");

            // Trigger function
            long waitTime = messageWaitTime + lastMessageSent - System.currentTimeMillis(); // This is how long we have to wait before triggering the function
            MessageWaitTimeTrigger messageWaitTimeTrigger = new MessageWaitTimeTrigger(waitTime, function);
            messageWaitTimeTrigger.start();
        }
    }

    /**
     * Follower receives accept from leader.
     * 1. Set state as accepted state
     * 2. Send accepted to leader
     */
    private void receiveAccept(Message message) {
        if (!node.isInRestartPhase()) {
            String[] messageParts = message.getContent().split(":");
            int roundNumber = Integer.parseInt(messageParts[0]);
            String crdtString = messageParts[1];

            // We need to set round number here again because might have sent <request-lease> and not received <request-state>
            node.setRoundNumber(roundNumber);
            node.setAcceptedCrdt(new LimitedResourceCrdt(crdtString));

            // Persist state before sending ACCEPTED to leader
            persister.persistState(true, node.getRoundNumber(), node.getCrdt(), node.getAcceptedCrdt(), node.leaderBallotNumber);

            // Delay message send
            delaysMessageSent();

            String messageStr = MessageType.ACCEPTED.getTitle() + ":" + node.getRoundNumber();
            messageHandler.sendToLeader(messageStr);
        }
    }

    /**
     * Leader receives accepted from follower.
     * Wait for quorum of accepted messages.
     * 1. Send decide to all nodes
     * 2. End coordination phase
     */
    private void receivedAccepted(Message message) {
        if (node.isLeader()) {
            int roundNumber = Integer.parseInt(message.getContent());

            // Check if message is from an old round
            if (isOldMessage(roundNumber)) {
                logger.debug("Received old accepted message.");
                sendDecideForOldMessage(message);
                return;
            }

            numberOfAccepted++;

            Function processAcceptedMajority = () -> {
                if (lastDecideRoundSend >= node.getRoundNumber()) {
                    // We have already sent an accepted message in this round. We can ignore this.
                    logger.debug("We have already sent a decide message in this round.");
                    return;
                }

                // Set last decide round number
                node.setLastDecideRoundNumber(node.getRoundNumber());

                // Persist state before sending DECIDE to all nodes
                persister.persistState(false, node.getLastDecideRoundNumber(), node.getCrdt(), Optional.empty(), node.leaderBallotNumber);

                String outMessage = MessageType.DECIDE.getTitle() + ":" + node.getLastDecideRoundNumber() + ":" + leaderMergedCrdt.toString();
                messageHandler.broadcast(outMessage);


                // End coordination phase
                node.setInCoordinationPhase(false);
                numberOfAccepted = 0;
                statesReceivedFrom.clear();
            };

            triggerNextCoordinationStateWhenReady(numberOfAccepted, lastAcceptSent, processAcceptedMajority);
        }
    }

    /**
     * Follower receives decide from leader.
     * 1. Merge state with own crdt
     */
    private void receiveDecide(Message message) {
        String[] messageParts = message.getContent().split(":");

        int roundNumber = Integer.parseInt(messageParts[0]);
        node.setRoundNumber(roundNumber); // Required to set if this is a response to an old message
        node.setLastDecideRoundNumber(roundNumber);

        String crdtString = messageParts[1];
        LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(crdtString);

        // Check to see how many resources are left
        int assignedResourcesToMe = mergingCrdt.queryProcess(node.getOwnIndex());
        int resourcesLeftForLeader = mergingCrdt.queryProcess(node.getNodesPorts().indexOf(node.getLeaderPort()));

        if (resourcesLeftForLeader == 0 && assignedResourcesToMe == 0) {
            // Leader has no resources left. Therefore, we are out of resources now!
            node.setOutOfResources(true);
        } else if (assignedResourcesToMe == 0) {
            // No resources assigned to me. We are working on the final resources now.
            node.setFinalResources(true);
            node.setOutOfResources(false);
        }

        node.getCrdt().merge(mergingCrdt);
        // Persist state after merging
        persister.persistState(false, node.getLastDecideRoundNumber(), node.getCrdt(), Optional.empty(), node.leaderBallotNumber);

        if (node.isInRestartPhase()) {
            node.setLeaderPort(message.getPort()); // If we were in restart phase we need to update leader port
            node.setInRestartPhase(false); // End of restart phase, if we were in it
        }

        logger.info("Received decided state: " + message.getContent());
        node.setInCoordinationPhase(false); // End of coordination phase
    }

    /**
     * Check if a message that we received is old.
     * If the round number is less than or equal to the last decided round number, then it is old.
     */
    private boolean isOldMessage(int receivedRoundNumber) {
        return receivedRoundNumber <= node.getLastDecideRoundNumber();
    }

    /**
     * Send decide with current leader state to node that we got an old message from.
     */
    private void sendDecideForOldMessage(Message message) {
        String outMessage = MessageType.DECIDE.getTitle() +":" + node.getLastDecideRoundNumber() +  ":" + node.getCrdt().toString();
        messageHandler.send(outMessage, message.getPort());
    }

    /**
     * Reassigns leases to all nodes that we got a state message from.
     * We can only take leases from nodes that we have received a state from.
     * <p>
     * BASE CASE: We have more available resources than nodes that we got a state from.
     * We can assign the resources to the nodes directly.
     * <p>
     * CASE (less resources than nodes): We need to coordinate for every lease.
     * We assign the remaining leases to the leader now and the followers need to send a new <REQL> for each new lease
     * that they need.
     * <p>
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

        logger.info("Available resources: " + availableResources);

        // We will set the lower bound for all active processes to highest lower bound
        int highestLowerBound = leaderMergedCrdt.getLowerCounter().stream().max(Integer::compare).get();

        int amountOfStates = statesReceivedFrom.size() + 1; // +1 for the leader

        if (availableResources == 0) {
            // We are out of resources now
            node.setOutOfResources(true);
        } else if (availableResources >= amountOfStates) {
            // BASE CASE: We have more resources than nodes. We can assign the resources to the nodes directly.
            node.setFinalResources(false);
            node.setOutOfResources(false);

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
            logger.info("Out of resources. Cannot decrement counter.");

            // Notify client about unsuccessful decrement
            messageHandler.send(MessageType.DENY_RES.getTitle(), message.getPort());
            return;
        }


        int ownResourcesLeft = node.getCrdt().queryProcess(node.getOwnIndex());

        if (ownResourcesLeft == 0 && node.isFinalResources()) {
            // We are in final resource mode and have no resources assigned. We need to ask the leader for every new lease.

            requestLeases();

            // Put message right back at the front of the queue, so it will be processed next.
            node.operationMessageQueue.addFirst(message);

        } else if (node.isFinalResources()) {
            // We are in final resource mode but got resources assigned. We can decrement the counter now.

            node.getCrdt().decrement(node.getOwnIndex());
            // Persist state before sending APPROVE to client
            persister.persistState(false, node.getRoundNumber(), node.getCrdt(), node.getAcceptedCrdt(), node.leaderBallotNumber);

            // Notify client about successful decrement
            messageHandler.send(MessageType.APPROVE_RES.getTitle(), message.getPort());
        } else {
            // BASE CASE: we have resources assigned

            boolean successful = node.getCrdt().decrement(node.getOwnIndex());
            if (!successful) {
                logger.error("Could not decrement counter.");
                // todo send request for lease
            } else {
                // Persist state before sending APPROVE to client
                persister.persistState(false, node.getRoundNumber(), node.getCrdt(), node.getAcceptedCrdt(), node.leaderBallotNumber);

                // Notify client about successful decrement
                messageHandler.send(MessageType.APPROVE_RES.getTitle(), message.getPort());

                // Query CRDT and request leases if we have no leases left
                ownResourcesLeft = node.getCrdt().queryProcess(node.getOwnIndex());
                if (ownResourcesLeft == 0) {
                    logger.info("No resources left.");
                    requestLeases();
                }
            }
        }
    }

    /**
     * Request leases.
     * As a follower sendd <REQL> to leader.
     * As a leader send <REQS> to all nodes.
     */
    private void requestLeases() {
        node.setInCoordinationPhase(true);

        if (node.isLeader()) {
            // Prepare coordination phase
            leaderMergedCrdt = node.getCrdt(); // set leader crdt first
            leaseRequestFrom.clear();
            leaseRequestFrom.add(node.getOwnPort());

            this.lastRequestStateSent = System.currentTimeMillis();

            // Increase round number & persist state again
            node.setRoundNumber(node.getRoundNumber() + 1);

            persister.persistState(true, node.getRoundNumber(), node.getCrdt(), Optional.empty(), node.leaderBallotNumber);

            // Send Request State to all other nodes
            String message = MessageType.REQS.getTitle() + ":" + node.getRoundNumber();
            messageHandler.broadcast(message);
        } else {
            persister.persistState(true, node.getRoundNumber(), node.getCrdt(), Optional.empty(), node.leaderBallotNumber);

            String outMessage = MessageType.REQL.getTitle() + ":" + node.getCrdt().toString();
            messageHandler.sendToLeader(outMessage);
        }
    }

    public void setStatesReceivedFrom(Set<Integer> statesReceivedFrom) {
        this.statesReceivedFrom = statesReceivedFrom;
    }


    /**
     * Thread that waits for remaining time and then triggers the function.
     */
    private class MessageWaitTimeTrigger extends Thread {

        long timeToWait;
        Function function;

        public MessageWaitTimeTrigger(long timeToWait, Function function) {
            this.timeToWait = timeToWait;
            this.function = function;
        }

        public void run() {
            try {
                sleep(timeToWait);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // Trigger function
            logger.debug("Triggering function after waiting for remaining time.");
            function.apply();
        }
    }

    // --------------------------------------------------------------------------------------
    // ----------------- HEARTBEAT/LEADER ELECTION MESSAGE HANDLING -------------------------
    // --------------------------------------------------------------------------------------


    private void handleElectionRequest(Message message) {
        node.leaderElectionInProcess = true;
        node.ballotLeaderElection.rnd= Integer.parseInt(message.getContent());
        ElectionReplyMessage electionReplyMessage = new ElectionReplyMessage(node.ballotLeaderElection.rnd, node.ballotNumber, node.isQuorumConnected());
        String outMessage = MessageType.ELECTIONREPLY.getTitle() + ":" + electionReplyMessage.toString();
        node.messageHandler.send(outMessage, message.getPort());
    }
    private void handleElectionResult(String message) {
        String[] parts = message.split(",");
        int id = Integer.parseInt(parts[0]);
        int ballotNumber = Integer.parseInt(parts[1]);
        boolean wasLeader = node.isLeader();
        this.node.setLeaderPort(id);
        this.node.leaderBallotNumber = ballotNumber;
        if(!wasLeader && node.isLeader()){
            logger.info("I'm the new leader");
            node.iAmNewLeader();
        } else if (!node.isLeader()) {
            logger.info("LeaderElection: I hereby accept " + id + " as my leader");
        }
        node.leaderElectionInProcess = false;


    }
}
