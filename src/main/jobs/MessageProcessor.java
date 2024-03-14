package main.jobs;

import main.Node;
import main.ballot_leader_election.ElectionReplyMessage;
import main.crdt.Crdt;
import main.crdt.LimitedResourceCrdt;
import main.crdt.ORSet;
import main.crdt.PNCounter;
import main.failure_detector.HeartbeatMessage;
import main.utils.*;

import java.util.*;


/**
 * Thread responsible for processing messages from the node's message queues.
 */
public class MessageProcessor extends Thread {

    private final Logger logger;

    private final Node node;
    private final MessageHandler messageHandler;

    /**
     * CRDT that is only used by the leader to merge all CRDTs they get from the State calls.
     */
    private LimitedResourceCrdt leaderMergedCrdt = null;
    private final Persister persister;
    private final long messageWaitTime = 70;

    /**
     * Set of all nodes (ports) that we have received a state from.
     */
    private Set<Integer> statesReceivedFrom = new HashSet<>();

    /**
     * Number of accepted messages received from followers in the current coordination phase.
     */
    private int numberOfAccepted = 0;

    /**
     * Number of promise messages received from followers in the current prepare phase.
     */
    private int numberOfPromises = 0;

    /**
     * Indicates which nodes have sent us the latest request for lease.
     * In one coordination phase multiple lease requests can be received.
     */
    private List<Integer> leaseRequestFrom = new ArrayList<>();

    /**
     * Timestamp when the last request for state/accept/prepare was broadcasted.
     * Used to see whether we need to wait for more messages.
     */
    private long lastRequestStateSent = 0;
    private long lastAcceptSent = 0;
    private long lastPrepareSent = 0;

    /**
     * Round when the last accepted was send. Prevents sending accepted messages multiple times in one round.
     */
    private int lastAcceptRoundSend = 0;

    /**
     * Flag to indicate that we have already sent an accept-sync message in this prepare phase.
     */
    private boolean prepareAcceptSyncAlreadySent = false;

    /**
     * Max round number received by followers in the prepare phase.
     */
    private int maxRoundNumberInPreparePhase = 0;

    public MessageProcessor(Node node, MessageHandler messageHandler) {
        this.node = node;
        this.logger = node.logger;
        this.messageHandler = messageHandler;
        this.persister = node.persister;
    }


    /**
     * Processes messages from the leader election, heartbeats coordination and operation message queues.
     * The priority of the queues is as follows:
     * findLeaderMessageQueue > heartbeatAndElectionMessageQueue > preparePhaseMessageQueue > coordinationMessageQueue > operationMessageQueue
     * If a node doesn't know the current leader only findLeader messages and heartbeat and election messages are handled.
     * A node doesn't know the current leader because for one or more of the following reasons:
     * no leader was elected yet
     * the leader has failed and an election is in process
     * the leader is not connecting to a quorum anymore
     * the node missed the last election
     */
    public void run() {
        while (true) {
            if (!node.monotonicCrdtMessageQueue.isEmpty()) {
                handleMonotonicCrdtMerge(node.monotonicCrdtMessageQueue.poll());
            }

            if (!node.findLeaderMessageQueue.isEmpty()) {
                matchFindLeaderMessage(node.findLeaderMessageQueue.poll());
            }
            if (!node.heartbeatAndElectionMessageQueue.isEmpty()) {
                matchHeartbeatAndElectionMessage(node.heartbeatAndElectionMessageQueue.poll());
            }
            if (!(node.isSearchingForLeader || node.leaderElectionInProcess)) {
                if (!node.preparePhaseMessageQueue.isEmpty() && node.isLeader() && node.isInPreparePhase()) {
                    matchPreparePhaseMessage(node.preparePhaseMessageQueue.poll());
                }
                if (!node.coordinationMessageQueue.isEmpty()) {
                    matchCoordinationMessage(node.coordinationMessageQueue.poll());
                } else if (!node.operationMessageQueue.isEmpty() && !node.isInCoordinationPhase() && !node.isInRestartPhase()) {
                    // Only process operation messages if we are not in the coordination or restart phase
                    matchOperationMessage(node.operationMessageQueue.poll());
                }
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
            case PREPARE:
                receivedPrepare();
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
     * Matches prepare phase message to the appropriate method. Only used by a newly elected leader.
     */
    private void matchPreparePhaseMessage(Message message) {
        if (message == null) {
            // We need to filter out null messages, because we can get them during node failures
            return;
        }
        switch (message.getType()) {
            case PROMISE:
                receivePromise(message);
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
                node.getLimitedResourceCrdt().increment(node.getOwnIndex());
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

    /**
     * Matches heartbeat and election messages to the appropriate method.
     */
    void matchHeartbeatAndElectionMessage(Message message) {
        if (message == null) {
            // We need to filter out null messages, because we can get them during node failures
            return;
        }
        switch (message.getType()) {
            case HB_REQUEST:
                node.failureDetector.sendHeartbeatReply(message.getPort());
                break;
            case HB_REPLY:
                node.failureDetector.updateNodeStatus(message.getPort(), HeartbeatMessage.fromString(message.getContent()));
                break;
            case ELECTION_REQUEST:
                handleElectionRequest(message);
                break;
            case ELECTION_REPLY:
                node.ballotLeaderElection.handleElectionReply(message);
                break;
            case ELECTION_RESULT:
                handleElectionResult(message.getContent());
                break;
            default:
                logger.info("Unknown message from :" + message.getPort() + " : Type: " + message.getType() + " Content: " + message.getContent());
        }
    }

    /**
     * Matches find leader messages to the appropriate method.
     */
    void matchFindLeaderMessage(Message message) {
        switch (message.getType()) {
            case FIND_LEADER_REQUEST:
                replyToFindLeaderMessage(message.getPort());
                break;
            case FIND_LEADER_REPLY:
                checkWhoIsLeader(message.getContent());
                break;
            default:
                logger.info("Unknown message from :" + message.getPort() + " : Type: " + message.getType() + " Content: " + message.getContent());
        }
    }

    // --------------------------------------------------------------------------------------
    // ----------------- PREPARE PHASE MESSAGE HANDLING -------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Newly elected leader receives promise from follower.
     * Needs to wait message timeout period or until it receives promise from a majority before sending <accept-sync>.
     */
    private void receivePromise(Message message) {
        if (node.isLeader()) {
            String[] messageParts = message.getContent().split(":");
            int roundNumber = Integer.parseInt(messageParts[0]);
            String crdtString = messageParts[1];

            // Check if we received prepare even though the prepare phase has ended
            if (!node.isInPreparePhase()) {
                logger.debug("Received old promise message.");
                // Send accept-sync to follower.
                String messageStr = MessageType.ACCEPT_SYNC.getTitle() + ":" + node.getLastDecideRoundNumber() + ":" + node.getLimitedResourceCrdt().toString();
                messageHandler.send(messageStr, message.getPort());
                return;
            }

            numberOfPromises++;

            maxRoundNumberInPreparePhase = Math.max(maxRoundNumberInPreparePhase, roundNumber);

            LimitedResourceCrdt state = new LimitedResourceCrdt(crdtString);
            leaderMergedCrdt.merge(state);
            statesReceivedFrom.add(message.getPort());

            triggerNextCoordinationStateWhenReady(numberOfPromises, lastPrepareSent, QuorumMessageType.PROMISE, roundNumber);
        }
    }

    /**
     * Send ACCEPT_SYNC to all followers (in restart phase).
     * Only triggered after we have received a state from all nodes OR a quorum and the timeout has passed.
     */
    private synchronized void sendAcceptSyncMessageAfterQuorumReached() {
        if (prepareAcceptSyncAlreadySent) {
            // We have already sent an accept-sync message in this round. We can ignore this.
            logger.debug("We have already sent a accept-sync message in this prepare phase.");
            return;
        }

        // Set new round number
        node.setRoundNumber(maxRoundNumberInPreparePhase);
        node.setLastDecideRoundNumber(maxRoundNumberInPreparePhase);

        // Send ACCEPT-SYNC to all nodes
        String outMessage = MessageType.ACCEPT_SYNC.getTitle() + ":" + maxRoundNumberInPreparePhase + ":" + leaderMergedCrdt.toString();
        messageHandler.broadcast(outMessage);
        prepareAcceptSyncAlreadySent = true;

        // Reset number of promises
        numberOfPromises = 0;
    }

    /**
     * Follower receives prepare from new leader.
     * Send current state and latest accepted state to leader.
     */
    private void receivedPrepare() {
        if (!node.isLeader()) {
            // Get accepted crdt or otherwise the current crdt
            LimitedResourceCrdt crdt = node.getAcceptedCrdt().orElse(node.getLimitedResourceCrdt());

            String message = MessageType.PROMISE.getTitle() + ":" + node.getRoundNumber() + ":" + crdt;
            messageHandler.sendToLeader(message);
        }
    }

    /**
     * Newly elected leader starts prepare phase by sending prepare message to all nodes.
     */
    private void startPreparePhase() {
        node.setInPreparePhase(true);

        leaderMergedCrdt = node.getLimitedResourceCrdt(); // set leader crdt first
        this.lastPrepareSent = System.currentTimeMillis();
        prepareAcceptSyncAlreadySent = false; // Reset flag

        // Send prepare message to all nodes
        String message = MessageType.PREPARE.getTitle();
        messageHandler.broadcast(message);
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
                String messageStr = MessageType.ACCEPT_SYNC.getTitle() + ":" + node.getLastDecideRoundNumber() + ":" + node.getLimitedResourceCrdt().toString() + ":" + node.ballotNumber;
                messageHandler.send(messageStr, message.getPort());
            }
        }
    }

    /**
     * Receive accept-sync from leader. Format of message: <accept-sync>:<round-number>:<crdt>
     */
    private void receiveAcceptSync(Message message) {
        String[] messageParts = message.getContent().split(":");
        int leaderRoundNumber = Integer.parseInt(messageParts[0]);
        String crdtString = messageParts[1];
        if (node.isInRestartPhase()) {
            int currentLeaderBallotNumber = Integer.parseInt(messageParts[2]);
            node.leaderBallotNumber = currentLeaderBallotNumber;
        }
        node.mergeCrdts(crdtString);
        node.setRoundNumber(leaderRoundNumber);
        node.setLastDecideRoundNumber(leaderRoundNumber);

        // Persist newly loaded state
        persister.persistState(false, node.getLastDecideRoundNumber(), node.getLimitedResourceCrdt(), Optional.empty(), node.leaderBallotNumber);

        node.setLeaderPort(message.getPort());
        node.setInRestartPhase(false);
        node.setInCoordinationPhase(false); // Stop coordination phase after receiving accept-sync
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

                persister.persistState(true, node.getRoundNumber(), node.getLimitedResourceCrdt(), Optional.empty(), node.leaderBallotNumber);
                node.setInCoordinationPhase(true);
                leaderMergedCrdt = node.getLimitedResourceCrdt(); // set leader crdt first
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
            String internalStateMessage = MessageType.STATE.getTitle() + ":" + node.getRoundNumber() + ":" + message.getContent();
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

            persister.persistState(true, node.getRoundNumber(), node.getLimitedResourceCrdt(), Optional.empty(), node.leaderBallotNumber);
            node.setInCoordinationPhase(true);

            // Function that sends state message. Is triggered below (either after delay or immediately)
            Function sendStateMessage = () -> {
                String outMessage = MessageType.STATE.getTitle() + ":" + node.getRoundNumber() + ":" + node.getLimitedResourceCrdt().toString();
                messageHandler.sendToLeader(outMessage);
            };

            // Either delay the message sending or send it straight away
            if (node.isAddMessageDelay()) {
                delaysMessageSent(sendStateMessage);
            } else {
                sendStateMessage.apply();
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
                logger.info("Received old state message.");
                sendDecideForOldMessage(message);
                return;
            }

            LimitedResourceCrdt state = new LimitedResourceCrdt(crdtString);
            leaderMergedCrdt.merge(state);
            logger.info("Merged state CRDT: " + state + " to " + leaderMergedCrdt.toString());
            statesReceivedFrom.add(message.getPort());

            triggerNextCoordinationStateWhenReady(statesReceivedFrom.size(), lastRequestStateSent, QuorumMessageType.STATE, roundNumber);
        }
    }

    /**
     * Send ACCEPT to all followers.
     * Only triggered after we have received a state from all nodes OR a quorum and the timeout has passed.
     */
    private synchronized void sendAcceptMessageAfterQuorumReached(int roundNumber) {
        if (lastAcceptRoundSend >= roundNumber) {
            // We have already sent an accepted message in this round. We can ignore this.
            logger.debug("We have already sent a accept message in this round.");
            return;
        }

        reassignLeases();
        logger.debug("Leader proposes state: " + leaderMergedCrdt);

        // Send ACCEPT to all nodes
        this.lastAcceptSent = System.currentTimeMillis();
        this.lastAcceptRoundSend = node.getRoundNumber();
        String outMessage = MessageType.ACCEPT.getTitle() + ":" + node.getRoundNumber() + ":" + leaderMergedCrdt.toString();
        messageHandler.broadcast(outMessage);

        // Reset state variables
        statesReceivedFrom.clear();
    }


    /**
     * Message types that we need at least a quorum of messages for to continue.
     */
    enum QuorumMessageType {
        PROMISE, STATE, ACCEPTED
    }

    private void triggerNextCoordinationBasedOnQuorumType(QuorumMessageType messageType, int roundNumber) {
        switch (messageType) {
            case PROMISE:
                sendAcceptSyncMessageAfterQuorumReached();
                break;
            case STATE:
                sendAcceptMessageAfterQuorumReached(roundNumber);
                break;
            case ACCEPTED:
                sendDecideMessageAfterQuorumReached(roundNumber);
                break;
        }
    }

    /**
     * Decides whether we are ready to process next coordination phase: e.g. after request state, accept or promise.
     * Case 1: We have received STATE/ACCEPTED/PROMISE message from all nodes.
     * Case 2: We have received STATE/ACCEPTED/PROMISE message from a quorum of nodes and we have passed the wait time.
     * Case 3: We have received STATE/ACCEPTED/PROMISE message from a quorum of nodes but we have not passed the wait time yet.
     * -> Start a new thread that waits for the remaining time and then triggers the function.
     */
    private void triggerNextCoordinationStateWhenReady(int amountOfMessagesReceived, long lastMessageSent, QuorumMessageType messageType, int roundNumber) {
        // +1 is for leader
        if (amountOfMessagesReceived + 1 == node.getNodesPorts().size()) {
            logger.debug("Received messages from all nodes.");

            // Trigger functions
            triggerNextCoordinationBasedOnQuorumType(messageType, roundNumber);
            return;
        }
        // +1 is for leader
        if (amountOfMessagesReceived + 1 >= node.getQuorumSize()
                && System.currentTimeMillis() > Long.sum(messageWaitTime, lastMessageSent)) {
            logger.debug("Received messages from quorum of nodes after wait time had passed. Message wait time: " + messageWaitTime + "lastMessageSent: " + lastMessageSent +"Sum: "+  Long.sum(messageWaitTime,lastMessageSent) +" Current time: " + System.currentTimeMillis());
            // Trigger function
            triggerNextCoordinationBasedOnQuorumType(messageType, roundNumber);
        } else if (amountOfMessagesReceived + 1 >= node.getQuorumSize()) {
            logger.debug("Received messages from quorum of nodes but wait time has not passed yet.");

            // Trigger function
            long waitTime = messageWaitTime + lastMessageSent - System.currentTimeMillis(); // This is how long we have to wait before triggering the function
            WaitTimeTrigger messageWaitTimeTrigger = new WaitTimeTrigger(waitTime, messageType, roundNumber);
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
            persister.persistState(true, node.getRoundNumber(), node.getLimitedResourceCrdt(), node.getAcceptedCrdt(), node.leaderBallotNumber);

            // Function that sends accepted message. Is triggered below (either after delay or immediately)
            Function sendAcceptedMessage = () -> {
                String messageStr = MessageType.ACCEPTED.getTitle() + ":" + node.getRoundNumber();
                messageHandler.sendToLeader(messageStr);
            };

            // Send message with delay or immediately
            if (node.isAddMessageDelay()) {
                delaysMessageSent(sendAcceptedMessage);
            } else {
                sendAcceptedMessage.apply();
            }
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
            triggerNextCoordinationStateWhenReady(numberOfAccepted, lastAcceptSent, QuorumMessageType.ACCEPTED, roundNumber);
        }
    }

    /**
     * Send DECIDE to all followers.
     * Only triggered after we have received a state from all nodes OR a quorum and the timeout has passed.
     */
    private synchronized void sendDecideMessageAfterQuorumReached(int roundNumber) {
        if (node.getLastDecideRoundNumber() >= roundNumber) {
            // We have already sent a decide message in this round. We can ignore this.
            logger.debug("We have already sent a decide message in this round.");
            return;
        }

        // Set last decide round number
        node.setLastDecideRoundNumber(node.getRoundNumber());

        // Persist state before sending DECIDE to all nodes
        persister.persistState(false, node.getLastDecideRoundNumber(), node.getLimitedResourceCrdt(), Optional.empty(), node.leaderBallotNumber);

        logger.debug("Leader decides state: " + leaderMergedCrdt + " with round number: " + node.getLastDecideRoundNumber());
        String outMessage = MessageType.DECIDE.getTitle() + ":" + node.getLastDecideRoundNumber() + ":" + leaderMergedCrdt.toString();
        messageHandler.broadcast(outMessage);

        // End coordination phase
        node.setInCoordinationPhase(false);
        numberOfAccepted = 0;
        statesReceivedFrom.clear();
    }

    /**
     * Follower receives decide from leader.
     * 1. Merge state with own crdt
     */
    private void receiveDecide(Message message) {
        String[] messageParts = message.getContent().split(":");

        int roundNumber = Integer.parseInt(messageParts[0]);

        if (isOldMessage(roundNumber)) {
            logger.debug("Received old decide message.");
            return;
        }

        node.setRoundNumber(roundNumber); // Required to set if this is a response to an old message
        node.setLastDecideRoundNumber(roundNumber);

        String crdtString = messageParts[1];
        LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(crdtString);
        node.getLimitedResourceCrdt().merge(mergingCrdt);

        // Check to see how many resources are left
        int assignedResourcesToMe = node.getLimitedResourceCrdt().queryProcess(node.getOwnIndex());
        int resourcesLeftForLeader = node.getLimitedResourceCrdt().queryProcess(node.getNodesPorts().indexOf(node.getLeaderPort()));

        if (resourcesLeftForLeader == 0 && assignedResourcesToMe == 0) {
            // Leader has no resources left. Therefore, we are out of resources now!
            node.setOutOfResources(true);
        } else if (assignedResourcesToMe == 0) {
            // No resources assigned to me. We are working on the final resources now.
            node.setFinalResources(true);
            node.setOutOfResources(false);
        }

        node.setAcceptedCrdt(null); // Reset accepted CRDT because it was decided now
        // Persist state after merging
        persister.persistState(false, node.getLastDecideRoundNumber(), node.getLimitedResourceCrdt(), Optional.empty(), node.leaderBallotNumber);

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
        String outMessage = MessageType.DECIDE.getTitle() + ":" + node.getLastDecideRoundNumber() + ":" + node.getLimitedResourceCrdt().toString();
        logger.debug("Sending decide for old message: " + outMessage);
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

        if (node.isCoordinateForEveryResource()) {
            if (availableResources > 0) {
                // Get index of first requester
                int indexOfRequester = node.getNodesPorts().indexOf(leaseRequestFrom.get(0));
                // Give requesting process one lease (just increment it)
                leaderMergedCrdt.setUpper(indexOfRequester, leaderMergedCrdt.getUpperCounter().get(indexOfRequester) + 1);

                // Rust take away one resource from the leader
                leaderMergedCrdt.setLower(node.getOwnIndex(), leaderMergedCrdt.getLowerCounter().get(node.getOwnIndex()) + 1);
            } else {
                // We are out of resources now
                node.setOutOfResources(true);
            }
            return;
        }

        // We will set the lower bound for all active processes to highest upper bound
        int highestUpperCounter = leaderMergedCrdt.getUpperCounter().stream().max(Integer::compare).get();

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
                    leaderMergedCrdt.setUpper(i, highestUpperCounter + resourcesPerNode + additional);
                    leaderMergedCrdt.setLower(i, highestUpperCounter);
                    resourcesLeft--;
                }
            }
        } else {
            // We have less resources than nodes that we got a state from. We need to coordinate from now on for every lease.
            // We assign the remaining leases to the leader for this time.

            // Set upper and lower count to same value for all nodes that we got a state from
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
        int ownResourcesLeft = node.getLimitedResourceCrdt().queryProcess(node.getOwnIndex());


        // Out of resources: deny request straight away.
        // We need to double check if node is really out of resources
        if (node.isOutOfResources() && node.getLimitedResourceCrdt().queryProcess(node.getNodesPorts().indexOf(node.getLeaderPort())) == 0
                && node.getLimitedResourceCrdt().queryProcess(node.getOwnIndex()) == 0) {
            logger.info("Out of resources. Cannot decrement counter.");

            // Notify client about unsuccessful decrement
            messageHandler.send(MessageType.DENY_RES.getTitle(), message.getPort());
            return;
        }

        // Used for benchmarking. Run coordination phase for every resource.
        // Only works if we do not request resources from the leader.
        if (node.isCoordinateForEveryResource()) {


            if (ownResourcesLeft == 0) {
                // We have no resources left. We need to ask the leader for every new lease.
                requestLeases();
                // Put message right back at the front of the queue, so it will be processed next.
                node.operationMessageQueue.addFirst(message);
            } else if (ownResourcesLeft > 0) {
                node.getLimitedResourceCrdt().decrement(node.getOwnIndex());
                // Notify client about successful decrement
                messageHandler.send(MessageType.APPROVE_RES.getTitle(), message.getPort());
            }
            return;
        }

        if (ownResourcesLeft == 0 && node.isFinalResources()) {
            // We are in final resource mode and have no resources assigned. We need to ask the leader for every new lease.

            requestLeases();

            // Put message right back at the front of the queue, so it will be processed next.
            node.operationMessageQueue.addFirst(message);

        } else if (node.isFinalResources()) {
            // We are in final resource mode but got resources assigned. We can decrement the counter now.

            node.getLimitedResourceCrdt().decrement(node.getOwnIndex());
            // Persist state before sending APPROVE to client
            persister.persistState(false, node.getRoundNumber(), node.getLimitedResourceCrdt(), node.getAcceptedCrdt(), node.leaderBallotNumber);

            // Notify client about successful decrement
            messageHandler.send(MessageType.APPROVE_RES.getTitle(), message.getPort());
        } else {
            // BASE CASE: we have resources assigned

            boolean successful = node.getLimitedResourceCrdt().decrement(node.getOwnIndex());
            if (!successful) {
                logger.error("Could not decrement counter.");
                // todo send request for lease
            } else {
                // Persist state before sending APPROVE to client
                persister.persistState(false, node.getRoundNumber(), node.getLimitedResourceCrdt(), node.getAcceptedCrdt(), node.leaderBallotNumber);

                // Notify client about successful decrement
                messageHandler.send(MessageType.APPROVE_RES.getTitle(), message.getPort());

                // Query CRDT and request leases if we have no leases left
                ownResourcesLeft = node.getLimitedResourceCrdt().queryProcess(node.getOwnIndex());
                if (ownResourcesLeft == 0) {
                    logger.info("No resources left.");
                    requestLeases();
                }
            }
        }
    }

    /**
     * Request leases.
     * As a follower send <REQL> to leader.
     * As a leader send <REQS> to all nodes.
     */
    private void requestLeases() {
        node.setInCoordinationPhase(true);

        if (node.isLeader()) {
            // Prepare coordination phase
            leaderMergedCrdt = node.getLimitedResourceCrdt(); // set leader crdt first
            leaseRequestFrom.clear();
            leaseRequestFrom.add(node.getOwnPort());

            this.lastRequestStateSent = System.currentTimeMillis();

            // Increase round number & persist state again
            node.setRoundNumber(node.getRoundNumber() + 1);

            persister.persistState(true, node.getRoundNumber(), node.getLimitedResourceCrdt(), Optional.empty(), node.leaderBallotNumber);

            // Send Request State to all other nodes
            logger.info("Leader ran out of resources. Starting coordination phase.");
            String message = MessageType.REQS.getTitle() + ":" + node.getRoundNumber();
            messageHandler.broadcast(message);
        } else {
            persister.persistState(true, node.getRoundNumber(), node.getLimitedResourceCrdt(), Optional.empty(), node.leaderBallotNumber);

            String outMessage = MessageType.REQL.getTitle() + ":" + node.getLimitedResourceCrdt().toString();
            messageHandler.sendToLeader(outMessage);
        }
    }

    public void setStatesReceivedFrom(Set<Integer> statesReceivedFrom) {
        this.statesReceivedFrom = statesReceivedFrom;
    }

    // --------------------------------------------------------------------------------------
    // ----------------- HEARTBEAT/LEADER ELECTION MESSAGE HANDLING -------------------------
    // --------------------------------------------------------------------------------------

    private void handleElectionRequest(Message message) {
        node.leaderElectionInProcess = true;
        node.ballotLeaderElection.rnd = Integer.parseInt(message.getContent());
        ElectionReplyMessage electionReplyMessage = new ElectionReplyMessage(node.ballotLeaderElection.rnd, node.ballotNumber, node.isQuorumConnected());
        String outMessage = MessageType.ELECTION_REPLY.getTitle() + ":" + electionReplyMessage.toString();
        node.messageHandler.send(outMessage, message.getPort());
    }

    private void handleElectionResult(String message) {
        String[] parts = message.split(",");
        int id = Integer.parseInt(parts[0]);
        int ballotNumber = Integer.parseInt(parts[1]);
        boolean wasLeader = node.isLeader();
        this.node.setLeaderPort(id);
        this.node.leaderBallotNumber = ballotNumber;
        if (!wasLeader && node.isLeader()) {
            logger.info("I'm the new leader");
            startPreparePhase();
        } else if (!node.isLeader()) {
            logger.info("LeaderElection: I hereby accept " + id + " as my leader");
        }
        node.leaderElectionInProcess = false;
    }

    // --------------------------------------------------------------------------------------
    // ----------------- FIND LEADER MESSAGE HANDLING --------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Reply to a FIND_LEADER message with the leader's port and the leaders ballot number.
     */
    private void replyToFindLeaderMessage(int replyPort) {
        String messageStr = MessageType.FIND_LEADER_REPLY.getTitle() + ":" + node.getLeaderPort() + "," + node.leaderBallotNumber;
        node.messageHandler.send(messageStr, replyPort);
    }

    /**
     * Puts the leader port, the time the same leader got confirmed and the leaders ballot number into the findLeaderAnswers map.
     * This map is then checked by the FailureDetector if a leader got the majority.
     */
    private void checkWhoIsLeader(String messageContent) {
        String[] parts = messageContent.split(",");
        int leaderPort = Integer.parseInt(parts[0]);
        int leaderBallot = Integer.parseInt(parts[1]);
        if (node.findLeaderAnswers.containsKey(leaderPort)) {
            node.findLeaderAnswers.put(leaderPort, new Integer[]{node.findLeaderAnswers.get(leaderPort)[0] + 1, leaderBallot});
        } else {
            node.findLeaderAnswers.put(leaderPort, new Integer[]{1, leaderBallot});
        }
    }

    // --------------------------------------------------------------------------------------
    // ----------------- MONOTONIC CRDT MESSAGE HANDLING ------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Merge incoming monotonic CRDT with existing CRDT in map.
     */
    private void handleMonotonicCrdtMerge(Message message) {
        if (message != null && message.getType() == MessageType.MERGE_MONOTONIC) {
            String[] parts = message.getContent().split(":");
            String id = parts[0];
            String crdtType = parts[1];
            String crdtString = parts[2];

            Crdt mergedCrdt = null;
            if (crdtType.equals("pncounter")) {
                mergedCrdt = new PNCounter(crdtString);
            } else if (crdtType.equals("orset")) {
                mergedCrdt = new ORSet(crdtString);
            }

            if (mergedCrdt == null) {
                logger.error("Unknown monotonic CRDT type: " + crdtType);
                return;
            }

            Crdt existingCrdt = node.getMonotonicCrdts().get(id);
            if (existingCrdt == null) {
                node.getMonotonicCrdts().put(id, mergedCrdt);
            } else {
                existingCrdt.merge(mergedCrdt);
                node.getMonotonicCrdts().put(id, existingCrdt);
                logger.debug("Merged monotonic CRDT: " + id + " " + existingCrdt);
            }
        }
    }


    // --------------------------------------------------------------------------------------
    // --------------------------------- HELPERS --------------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Method used to simulate delays in message sending.
     *
     * @param function Function that is triggered after the delay. It is used to send the message.
     */
    private void delaysMessageSent(Function function) {
        int delayInSeconds = 5;
        WaitTimeTrigger messageWaitTimeTrigger = new WaitTimeTrigger(delayInSeconds * 1000, function);
        messageWaitTimeTrigger.start();
    }

    /**
     * Thread that waits for remaining time and then triggers the function.
     */
    private class WaitTimeTrigger extends Thread {


        long timeToWait;
        QuorumMessageType messageType = null;
        int roundNumber = -1;
        Function function = null;

        public WaitTimeTrigger(long timeToWait, QuorumMessageType messageType, int roundNumber) {
            this.timeToWait = timeToWait;
            this.messageType = messageType;
            this.roundNumber = roundNumber;
        }

        public WaitTimeTrigger(long timeToWait, Function function) {
            this.timeToWait = timeToWait;
            this.function = function;
        }

        public void run() {
            try {
                sleep(timeToWait);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (function != null) {
                function.apply();
            } else {

                // Trigger function
                logger.debug("Triggering function after waiting for remaining time for message type " + messageType + " and round number: " + roundNumber);
                triggerNextCoordinationBasedOnQuorumType(messageType, roundNumber);
            }
        }
    }

    public void setLeaderMergedCrdt(LimitedResourceCrdt leaderMergedCrdt) {
        this.leaderMergedCrdt = leaderMergedCrdt;
    }

    public LimitedResourceCrdt getLeaderMergedCrdt() {
        return leaderMergedCrdt;
    }
}
