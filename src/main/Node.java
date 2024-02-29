package main;

import main.crdt.LimitedResourceCrdt;
import main.utils.Message;
import main.utils.MessageType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;


/**
 * Node in the network. Connects to other nodes and clients.
 * Responsible for its own CRDT.
 */
class Node {

    /**
     * UDP socket for receiving and sending messages.
     */
    private DatagramSocket socket;

    /**
     * Utils object that handels sending messages.
     */
    private final MessageHandler messageHandler;
    /**
     * Flag to indicate if the node is currently in lease coordination phase.
     */
    boolean inCoordinationPhase = false;

    /**
     * Is set when we have less resources than processes, so every lease needs a coordination phase.
     */
    boolean finalResources = false;

    boolean outOfResources = false;

    /**
     * Queue of messages to be processed outside of coordination phase. Using concurrent queue to make it thread safe.
     */
    LinkedBlockingDeque<Message> operationMessageQueue = new LinkedBlockingDeque<>();
    /**
     * Queue of messages to be processed. Using concurrent queue to make it thread safe.
     */
    Queue<Message> coordiantionMessageQueue = new ConcurrentLinkedQueue<>();
    private int ownPort;
    private int leaderPort;

    /**
     * Index of the node in the network.
     */
    private int ownIndex;
    /**
     * List of all ports of the nodes in the network.
     */
    private List<Integer> nodesPorts;
    /**
     * Maps ports to their respective output sockets.
     */
    private ConcurrentHashMap<Integer, Socket> nodeOutputSockets = new ConcurrentHashMap<>();
    /**
     * CRDT that only allows access to limited ressources.
     */
    private LimitedResourceCrdt crdt;
    /**
     * CRDT tht we have accepted but is not decided yet.
     */
    private LimitedResourceCrdt acceptedCrdt = null;
    /**
     * CRDT that is only used by the leader to merge all CRDTs they get from the State calls.
     */
    private LimitedResourceCrdt leaderMergedCrdt = null;
    private int numberOfStates = 0;

    /**
     * Set of all nodes (ports) that we have received a state from.
     */
    private Set<Integer> statesReceivedFrom = new HashSet<>();

    /**
     * Indicates which node has send us the latest request for lease.
     */
    // todo: make sure this is not overwritten when we receive multiple requests
    private int leaseRequestReceivedFrom = -1;

    private int numberOfAccepted = 0;

    public Node(int port, List<Integer> nodesPorts) {
        this.ownPort = port;
        this.nodesPorts = nodesPorts;
        this.crdt = new LimitedResourceCrdt(nodesPorts.size());

        // Get own index in port list
        int ownIndex = nodesPorts.indexOf(port);
        if (ownIndex == -1) {
            throw new IllegalArgumentException("Port not in list of nodes.");
        }
        this.ownIndex = ownIndex;

        // Open UDP port
        try {
            socket = new DatagramSocket(port);
            this.messageHandler = new MessageHandler(socket, port);
        } catch (Exception e) {
            System.out.println("Could not open socket!");
            throw new RuntimeException(e);
        }
    }

    public void init() throws Exception {
        MessageReceiver messageReceiver = new MessageReceiver();
        messageReceiver.start();

        MessageProcessor messageProcessor = new MessageProcessor();
        messageProcessor.start();

        CrdtMerger merger = new CrdtMerger();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(merger, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Deserializes the CRDT from the message and merges it with the current CRDT.
     */
    private void mergeCrdts(String crdtString) {
        LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(crdtString);
        crdt.merge(mergingCrdt);
        System.out.println("Merged crdt: " + crdt);
    }

    public LimitedResourceCrdt getCrdt() {
        return crdt;
    }

    public void setLeaderPort(int leaderPort) {
        this.leaderPort = leaderPort;
    }

    // --------------------------------------------------------------------------------------
    // -------------------------- INNER THREAD CLASSES --------------------------------------
    // --------------------------------------------------------------------------------------

    /**
     * Thread responsible for broadcasting our current CRDT state to all nodes in the network.
     */
    class CrdtMerger implements Runnable {

        public void run() {
            if (!inCoordinationPhase) {
                System.out.println("Broadcasting merge.");
                String crdtString = crdt.toString();
                String message = "merge:" + crdtString;
                messageHandler.broadcast(message, nodesPorts);
            }
        }
    }

    /**
     * Thread responsible for processing messages from the queue.
     */
    class MessageProcessor extends Thread {
        public void run() {
            while (true) {
                if (!coordiantionMessageQueue.isEmpty()) {
                    matchCoordinationMessage(coordiantionMessageQueue.poll());
                } else if (!operationMessageQueue.isEmpty() && !inCoordinationPhase) {
                    // Only process operation messages if we are not in the coordination phase
                    matchOperationMessage(operationMessageQueue.poll());
                }
            }
        }

        /**
         * Matches coordination message to the appropriate method.
         */
        private void matchCoordinationMessage(Message message) {
            String crdtString, outMessage;
            switch (message.getType()) {
                case REQL:
                    inCoordinationPhase = true;
                    leaseRequestReceivedFrom = message.getPort();
                    leaderMergedCrdt = crdt; // set leader crdt first
                    crdtString = message.getContent();
                    leaderMergedCrdt.merge(new LimitedResourceCrdt(crdtString)); // merge with received crdt

                    // todo dont broadcast here
                    messageHandler.broadcast(MessageType.REQS.getTitle(), nodesPorts);
                    break;
                case REQS:
                    inCoordinationPhase = true;
                    outMessage = MessageType.STATE.getTitle() + ":" + crdt.toString();
                    messageHandler.send(outMessage, leaderPort);
                    break;
                case STATE:
                    receiveState(message);
                    break;
                case ACCEPT:
                    acceptedCrdt = new LimitedResourceCrdt(message.getContent());
                    messageHandler.send(MessageType.ACCEPTED.getTitle(), leaderPort);
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

        private void receiveDecide(Message message) {
            LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(message.getContent());

            // Check to see how many resources are left
            int assignedResourcesToMe = mergingCrdt.queryProcess(ownIndex);
            int resourcesLeftForLeader = mergingCrdt.queryProcess(nodesPorts.indexOf(leaderPort));

            if (resourcesLeftForLeader == 0) {
                System.out.println("Leader has no resources left. Therefore we are out of resources now!");
                outOfResources = true;
            } else if (assignedResourcesToMe == 0) {
                System.out.println("No resources assigned to me. We are working on the final resources now.");
                finalResources = true;
            }

            crdt.merge(mergingCrdt);
            System.out.println("Received decided state: " + message.getContent());
            inCoordinationPhase = false; // End of coordination phase
        }

        /**
         * Leader receives state message from follower.
         */
        private void receiveState(Message message) {
            LimitedResourceCrdt state = new LimitedResourceCrdt(message.getContent());
            leaderMergedCrdt.merge(state);
            numberOfStates++;
            statesReceivedFrom.add(message.getPort());

            // todo change number here
            if (numberOfStates == nodesPorts.size() - 1) {

                reassignLeases();
                System.out.println("Leader proposes state: " + leaderMergedCrdt);


                String outMessage = MessageType.ACCEPT.getTitle() + ":" + leaderMergedCrdt.toString();
                messageHandler.broadcast(outMessage, nodesPorts);

                // Reset state variables
                numberOfStates = 0;
                statesReceivedFrom.clear();
            }
        }

        /**
         * Reassigns leases to all nodes that we got a state message from.
         */
        private void reassignLeases() {

            // Only get the available resources from nodes that we got a state from. We assume that all other nodes have
            // already given away all their resources.
            int availableResources = 0;
            for (int i = 0; i < nodesPorts.size(); i++) {
                if (i == ownIndex || statesReceivedFrom.contains(nodesPorts.get(i))) {
                    availableResources += leaderMergedCrdt.queryProcess(i);
                }
            }

            System.out.println("Available resources: " + availableResources);
            int highestLowerBound = leaderMergedCrdt.getLowerCounter().stream().max(Integer::compare).get();

            int amountOfStates = statesReceivedFrom.size() + 1; // +1 for the leader

            if (availableResources >= amountOfStates) {
                // We have enough resources to give to all nodes that we got a state from.
                int resourcesPerNode = availableResources / amountOfStates;
                int resourcesLeft = availableResources % amountOfStates;

                for (int i = 0; i < nodesPorts.size(); i++) {
                    if (i == ownIndex || statesReceivedFrom.contains(nodesPorts.get(i))) {
                        int additional = resourcesLeft > 0 ? 1 : 0; // Add one additional resource to the first nodes
                        leaderMergedCrdt.setUpper(i, highestLowerBound + resourcesPerNode + additional);
                        leaderMergedCrdt.setLower(i, highestLowerBound);
                        resourcesLeft--;
                    }
                }
            } else {
                // We have less resources than nodes that we got a state from. We need to coordinate from now on for every lease.
                // We assign the remaining leases to the leader for this time.

                if (finalResources) {
                    // We are already in final resource mode. This means one node has asked for one resource.
                    int indexOfRequester = nodesPorts.indexOf(leaseRequestReceivedFrom);

                    int leaderUpperCounter = leaderMergedCrdt.getUpperCounter().get(ownIndex);

                    for (int i = 0; i < nodesPorts.size(); i++) {
                        if (i == ownIndex || statesReceivedFrom.contains(nodesPorts.get(i))) {
                            leaderMergedCrdt.setUpper(i, leaderUpperCounter);
                            leaderMergedCrdt.setLower(i, leaderUpperCounter);
                        }
                    }

                    // Give requesting process one lease
                    leaderMergedCrdt.setUpper(indexOfRequester, leaderUpperCounter + 1);
                    availableResources--;

                    // Assign the remaining resources to the leader
                    leaderMergedCrdt.setUpper(ownIndex, leaderUpperCounter + availableResources);
                } else {
                    finalResources = true;

                    for (int i = 0; i < nodesPorts.size(); i++) {
                        if (i == ownIndex || statesReceivedFrom.contains(nodesPorts.get(i))) {
                            leaderMergedCrdt.setUpper(i, highestLowerBound);
                            leaderMergedCrdt.setLower(i, highestLowerBound);
                        }
                    }

                    // Assign the remaining resources to the leader
                    leaderMergedCrdt.setUpper(ownIndex, highestLowerBound + availableResources);
                }

            }
        }

        private void receivedAccepted(String messageStr) {
            numberOfAccepted++;
            // todo change number here
            if (numberOfAccepted == nodesPorts.size() - 1) {
                String message = MessageType.DECIDE.getTitle() + ":" + leaderMergedCrdt.toString();
                messageHandler.broadcast(message, nodesPorts);
                numberOfAccepted = 0;
                inCoordinationPhase = false; // End of coordination phase for leader
            }
        }

        /**
         * Matches operation message to the appropriate method.
         */
        private void matchOperationMessage(Message message) {
            switch (message.getType()) {
                case INC:
                    crdt.increment(ownIndex);
                    break;
                case DEC:
                    decrementCrdt(message);
                    break;
                case MERGE:
                    mergeCrdts(message.getContent());
                    break;
                default:
                    System.out.println("Unknown message: " + message.getContent());
            }
        }

        private void decrementCrdt(Message message) {
            if (outOfResources) {
                System.out.println("Out of resources. Cannot decrement counter.");
                return;
            }

            if (finalResources) {
                // We are working on the final resources. We need to ask the leader for every new lease.
                inCoordinationPhase = true;
                String outMessage = MessageType.REQL.getTitle() + ":" + crdt.toString();
                messageHandler.send(outMessage, leaderPort);

                // Put message right back at the front of the queue, so it will be processed next.
                operationMessageQueue.addFirst(message);
            } else {
                boolean successful = crdt.decrement(ownIndex);
                if (!successful) {
                    System.out.println("Could not decrement counter.");
                    // todo send request for lease
                } else {
                    int resourcesLeft = crdt.queryProcess(ownIndex);
                    if (resourcesLeft == 0) {
                        System.out.println("No resources left.");
                        inCoordinationPhase = true;
                        String outMessage = MessageType.REQL.getTitle() + ":" + crdt.toString();
                        messageHandler.send(outMessage, leaderPort);
                    }
                }
            }
        }
    }

    /**
     * Thread responsible for receiving messages from other nodes or clients
     */
    class MessageReceiver extends Thread {

        public void run() {
            byte[] receive;
            DatagramPacket receivePacket;

            try {
                while (true) {
                    // Clear the buffer before every message.
                    receive = new byte[65535];
                    receivePacket = new DatagramPacket(receive, receive.length);

                    // Receive message
                    socket.receive(receivePacket);

                    String receivedMessage = new String(receivePacket.getData(), 0, receivePacket.getLength());
                    System.out.println("Message from Client: " + receivedMessage);
                    Message message = new Message(receivePacket.getAddress(), receivePacket.getPort(), receivedMessage);

                    // Add message to correct queue
                    if (message.getType().isCoordinationMessage()) {
                        coordiantionMessageQueue.add(message);
                    } else {
                        operationMessageQueue.add(message);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
