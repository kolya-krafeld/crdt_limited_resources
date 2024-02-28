package main;

import main.crdt.LimitedResourceCrdt;
import main.utils.MessageType;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;


/**
 * Node in the network. Connects to other nodes and clients.
 * Responsible for its own CRDT.
 */
class Node {

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
    private int numberOfAccepted = 0;

    /**
     * Utils object that handels sending messages.
     */
    private final MessageHandler messageHandler;

    /**
     * Flag to indicate if the node is currently in lease coordination phase.
     */
    boolean inCoordinationPhase = false;

    /**
     * Queue of messages to be processed outside of coordination phase. Using concurrent queue to make it thread safe.
     */
    Queue<String> operationMessageQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue of messages to be processed. Using concurrent queue to make it thread safe.
     */
    Queue<String> coordiantionMessageQueue = new ConcurrentLinkedQueue<>();

    public Node(int port, List<Integer> nodesPorts) {
        this.ownPort = port;
        this.messageHandler = new MessageHandler(port);
        this.nodesPorts = nodesPorts;
        this.crdt = new LimitedResourceCrdt(nodesPorts.size());

        // Get own index in port list
        int ownIndex = nodesPorts.indexOf(port);
        if (ownIndex == -1) {
            throw new IllegalArgumentException("Port not in list of nodes.");
        }
        this.ownIndex = ownIndex;
    }

    public void init() throws Exception {
        MessageReceiver messageReceiver = new MessageReceiver(ownPort);
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
    private void mergeCrdts(String messageStr) {
        String[] parts = messageStr.split(":");
        String crdtString = parts[1];
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
                messageHandler.broadcast(message, nodesPorts, nodeOutputSockets);
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
                    String message = operationMessageQueue.poll();
                    matchOperationMessage(message);
                }
            }
        }

        /**
         * Matches coordination message to the appropriate method.
         */
        private void matchCoordinationMessage(String messageStr) {
            MessageType messageType = MessageType.getMessageTypeFromMessageString(messageStr);
            String crdtString, message;
            switch (messageType) {
                case REQL:
                    inCoordinationPhase = true;
                    leaderMergedCrdt = crdt; // set leader crdt first
                    crdtString = messageStr.split(":")[1];
                    leaderMergedCrdt.merge(new LimitedResourceCrdt(crdtString)); // merge with received crdt

                    // todo dont broadcast here
                    messageHandler.broadcast(MessageType.REQS.getTitle(), nodesPorts, nodeOutputSockets);
                    break;
                case REQS:
                    inCoordinationPhase = true;
                    message = MessageType.STATE.getTitle() + ":" + crdt.toString();
                    messageHandler.send(message, leaderPort, nodeOutputSockets);
                    break;
                case STATE:
                    receiveState(messageStr);
                    break;
                case ACCEPT:
                    crdtString = messageStr.split(":")[1];
                    acceptedCrdt = new LimitedResourceCrdt(crdtString);
                    messageHandler.send(MessageType.ACCEPTED.getTitle(), leaderPort, nodeOutputSockets);
                    break;
                case ACCEPTED:
                    receivedAccepted(messageStr);
                    break;
                case DECIDE:
                    crdtString = messageStr.split(":")[1];
                    LimitedResourceCrdt mergingCrdt = new LimitedResourceCrdt(crdtString);
                    crdt.merge(mergingCrdt);
                    System.out.println("Received decided state: " + crdtString);
                    inCoordinationPhase = false; // End of coordination phase
                    break;
                default:
                    System.out.println("Unknown message: " + messageStr);
            }
        }

        private void receiveState(String messageStr) {
            String crdtString = messageStr.split(":")[1];
            LimitedResourceCrdt state = new LimitedResourceCrdt(crdtString);
            leaderMergedCrdt.merge(state);
            numberOfStates++;
            // todo change number here
            if (numberOfStates == nodesPorts.size() - 1) {

                // Do maths here
                int availableResources = leaderMergedCrdt.query();
                System.out.println("Available resources: " + availableResources);
                int resourcesPerNode = availableResources / nodesPorts.size();
                int resourcesLeft = availableResources % nodesPorts.size();
                int highestLowerBound = leaderMergedCrdt.getLowerCounter().stream().max(Integer::compare).get();
                for (int i = 0; i < nodesPorts.size(); i++) {
                    int additional = resourcesLeft > 0 ? 1 : 0;
                    leaderMergedCrdt.setUpper(i, highestLowerBound + resourcesPerNode + additional);
                    leaderMergedCrdt.setLower(i, highestLowerBound);
                    resourcesLeft--;
                }
                System.out.println("Leader proposes state: " + leaderMergedCrdt);



                String message = MessageType.ACCEPT.getTitle() + ":" + leaderMergedCrdt.toString();
                messageHandler.broadcast(message, nodesPorts, nodeOutputSockets);
                numberOfStates = 0;
            }
        }

        private void receivedAccepted(String messageStr) {
            if (numberOfAccepted == nodesPorts.size() - 1) {
                String message = MessageType.DECIDE.getTitle() + ":" + leaderMergedCrdt.toString();
                messageHandler.broadcast(message, nodesPorts, nodeOutputSockets);
                numberOfAccepted = 0;
                inCoordinationPhase = false; // End of coordination phase for leader
            }
        }

        /**
         * Matches operation message to the appropriate method.
         */
        private void matchOperationMessage(String messageStr) {
            MessageType messageType = MessageType.getMessageTypeFromMessageString(messageStr);
            switch (messageType) {
                case INC:
                    crdt.increment(ownIndex);
                    break;
                case DEC:
                    boolean successful = crdt.decrement(ownIndex);
                    if (!successful) {
                        System.out.println("Could not decrement counter.");
                        // todo send request for lease
                    } else {
                        int resourcesLeft = crdt.queryProcess(ownIndex);
                        if (resourcesLeft == 0) {
                            System.out.println("No resources left.");
                            inCoordinationPhase = true;
                            String message = MessageType.REQL.getTitle() + ":" + crdt.toString();
                            messageHandler.send(message, leaderPort, nodeOutputSockets);
                        }
                    }
                    break;
                case MERGE:
                    mergeCrdts(messageStr);
                    break;
                default:
                    System.out.println("Unknown message: " + messageStr);
            }
        }
    }

    /**
     * Thread responsible for receiving messages from other nodes or clients
     */
    class MessageReceiver extends Thread {
        // Receiving socket
        private final ServerSocket serverSocket;

        public MessageReceiver(int port) throws IOException {
            this.serverSocket = new ServerSocket(port);
        }

        public void run() {
            Socket clientSocket = null;
            try {
                System.out.println("Receiver created!");
                clientSocket = serverSocket.accept();
                System.out.println("Client connected");

                DataInputStream din = new DataInputStream(clientSocket.getInputStream());
                while (true) {
                    String receivedMessage = din.readUTF();
                    System.out.println("Message from Client: " + receivedMessage);

                    // Add message to correct queue
                    MessageType messageType = MessageType.getMessageTypeFromMessageString(receivedMessage);
                    if (messageType.isCoordinationMessage()) {
                        coordiantionMessageQueue.add(receivedMessage);
                    } else {
                        operationMessageQueue.add(receivedMessage);
                    }

                    if (receivedMessage.equalsIgnoreCase("bye")) {
                        System.out.println("Client left");
                        clientSocket.close();
                        serverSocket.close();
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (clientSocket != null)
                        clientSocket.close();

                    if (serverSocket != null)
                        serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
