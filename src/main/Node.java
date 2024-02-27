package main;

import main.crdt.LimitedResourceCrdt;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Node in the network. Connects to other nodes and clients.
 * Responsible for its own CRDT.
 */
class Node {

    private int ownPort;

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
     * Utils object that handels sending messages.
     */
    private final MessageHandler messageHandler;

    /**
     * Flag to indicate if the node is currently in lease coordination phase.
     */
    boolean inCoordinationPhase = false;

    public Node(int port, List<Integer> nodesPorts) {
        this.ownPort = port;
        this.messageHandler = new MessageHandler(port);
        this.nodesPorts = nodesPorts;
        this.crdt = new LimitedResourceCrdt(nodesPorts.size());
    }

    public void init() throws Exception {
        MessageReceiver messageReceiver = new MessageReceiver(ownPort);
        messageReceiver.start();

        CrdtMerger merger = new CrdtMerger();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(merger, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Matches the message to the appropriate method.
     */
    public void matchMessages(String messageStr) {

        if (messageStr.startsWith("merge:")) { // CRDT merge message
            mergeCrdts(messageStr);
        } else {
            System.out.println("Unknown message: " + messageStr);
        }
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
                    matchMessages(receivedMessage);

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
