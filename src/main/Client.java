package main;

import main.crdt.Crdt;
import main.crdt.ORSet;
import main.crdt.PNCounter;
import main.utils.Message;
import main.utils.MessageType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Client class that sends requests to the system.
 */
public class Client extends Thread {

    private final List<Integer> nodePorts;
    private final List<Node> nodes;
    private final DatagramSocket socket;
    private int resourcesRequested = 0;
    private int resourcesReceived = 0;
    private int resourcesDenied = 0;
    private int totalResponses = 0;

    private Mode requestMode = Mode.RANDOM;
    private int numberOfRequest;
    private int sleepTimeBetweenRequests;

    private ScheduledExecutorService executor;
    private MessageReceiver messageReceiver;

    private boolean printReceivedMessages = true;

    // Wait object for synchronization
    Object sharedObject = new Object();

    public Client(List<Integer> nodePorts, List<Node> nodes, int numberOfRequest, int sleepTimeBetweenRequests) {
        this(nodePorts, nodes, numberOfRequest, sleepTimeBetweenRequests, 8080);
    }

    public Client(List<Integer> nodePorts, List<Node> nodes, int numberOfRequest, int sleepTimeBetweenRequests, int clientPort) {
        this.nodePorts = nodePorts;
        this.nodes = nodes;
        this.numberOfRequest = numberOfRequest;
        this.sleepTimeBetweenRequests = sleepTimeBetweenRequests;

        try {
            this.socket = new DatagramSocket(clientPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws Exception {
        Config config = new Config(100, 5, 2, 5);
        int numberOfNodes = 3;
        List<Integer> ports = new ArrayList<>();
        // Set ports
        for (int i = 0; i < numberOfNodes; i++) {
            ports.add(8000 + i);
        }

        // Create nodes
        List<Node> nodes = new ArrayList<>();
        Node node;
        for (int i = 0; i < numberOfNodes; i++) {
            node = new Node(ports.get(i), ports, config);
            node.getLimitedResourceCrdt().setUpper(i, 10);
            node.setLeaderPort(ports.get(0));
            node.init(true);
            nodes.add(node);
        }

        nodes.get(0).addPNCounterCrdt("counter");
        nodes.get(0).addORSetCrdt("set");

        // Delay coordination messages from this node
        //nodes.get(1).setAddMessageDelay(true);
        //nodes.get(2).setAddMessageDelay(true);

        Client client = new Client(ports, nodes, 50, 1000);
        client.start();

    }

    public void run() {
        messageReceiver = new MessageReceiver();
        messageReceiver.start();

        StatePrinter statePrinter = new StatePrinter();
        NodeKiller nodeKiller = new NodeKiller();
        executor = Executors.newScheduledThreadPool(2);
        //executor.scheduleAtFixedRate(resourceRequester, 2, 1, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(statePrinter, 10, 5, TimeUnit.SECONDS);
        //executor.scheduleAtFixedRate(nodeKiller, 6, 30, TimeUnit.SECONDS);


        try {
//            Thread.sleep(sleepTimeBetweenRequests);
//            for (int i = 0; i < this.numberOfRequest; i++) {
//                requestResource();
//                // incrementMonotonicCrdts();
//                Thread.sleep(sleepTimeBetweenRequests);
//            }

            // Wait for message receiver to get response for all requests
            synchronized(sharedObject) {
                sharedObject.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void stopProcesses() {
        executor.shutdown();
        messageReceiver.interrupt();
    }

    public void incrementMonotonicCrdts() {
        int indexOfNode = (int) (Math.random() * nodePorts.size());
        Node node = nodes.get(indexOfNode);

        double random = Math.random();
        Crdt counter = node.getMonotonicCrdts().get("counter");
        if (counter != null) {
            PNCounter pnCounter = (PNCounter) counter;
            if (random < 0.3) {
                pnCounter.decrement(indexOfNode);
            } else {
                pnCounter.increment(indexOfNode);
            }
        }

        Crdt set = node.getMonotonicCrdts().get("set");
        if (set != null) {
            ORSet<String> orSet = (ORSet<String>) set;
            if (random < 0.3) {
                // Remove random entry
                Set<String> currentEntries = orSet.query();
                if (currentEntries.size() == 0) {
                    return;
                }
                orSet.remove(currentEntries.stream().collect(Collectors.toList()).get((int) (Math.random() * currentEntries.size())));
            } else {
                orSet.add("element" + indexOfNode);
            }
        }
    }

    public void requestResource() {
        byte[] buf = "decrement".getBytes();
        int indexOfNode = 0;
        if (requestMode == Mode.RANDOM) {
            indexOfNode = (int) (Math.random() * nodePorts.size());
        } else if (requestMode == Mode.EXCLUDE_LEADER) {
            indexOfNode = (int) (Math.random() * (nodePorts.size() - 1)) + 1;
        } else if (requestMode == Mode.ONLY_LEADER) {
            indexOfNode = 0;
        } else if (requestMode == Mode.ONLY_FOLLOWER) {
            indexOfNode = 1;
        }

        try {
            InetAddress ip = InetAddress.getByName("localhost");
            DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip, nodePorts.get(indexOfNode));
            socket.send(sendPacket);
            resourcesRequested++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public enum Mode {
        RANDOM,
        ONLY_LEADER,
        ONLY_FOLLOWER,
        EXCLUDE_LEADER
    }

    /**
     * Kills random node at specific interval.
     */
    class NodeKiller extends Thread {

        public void run() {
            // Get random number between 1 and nodes.size()
            //int random = (int) (Math.random() * (nodes.size() - 1)) +1;
            int random = 0;

            Node node = nodes.get(random);
            System.out.println("Killing node: " + node.getOwnPort());
            node.kill();

            // Sleep for 20 sec
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            node.restart();
        }
    }

    /**
     * Prints state of Client
     */
    class StatePrinter implements Runnable {

        public void run() {
            System.out.println("State: Resources requested: " + resourcesRequested + ", resources received: " + resourcesReceived + ", resources denied: " + resourcesDenied);
        }
    }

    /**
     * Thread responsible for receiving messages from other node
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
                    if (printReceivedMessages) System.out.println("Message from Node: " + receivedMessage);
                    Message message = new Message(receivePacket.getAddress(), receivePacket.getPort(), receivedMessage);

                    if (message.getType().equals(MessageType.DENY_RES)) {
                        resourcesDenied++;
                        totalResponses++;
                    } else if (message.getType().equals(MessageType.APPROVE_RES)) {
                        resourcesReceived++;
                        totalResponses++;
                    }

                    if (totalResponses == numberOfRequest) {
                        // Notify waiting Client thread
                        synchronized(sharedObject) {
                            sharedObject.notify();
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void setRequestMode(Mode requestMode) {
        this.requestMode = requestMode;
    }

    public int getResourcesRequested() {
        return resourcesRequested;
    }

    public int getResourcesReceived() {
        return resourcesReceived;
    }

    public int getResourcesDenied() {
        return resourcesDenied;
    }

    public void setPrintReceivedMessages(boolean printReceivedMessages) {
        this.printReceivedMessages = printReceivedMessages;
    }
}