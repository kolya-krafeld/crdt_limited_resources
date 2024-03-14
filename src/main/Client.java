package main;

import main.crdt.Crdt;
import main.crdt.ORSet;
import main.crdt.PNCounter;
import main.utils.Message;
import main.utils.MessageType;
import main.utils.NodeKiller;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Client class that sends requests to nodes in the system.
 */
public class Client extends Thread {

    /**
     * Ports of nodes in the system.
     */
    private final List<Integer> nodePorts;

    /**
     * List of nodes in the system.
     */
    private final List<Node> nodes;

    /**
     * UDP socket for sending messages to and receiving messages from nodes.
     */
    private final DatagramSocket socket;
    /**
     * Object for synchronization between threads.
     */
    Object sharedObject = new Object();

    // Counters for tracking number of resources requested, received and denied
    private int resourcesRequested = 0;
    private int resourcesReceived = 0;
    private int resourcesDenied = 0;
    private int totalResponses = 0;

    /**
     * Mode for sending requests to nodes. Random, only leader, only follower, exclude leader.
     */
    private MessageDistributionMode requestMode = MessageDistributionMode.RANDOM;

    /**
     * Mode for client. Normal, test, benchmark.
     */
    private Mode mode;

    /**
     * Number of requests to send to nodes.
     */
    private int numberOfRequest;
    /**
     * Time to sleep between sending requests.
     */
    private int sleepTimeBetweenRequests;
    /**
     * Time to sleep between killing nodes.
     */
    private ScheduledExecutorService executor;
    private MessageReceiver messageReceiver;
    private boolean printReceivedMessages = true;
    private boolean killNodes = false;
    private NodeKiller.NodeKillerType nodeKillerType = NodeKiller.NodeKillerType.SINGLE_FOLLOWER;

    public Client(List<Integer> nodePorts, List<Node> nodes, int numberOfRequest, int sleepTimeBetweenRequests, Mode mode) {
        this(nodePorts, nodes, numberOfRequest, sleepTimeBetweenRequests, 8080, mode);
    }

    public Client(List<Integer> nodePorts, List<Node> nodes, Mode mode) {
        this(nodePorts, nodes, -1, -1, 8080, mode);
    }

    public Client(List<Integer> nodePorts, List<Node> nodes, int numberOfRequest, int sleepTimeBetweenRequests, int clientPort, Mode mode) {
        this.nodePorts = nodePorts;
        this.nodes = nodes;
        this.numberOfRequest = numberOfRequest;
        this.sleepTimeBetweenRequests = sleepTimeBetweenRequests;
        this.mode = mode;
        try {
            this.socket = new DatagramSocket(clientPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws Exception {
        int numberOfNodes = 3;
        int numberOfResourcesPerNode = 10;
        int numberOfRequests = 100;
        boolean addMessageDelay = false;
        boolean killNodes = false;
        int messageRequestDelay = 750;
        MessageDistributionMode requestMode = MessageDistributionMode.RANDOM;


        Config config = new Config(100, 5, 2, 5);
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
            node.getLimitedResourceCrdt().setUpper(i, numberOfResourcesPerNode);
            node.setLeaderPort(ports.get(0));
            node.init(true);
            nodes.add(node);
        }

        // Add monotonic CRDTs to nodes
        nodes.get(0).addPNCounterCrdt("counter");
        nodes.get(0).addORSetCrdt("set");

        // Delay coordination messages from this node
        if (addMessageDelay) {
            nodes.get(1).setAddMessageDelay(true);
            nodes.get(2).setAddMessageDelay(true);
        }


        Client client = new Client(ports, nodes, numberOfRequests, messageRequestDelay, Mode.NORMAL);
        client.setRequestMode(requestMode);
        if (killNodes) {
            client.setKillNodes(killNodes);
            client.setNodeKillerType(NodeKiller.NodeKillerType.RANDOM);
        }
        client.start();
    }

    public void run() {
        messageReceiver = new MessageReceiver();
        messageReceiver.start();
        executor = Executors.newScheduledThreadPool(3);
        StatePrinter statePrinter = new StatePrinter();
        executor.scheduleAtFixedRate(statePrinter, 5, 5, TimeUnit.SECONDS);

        if (killNodes) {
            // Kill nodes every 5 seconds
            NodeKiller nodeKiller = new NodeKiller(nodeKillerType, nodes, true, 2000);
            executor.scheduleAtFixedRate(nodeKiller, 5, 8, TimeUnit.SECONDS);
        }

        try {
            if (mode == Mode.TEST) {
                if (requestMode != MessageDistributionMode.SPECIFIC_NODES) {
                    for (int i = 0; i < this.numberOfRequest; i++) {
                        Thread.sleep(sleepTimeBetweenRequests);
                        requestLimitedResource();
                    }
                }
            } else if (mode == Mode.NORMAL) {
                for (int i = 0; i < this.numberOfRequest; i++) {
                    Thread.sleep(sleepTimeBetweenRequests);
                    updateMonotonicCrdts();
                    requestLimitedResource();
                }
            }
            // Wait for message receiver to get response for all requests
            synchronized (sharedObject) {
                sharedObject.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void stopProcesses() {
        executor.shutdown();
        messageReceiver.interrupt();

        socket.close();
    }

    /**
     * Method request random increment/decrement or add/remove on monotonic CRDTs with identifier 'counter' & 'set'.
     */
    public void updateMonotonicCrdts() {
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

    /**
     * Method to request limited resource from node.
     * Mode determines whether we send request to random node, the leader or specific follower.
     */
    public void requestLimitedResource(int nodeIndex) {
        byte[] buf = "decrement".getBytes();
        int indexOfNode = 0;
        if (requestMode == MessageDistributionMode.RANDOM) {
            // Send request to random node
            indexOfNode = (int) (Math.random() * nodePorts.size());
        } else if (requestMode == MessageDistributionMode.EXCLUDE_LEADER) {
            // Send request to random node, excluding the leader
            indexOfNode = (int) (Math.random() * (nodePorts.size() - 1)) + 1;
        } else if (requestMode == MessageDistributionMode.ONLY_LEADER) {
            // Send request to leader (assuming leader is first node in list)
            indexOfNode = 0;
        } else if (requestMode == MessageDistributionMode.SINGLE_FOLLOWER) {
            // Send request to first follower node
            indexOfNode = 1;
        } else if (requestMode == MessageDistributionMode.SPECIFIC_NODES) {
            indexOfNode = nodeIndex;
        }

        try {
            // Send udp request
            InetAddress ip = InetAddress.getByName("localhost");
            DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip, nodePorts.get(indexOfNode));
            socket.send(sendPacket);
            resourcesRequested++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void requestLimitedResource() {
        requestLimitedResource(-1);
    }

    public void setRequestMode(MessageDistributionMode requestMode) {
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

    public void setKillNodes(boolean killNodes) {
        this.killNodes = killNodes;
    }

    public void setNodeKillerType(NodeKiller.NodeKillerType nodeKillerType) {
        this.nodeKillerType = nodeKillerType;
    }

    public void setPrintReceivedMessages(boolean printReceivedMessages) {
        this.printReceivedMessages = printReceivedMessages;
    }

    public enum MessageDistributionMode {
        RANDOM,
        ONLY_LEADER,
        SINGLE_FOLLOWER,
        EXCLUDE_LEADER,
        SPECIFIC_NODES
    }

    public enum Mode {
        NORMAL,
        TEST,
        BENCHMARK
    }

    /**
     * Thread that periodically prints state of Client
     */
    class StatePrinter implements Runnable {

        public void run() {
            System.out.println("------------------------");
            System.out.println("STATE");
            System.out.println("Non-monotonic CRDT: Resources requested: " + resourcesRequested + ", resources received: " + resourcesReceived + ", resources denied: " + resourcesDenied);
            System.out.println("Monotonic CRDTs:");
            Node node = nodes.get(1);
            for (Map.Entry<String, Crdt> entry : node.getMonotonicCrdts().entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue().query());
            }
            System.out.println("------------------------");
        }
    }

    /**
     * Thread responsible for receiving messages from nodes
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

                    while (totalResponses == numberOfRequest) {
                        // Notify waiting Client thread
                        synchronized (sharedObject) {
                            sharedObject.notify();
                        }
                    }
                }
            } catch (SocketException se) {
                if (!se.getMessage().equals("Socket closed")) {
                    System.out.println("Socket exception in client: " + se.getMessage());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}