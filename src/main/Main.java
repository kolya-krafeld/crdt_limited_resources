package main;

import main.utils.Message;
import main.utils.MessageType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {

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
            node = new Node(ports.get(i), ports);
            node.getCrdt().setUpper(i, 10);
            node.setLeaderPort(ports.get(0));
            node.init();
            nodes.add(node);
        }

        // Delay coordination messages from this node
        nodes.get(nodes.size() - 1).setAddMessageDelay(true);

        Client client = new Client(ports, nodes);
        client.init();

    }

    static class Client {

        enum Mode {
            RANDOM,
            ONLY_LEADER,
            ONLY_FOLLOWER
        }

        private final Mode mode = Mode.RANDOM;

        private final List<Integer> nodePorts;
        private final List<Node> nodes;
        private final DatagramSocket socket;

        private int resourcesRequested = 0;
        private int resourcesReceived = 0;
        private int resourcesDenied = 0;

        public Client(List<Integer> nodePorts, List<Node> nodes) {
            this.nodePorts = nodePorts;
            this.nodes = nodes;

            try {
                this.socket = new DatagramSocket(8080);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void init() {
            MessageReceiver messageReceiver = new MessageReceiver();
            messageReceiver.start();

            StatePrinter statePrinter = new StatePrinter();
            NodeKiller nodeKiller = new NodeKiller();
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
            //executor.scheduleAtFixedRate(resourceRequester, 2, 1, TimeUnit.SECONDS);
            executor.scheduleAtFixedRate(statePrinter, 10, 5, TimeUnit.SECONDS);
            //executor.scheduleAtFixedRate(nodeKiller, 6, 30, TimeUnit.SECONDS);


            try {
                Thread.sleep(2000);
                for (int i = 0; i < 60; i++) {
                    requestResource();
                    Thread.sleep(1000);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void requestResource() {
            byte[] buf = "decrement".getBytes();
            int indexOfNode = 0;
            if (mode == Mode.RANDOM) {
                indexOfNode = (int) (Math.random() * nodePorts.size());
            } else if (mode == Mode.ONLY_LEADER) {
                indexOfNode = 0;
            } else if (mode == Mode.ONLY_FOLLOWER) {
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

        /**
         * Kills random follower node at specific interval.
         */
        class NodeKiller extends Thread {

            public void run() {
                // Get random number between 1 and nodes.size()
                //int random = (int) (Math.random() * (nodes.size() - 1)) +1;
                int random = 1;

                Node node = nodes.get(random);
                if (node.isLeader()) {
                    return;
                }
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
                        System.out.println("Message from Node: " + receivedMessage);
                        Message message = new Message(receivePacket.getAddress(), receivePacket.getPort(), receivedMessage);

                        if (message.getType().equals(MessageType.DENY_RES)) {
                            resourcesDenied++;
                        } else if (message.getType().equals(MessageType.APPROVE_RES)) {
                            resourcesReceived++;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}