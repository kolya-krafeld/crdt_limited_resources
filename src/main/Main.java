package main;

import java.io.IOException;
import java.net.*;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import main.utils.Message;
import main.utils.MessageType;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = new Config(100,5, 2);
        List<Integer> ports = List.of(8000, 8001, 8002);
        Node node1 = new Node(8000, ports, config);
        node1.getCrdt().setUpper(0, 10);
        node1.getCrdt().setUpper(1, 10);
        node1.setLeaderPort(8000);
        node1.init();

        Node node2 = new Node(8001, ports, config);
        node2.getCrdt().setUpper(0, 10);
        node2.getCrdt().setUpper(1, 10);
        node2.setLeaderPort(8000);
        node2.init();

        Node node3 = new Node(8002, ports, config);
        node3.getCrdt().setUpper(0, 10);
        node3.getCrdt().setUpper(2, 10);
        node3.setLeaderPort(8000);
        node3.init();

        Client client = new Client(node1, node2);
        client.init();


//        CrdtChanger2 crdtChanger = new CrdtChanger2(node1, node2);
//
//        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
//        executor.scheduleAtFixedRate(crdtChanger, 2, 1, TimeUnit.SECONDS);
    }

    static class Client {

        private final Node node1;
        private final Node node2;
        private final DatagramSocket socket;

        private int resourcesRequested = 0;
        private int resourcesReceived = 0;
        private int resourcesDenied = 0;

        public Client(Node node1, Node node2) {
            this.node1 = node1;
            this.node2 = node2;
            try {
                this.socket = new DatagramSocket(8080);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void init() {

            MessageReceiver messageReceiver = new MessageReceiver();
            messageReceiver.start();

            ResourceRequester resourceRequester = new ResourceRequester();
            StatePrinter statePrinter = new StatePrinter();
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            //executor.scheduleAtFixedRate(resourceRequester, 2, 1, TimeUnit.SECONDS);
            executor.scheduleAtFixedRate(statePrinter, 10, 5, TimeUnit.SECONDS);

            try {
                Thread.sleep(2000);
                for (int i = 0; i < 40; i++) {
                    requestResource();
                    Thread.sleep(1000);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void requestResource() {
            byte[] buf = "decrement".getBytes();
            try {
                InetAddress ip = InetAddress.getByName("localhost");
                DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip, 8001);
                socket.send(sendPacket);
                resourcesRequested++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Requests resources from the nodes.
         */
        class ResourceRequester implements Runnable {

            public void run() {
                byte[] buf = "decrement".getBytes();
                try {
                    InetAddress ip = InetAddress.getByName("localhost");
                    DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip, 8001);
                    socket.send(sendPacket);
                    resourcesRequested++;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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

                        if (message.getType().equals(MessageType.DENYR)) {
                            resourcesDenied++;
                        } else if (message.getType().equals(MessageType.APPROVER)) {
                            resourcesReceived++;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    // Just used for test purposes to see what happens when we are constantly changing the CRDTs.
    static class CrdtChanger2 implements Runnable {

        private final Node node1;
        private final Node node2;
        private final DatagramSocket socket;

        public CrdtChanger2(Node node1, Node node2) {
            this.node1 = node1;
            this.node2 = node2;
            try {
                this.socket = new DatagramSocket(8080);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void run() {
            byte[] buf = "decrement".getBytes();
            try {
                InetAddress ip = InetAddress.getByName("localhost");
                DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip, 8001);
                socket.send(sendPacket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


}