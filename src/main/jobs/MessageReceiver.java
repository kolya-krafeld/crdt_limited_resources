package main.jobs;

import main.Node;
import main.utils.Message;
import main.utils.MessageType;

import java.io.IOException;
import java.net.DatagramPacket;

/**
 * Thread responsible for receiving messages from other nodes or clients.
 * Messages are then added to the correct queue for processing.
 */
public class MessageReceiver extends Thread {

    private Node node;

    public MessageReceiver(Node node) {
        this.node = node;
    }

    public void run() {
        byte[] receive;
        DatagramPacket receivePacket;

        try {
            while (true) {
                // Clear the buffer before every message.
                receive = new byte[65535];
                receivePacket = new DatagramPacket(receive, receive.length);

                // Receive message
                node.socket.receive(receivePacket);

                String receivedMessage = new String(receivePacket.getData(), 0, receivePacket.getLength());
                System.out.println("Message from Client: " + receivedMessage);
                Message message = new Message(receivePacket.getAddress(), receivePacket.getPort(), receivedMessage);

                // Add message to correct queue, Heartbeats get handled immediately
                if(message.getType() == MessageType.HEARTBEAT_PING) {
                    node.failureDetector.sendHeartbeatPong(message.getPort());
                } else if (message.getType() == MessageType.HEARTBEAT_PONG) {
                    node.failureDetector.updateNodeStatus(message.getPort());
                }
                else if (message.getType().isCoordinationMessage()) {
                    node.coordiantionMessageQueue.add(message);
                } else {
                    node.operationMessageQueue.add(message);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
