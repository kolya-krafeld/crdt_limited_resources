package main.jobs;

import main.Node;
import main.utils.Logger;
import main.utils.Message;
import main.utils.MessageType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketException;

/**
 * Thread responsible for receiving messages from other nodes or clients.
 * Messages are then added to the correct queue for processing.
 */
public class MessageReceiver extends Thread {

    private final Logger logger;

    private Node node;

    private boolean stopped = false;

    public MessageReceiver(Node node) {
        this.node = node;
        this.logger = node.logger;
    }



    public void run() {
        byte[] receive;
        DatagramPacket receivePacket;

        try {
            while (!stopped) {
                // Clear the buffer before every message.
                receive = new byte[65535];
                receivePacket = new DatagramPacket(receive, receive.length);

                // Receive message
                node.socket.receive(receivePacket);

                String receivedMessage = new String(receivePacket.getData(), 0, receivePacket.getLength());

                if (!receivedMessage.contains("heartbeat")) {
                    logger.info("Message received from " + receivePacket.getPort() + ": " + receivedMessage);
                }
                Message message = new Message(receivePacket.getAddress(), receivePacket.getPort(), receivedMessage);

                // Add message to the correct queue for processing
                MessageType mT = message.getType();
                if(message.getType().isLeaderElectionMessage()) {
                    node.heartbeatAndElectionMessageQueue.add(message);
                } else if (message.getType().isPreparePhaseMessage()) {
                    node.preparePhaseMessageQueue.add(message);
                } else if (message.getType().isCoordinationMessage()) {
                    node.coordinationMessageQueue.add(message);
                } else {
                    if (message == null) {
                        logger.error("Message is null");
                    }
                    node.operationMessageQueue.add(message);
                }
            }
        } catch (SocketException se) {
            logger.error("Socket exception: " + se.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stopReceiver() {
        stopped = true;
    }
}
