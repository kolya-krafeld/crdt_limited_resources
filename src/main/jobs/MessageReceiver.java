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

    private int decMessagesReceived = 0;

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
                this.node.failureDetector.addTimeWhenWeReceivedLastMessageFromNode(message.getPort());

                //if message was forwarded to leader, we need to extract the original message
                if (message.getType() == MessageType.FORWARDED_TO_LEADER) {
                    message = Message.messageFromForwaredMessage(message.getContent());
                }

                if (message.getType() == null) {
                    logger.error("Message type is null");
                    logger.error(message.toString());
                }

                // If message is for leader, but node is not the leader, forward the message to the leader
                if (message.getType() != null && message.getType().isForLeaderMessage() && !node.isLeader()) {
                    logger.warn("Message is for leader, but this node is not the leader, forwarding message to the leader: " + node.getLeaderPort());
                    String forwardMessage = MessageType.FORWARDED_TO_LEADER + ":" + message;
                    node.messageHandler.sendToLeader(forwardMessage);
                } else {
                    // Add message to the correct queue for processing
                    if (message.getType() == MessageType.MERGE_MONOTONIC) {
                        node.monotonicCrdtMessageQueue.add(message);
                    } else if (message.getType().isFindLeaderMessage()) {
                        node.findLeaderMessageQueue.add(message);
                    } else if (message.getType().heartbeatOrLeaderElectionMessage()) {
                        node.heartbeatAndElectionMessageQueue.add(message);
                    } else if (message.getType().isPreparePhaseMessage()) {
                        node.preparePhaseMessageQueue.add(message);
                    } else if (message.getType().isCoordinationMessage()) {
                        node.coordinationMessageQueue.add(message);
                    } else {
                        if (message == null) {
                            logger.error("Message is null");
                        }

                        // Todo remove: only used for benchmarking
                        if (message.getType() == MessageType.DEC) {
                            decMessagesReceived++;
                            if (decMessagesReceived % 10000 == 0)
                                System.out.println("Received " + decMessagesReceived + " dec messages");
                        }
                        node.operationMessageQueue.add(message);
                    }
                }
            }
        } catch (SocketException se) {
            logger.info("Socket exception: " + se.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stopReceiver() {
        stopped = true;
    }
}
