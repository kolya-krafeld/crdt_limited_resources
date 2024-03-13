package main.utils;

import main.Node;

import java.io.IOException;
import java.net.*;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Class responsible for sending messages to other nodes or clients.
 * Provides send and broadcast methods.
 */
public class MessageHandler {


    Node node;
    InetAddress ip;
    private int ownPort;
    /**
     * UDP socket for receiving and sending messages.
     */
    private DatagramSocket socket;

    public MessageHandler(Node node, DatagramSocket socket, int ownPort) throws UnknownHostException {
        this.node = node;
        this.socket = socket;
        this.ip = InetAddress.getLocalHost();
        this.ownPort = ownPort;
    }

    /**
     * Send a message to the leader.
     */
    public void sendToLeader(String message) {
        send(message, node.getLeaderPort());
    }

    /**
     * Send a message to a specific node.
     */
    public void send(String message, int port) {
        byte[] buf = message.getBytes();
        try {
            DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip, port);
            socket.send(sendPacket);
        } catch (SocketException se) {
            if (!se.getMessage().equals("Socket closed")) {
                System.out.println(node.getOwnPort() + " Socket exception: " + se.getMessage());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Broadcast messages to all nodes in the network.
     */
    public void broadcast(String message) {
        broadcastWithIgnore(message, node.getNodesPorts(), emptyList(), false);
    }

    /**
     * Send a message to all nodes in the network except for the ones in the list and yourself.
     */
    public void broadcastWithIgnore(String message, List<Integer> nodesPorts, List<Integer> ignorePorts, boolean sendToYourself) {
        byte[] buf = message.getBytes();
        DatagramPacket sendPacket;
        for (int port : nodesPorts) {
            if (sendToYourself || (port != ownPort && !ignorePorts.contains(port))) { // No need to broadcast to yourself and to the nodes in the ignore list
                try {
                    sendPacket = new DatagramPacket(buf, buf.length, ip, port);
                    socket.send(sendPacket);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void broadcastIncludingSelf(String message) {
        broadcastWithIgnore(message, node.getNodesPorts(), emptyList(), true);
    }

    public void setSocket(DatagramSocket socket) {
        this.socket = socket;
    }
}
