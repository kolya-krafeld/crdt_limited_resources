package main;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

public class MessageHandler {

    InetAddress ip;
    private int ownPort;
    /**
     * UDP socket for receiving and sending messages.
     */
    private DatagramSocket socket;

    public MessageHandler(DatagramSocket socket, int ownPort) throws UnknownHostException {
        this.socket = socket;
        this.ip = InetAddress.getLocalHost();
        this.ownPort = ownPort;
    }

    /**
     * Send a message to a specific node.
     */
    public void send(String message, int port) {
        byte[] buf = message.getBytes();
        try {
            DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip, port);
            socket.send(sendPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Broadcast messages to all nodes in the network.
     */
    public void broadcast(String message, List<Integer> nodesPorts) {
        broadcastWithIgnore(message, nodesPorts, emptyList());
    }

    /**
     * Send a message to all nodes in the network except for the ones in the list and yourself.
     */
    public void broadcastWithIgnore(String message, List<Integer> nodesPorts, List<Integer> ignorePorts) {
        byte[] buf = message.getBytes();
        DatagramPacket sendPacket;
        for (int port : nodesPorts) {
            if (port != ownPort && !ignorePorts.contains(port)) { // No need to broadcast to yourself and to the nodes in the ignore list
                try {
                    sendPacket = new DatagramPacket(buf, buf.length, ip, port);
                    socket.send(sendPacket);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
