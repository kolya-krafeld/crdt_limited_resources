package main;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MessageHandler {

    InetAddress ip;
    private int ownPort;
    /**
     * UDP socket for receiving and sending messages.
     */
    private DatagramSocket socket;

    public MessageHandler(int ownPort) {
        this.ownPort = ownPort;
    }

    public MessageHandler(DatagramSocket socket, int ownPort) throws UnknownHostException {
        this.socket = socket;
        this.ip = InetAddress.getLocalHost();
        this.ownPort = ownPort;
    }

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
     * Send a message to a specific node.
     */
    public void send(String message, int port, ConcurrentHashMap<Integer, Socket> nodeOutputSockets) {
        try {
            Socket socket;
            if (nodeOutputSockets.containsKey(port)) {
                socket = nodeOutputSockets.get(port);
            } else {
                socket = new Socket("127.0.0.1", port);
                nodeOutputSockets.put(port, socket);
            }
            DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
            dout.writeUTF(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Broadcast messages to all nodes in the network.
     */
    public void broadcast(String message, List<Integer> nodesPorts) {
        byte[] buf = message.getBytes();
        DatagramPacket sendPacket;
        for (int port : nodesPorts) {
            if (port != ownPort) { // No need to broadcast to yourself
                try {
                    sendPacket = new DatagramPacket(buf, buf.length, ip, port);
                    socket.send(sendPacket);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Broadcast messages to all nodes in the network.
     */
    public void broadcast(String message, List<Integer> nodesPorts, ConcurrentHashMap<Integer, Socket> nodeOutputSockets) {
        for (int port : nodesPorts) {
            if (port != ownPort) { // No need to broadcast to yourself
                try {
                    Socket socket;
                    if (nodeOutputSockets.containsKey(port)) {
                        socket = nodeOutputSockets.get(port);
                    } else {
                        socket = new Socket("127.0.0.1", port);
                        nodeOutputSockets.put(port, socket);
                    }
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
