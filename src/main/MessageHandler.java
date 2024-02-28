package main;

import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MessageHandler {

    private int ownPort;

    public MessageHandler(int ownPort) {
        this.ownPort = ownPort;
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
