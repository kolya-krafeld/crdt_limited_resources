package main;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        List<Integer> ports = List.of(8000, 8001);
        Node node1 = new Node(8000, ports);
        node1.getCrdt().setUpper(0,10);
        node1.getCrdt().setUpper(1,10);
        node1.setLeaderPort(8000);
        node1.init();

        Node node2 = new Node(8001, ports);
        node2.getCrdt().setUpper(0,10);
        node2.getCrdt().setUpper(1,10);
        node2.setLeaderPort(8000);
        node2.init();

        CrdtChanger2 crdtChanger = new CrdtChanger2(node1, node2);

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(crdtChanger, 2, 1, TimeUnit.SECONDS);
    }

    // Just used for test purposes to see what happens when we are constantly changing the CRDTs.
    static class CrdtChanger implements Runnable {

        private final Node node1;
        private final Node node2;

        public CrdtChanger(Node node1, Node node2) {
            this.node1 = node1;
            this.node2 = node2;
        }

        public void run() {
            double random = Math.random();

            if (random < 0.5) {
                node1.getCrdt().increment(0);
                node2.getCrdt().increment(1);
            }
            if (random < 0.3){
                node1.getCrdt().decrement(0);
            }
            if (random > 0.9) {
                node2.getCrdt().decrement(1);
            }
        }
    }

    // Just used for test purposes to see what happens when we are constantly changing the CRDTs.
    static class CrdtChanger2 implements Runnable {

        private final Node node1;
        private final Node node2;
        private final Socket socket;

        public CrdtChanger2(Node node1, Node node2) {
            this.node1 = node1;
            this.node2 = node2;
            try {
                this.socket = new Socket("127.0.0.1", 8001);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void run() {
            try {
                DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                dout.writeUTF("decrement");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}