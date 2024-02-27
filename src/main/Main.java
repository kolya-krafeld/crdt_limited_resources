package main;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        List<Integer> ports = List.of(8000, 8001);
        Node node1 = new Node(8000, ports);
        node1.init();

        Node node2 = new Node(8001, ports);
        node2.init();

        CrdtChanger crdtChanger = new CrdtChanger(node1, node2);

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
}