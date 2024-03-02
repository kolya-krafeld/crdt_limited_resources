package test.failure_detector;

import main.Config;
import main.Node;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


class FailureDetectorTest {

    @Test
    void testFailureDetector() throws InterruptedException {

        Config config = new Config(500, 3, 1);

        List<Integer> ports = List.of(8000, 8001, 8002, 8003, 8004);
        List<Node> nodes = new ArrayList<>();
        ports.forEach(port -> {
            Node node = new Node(port, ports, config);
            try {
                node.init();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            nodes.add(node);
        });

        Thread.sleep(2000);
        for (Node node : nodes) {
            assertEquals(5, node.failureDetector.numberOfConnectedNodes());
            assertEquals(true, node.failureDetector.isConnectedToQuorum());
        }


        for (int i = 0; i < 2; i++) {
            nodes.get(i).socket.close();
        }
        Thread.sleep(3000);

        for (Node node : nodes) {
            if (node.getOwnPort() < 8002) {
                assertEquals(5, node.failureDetector.numberOfConnectedNodes());
                assertEquals(true, node.failureDetector.isConnectedToQuorum());
            } else {
                assertEquals(3, node.failureDetector.numberOfConnectedNodes());
                assertEquals(true, node.failureDetector.isConnectedToQuorum());
            }
        }

        nodes.get(2).socket.close();
        Thread.sleep(3000);

        for (Node node : nodes) {
            if (node.getOwnPort() < 8002) {
                assertEquals(5, node.failureDetector.numberOfConnectedNodes());
                assertEquals(true, node.failureDetector.isConnectedToQuorum());
            } else if (node.getOwnPort() == 8002) {
                assertEquals(3, node.failureDetector.numberOfConnectedNodes());
                assertEquals(true, node.failureDetector.isConnectedToQuorum());
            } else {
                assertEquals(2, node.failureDetector.numberOfConnectedNodes());
                assertEquals(false, node.failureDetector.isConnectedToQuorum());
            }
        }

    }

}
