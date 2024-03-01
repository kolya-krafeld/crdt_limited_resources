package test.failure_detector;

import main.Config;
import main.Node;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;


class FailureDetectorTest {

    @Test
    void testFailureDetector() throws InterruptedException {

        Config config = new Config(100,5, 2);

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

         Thread.sleep(1000);
         for (Node node : nodes) {
             System.out.println(
                     "Node " + nodes.indexOf(node) + "  connected to:  " + node.numberOfConnectedNodes() + "  Quorum connected: " + node.isConnectedToQuorum());
         }

//             for (int i = 0; i < 2; i++) {
//                 nodes.get(i).stopFailureDetector();
//             }
//
//             System.out.println("stopping failure detector for nodes 1 and 2");
//
//             Thread.sleep(1000);
//             for (Node node : nodes) {
//                 System.out.println(
//                     "Node " + nodes.indexOf(node) + "  connected to:  " + node.numberOfConnectedNodes() + "  Quorum connected: " + node.isConnectedToQuorum());
//                 }
//
//             nodes.get(2).stopFailureDetector();
//
//             System.out.println("stopping failure detector for node 3");
//
//             Thread.sleep(1000);
//             for (Node node : nodes) {
//                 System.out.println(
//                     "Node " + nodes.indexOf(node) + "  connected to:  " + node.numberOfConnectedNodes() + "  Quorum connected: " + node.isConnectedToQuorum());
//                 }
//
//             for (Node node : nodes) {
//                 node.stopFailureDetector();
//             }
//          }

    }

}