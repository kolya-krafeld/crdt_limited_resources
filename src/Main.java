import java.util.ArrayList;
import java.util.List;

import config.Config;
import crdt.LimitedResourceCrdt;
import node.Node;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // LimitedResourceCrdt crdt1 = new LimitedResourceCrdt(3);
        // LimitedResourceCrdt crdt2 = new LimitedResourceCrdt(3);

        // crdt1.setUpper(0, 10);
        // crdt1.setUpper(1, 10);
        // crdt1.setUpper(2, 10);

        // crdt2.merge(crdt1);
        // System.out.println("Compare both crdts:" + crdt2.compare(crdt1));

        // System.out.println("Try to set lower counter above legal value: " + crdt2.setLower(0, 12));

        testFailureDetector();

    }



    public static void testFailureDetector() throws InterruptedException {

        Config config = new Config(1000, 400);
        // Erstellen Sie eine Liste von Knoten
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            nodes.add(new Node(i, nodes, config));
        }

        for (Node node : nodes) {
            node.startFailureDetector();
        }

        Thread.sleep(1000);
        for (Node node : nodes) {
            System.out.println(
                    "Node " + nodes.indexOf(node) + "  connected to:  " + node.numberOfConnectedNodes() + "  Quorum connected: " + node.isConnectedToQuorum());
        }
    
        for (int i = 0; i < 2; i++) {
            nodes.get(i).stopFailureDetector();
        }

        System.out.println("stopping failure detector for nodes 1 and 2");

        Thread.sleep(1000);
        for (Node node : nodes) {
            System.out.println(
                "Node " + nodes.indexOf(node) + "  connected to:  " + node.numberOfConnectedNodes() + "  Quorum connected: " + node.isConnectedToQuorum());
            }

        nodes.get(2).stopFailureDetector();

        System.out.println("stopping failure detector for node 3");

        Thread.sleep(1000);
        for (Node node : nodes) {
            System.out.println(
                "Node " + nodes.indexOf(node) + "  connected to:  " + node.numberOfConnectedNodes() + "  Quorum connected: " + node.isConnectedToQuorum());
            }

        for (Node node : nodes) {
            node.stopFailureDetector();
        }
    // }

}
}