package test;

import main.Client;
import main.Config;
import main.Node;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests different behaviors of the system with different loads and node failures
 * Tests have to be run one by one, as they are using the same ports and nodes are not getting closed immediately
 */
public class SystemTests {

    private void setUpNodes(List<Node> nodes, List<Integer> ports, int numberOfNodes, int requestResources) {
        Config config = new Config(100, 5, 2, 5);

        // Set ports
        for (int i = 0; i < numberOfNodes; i++) {
            ports.add(8000 + i);
        }

        // Create nodes
        Node node;
        for (int i = 0; i < numberOfNodes; i++) {
            node = new Node(ports.get(i), ports, config);
            node.getLimitedResourceCrdt().setUpper(i, requestResources / numberOfNodes);
            node.setLeaderPort(ports.get(0));
            node.init(true);
            nodes.add(node);
        }
    }

    /**
     * sending requests to all nodes, randomly split
     *  in this test requests are send automatically from the client
     */
    @Test
    public void testSystemWithRandomLoad() {
        int requestResources = 30;
        int additionalRequests = 10;
        int numberOfNodes = 3;

        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);

        Client client = new Client(ports, nodes, requestResources + additionalRequests, 100, Client.Mode.TEST);
        try {
            client.start();
            client.join();

            assertEquals("Should request all resources", requestResources + additionalRequests, client.getResourcesRequested());
            assertEquals("Should get all requested resources", requestResources, client.getResourcesReceived());
            assertEquals("Additional requests should be denied", additionalRequests, client.getResourcesDenied());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();
        }
    }

    /**
     * sending requests only to one node
     * in this test requests are send automatically from the client
     */
    @Test
    public void testSystemWithNodeHeaveLoad() {
        int requestResources = 30;
        int additionalRequests = 10;
        int numberOfNodes = 3;

        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);

        Client client = new Client(ports, nodes, requestResources + additionalRequests, 10, Client.Mode.TEST);
        client.setRequestMode(Client.MessageDistributionMode.ONLY_FOLLOWER);
        try {
            client.start();
            client.join();

            assertEquals("Should request all resources", requestResources + additionalRequests, client.getResourcesRequested());
            assertEquals("Should get all requested resources", requestResources, client.getResourcesReceived());
            assertEquals("Additional requests should be denied", additionalRequests, client.getResourcesDenied());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();
        }
    }

    /**
     * sending requests only to the leader
     * in this test requests are send automatically from the client
     */
    @Test
    public void testSystemWithHeaveLoadOnLeader() {
        int requestResources = 30;
        int additionalRequests = 10;
        int numberOfNodes = 3;

        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);

        Client client = new Client(ports, nodes, requestResources + additionalRequests, 10, Client.Mode.TEST);
        client.setRequestMode(Client.MessageDistributionMode.ONLY_LEADER);
        try {
            client.start();
            client.join();

            assertEquals("Should request all resources", requestResources + additionalRequests, client.getResourcesRequested());
            assertEquals("Should get all requested resources", requestResources, client.getResourcesReceived());
            assertEquals("Additional requests should be denied", additionalRequests, client.getResourcesDenied());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();
        }
    }

    /**
     * kills follower node with restarting
     * in this test requests are send manually
     */
    @Test
    public void testKillFollowerWithRestart() {
        int requestResources = 9;
        int numberOfNodes = 3;
        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);
        Client client = new Client(ports, nodes, Client.Mode.TEST);
        client.setRequestMode(Client.MessageDistributionMode.SPECIFIC_NODES);
        try {
            client.start();
            client.requestLimitedResource(1);
            client.requestLimitedResource(1);
            client.requestLimitedResource(2);
            client.requestLimitedResource(2);
            Thread.sleep(500); //give nodes time to answer to requests
            assertEquals("Should get the same amount of resources as requested", 4, client.getResourcesReceived());
            nodes.get(1).kill(); //Node 1 should have 3 leases, so one is lost
            //try to get more resources
            for (int i = 0; i < 5; i++) {
                client.requestLimitedResource(0);
                client.requestLimitedResource(1);
                client.requestLimitedResource(2);
            }
            Thread.sleep(1000); //give nodes time to answer to requests
            assertEquals("One from the total 9 leases is hold by dead Node 1", 8, client.getResourcesReceived());
            nodes.get(1).restart();
            Thread.sleep(1000); //give node time to restart and sync with the leader
            for (int i = 0; i < 2; i++) {
                client.requestLimitedResource(0);
                client.requestLimitedResource(1);
                client.requestLimitedResource(2);
            }
            Thread.sleep(500); //give node time to answer to requests
            assertEquals("Restored lease from node 1 should be given out", 9, client.getResourcesReceived());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();
        }
    }

    /**
     * kills leader node with restarting
     * in this test requests are send manually
     */
    @Test
    public void testKillLeaderWithRestarting() {
        int requestResources = 9;
        int numberOfNodes = 3;
        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);
        Client client = new Client(ports, nodes, Client.Mode.TEST);
        client.setRequestMode(Client.MessageDistributionMode.SPECIFIC_NODES);
        try {
            client.start();
            client.requestLimitedResource(0);
            client.requestLimitedResource(0);
            client.requestLimitedResource(1);
            client.requestLimitedResource(1);
            client.requestLimitedResource(2);
            client.requestLimitedResource(2);
            Thread.sleep(500); // give nodes time to answer to requests
            assertEquals("Expect", 6, client.getResourcesReceived());
            nodes.get(0).kill(); // Node 0 should have 3 leases, so one is lost, as this node is the leader, the leader election gets started
            Thread.sleep(2000); // give nodes time to detect dead leader and elect a new one
            if (!nodes.get(1).isLeader() && !nodes.get(2).isLeader()) {
                assertTrue("We must have a new leader by now", nodes.get(1).isLeader() || nodes.get(2).isLeader());
            }
            //try to get more resources
            for (int i = 0; i < 5; i++) {
                client.requestLimitedResource(0);
                client.requestLimitedResource(1);
                client.requestLimitedResource(2);
            }
            Thread.sleep(1000); //give nodes time to answer to requests
            assertEquals("One from the total 9 leases is hold by dead Node 0", 8, client.getResourcesReceived());
            nodes.get(0).restart();
            Thread.sleep(1000); //give node time to restart and sync with the new leader
            for (int i = 0; i < 2; i++) {
                client.requestLimitedResource(0);
                client.requestLimitedResource(1);
                client.requestLimitedResource(2);
            }
            Thread.sleep(500); //give node time to answer to requests
            assertEquals("Restored lease from node 0 should be given out", 9, client.getResourcesReceived());
            int leaders = 0;
            for (Node node : nodes) {
                if (node.isLeader()) {
                    leaders++;
                }
            }
            assertEquals("There should be only one leader", 1, leaders);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();
        }
    }

    /**
     * every node takes 5 seconds to answer to coordination messages
     * this means they arrive after the coordination phase has finished
     * the system should work nonetheless
     * in this test requests are send manually
     */
    @Test
    public void testDelayStateUntilAfterCoordinationPhase() {
        int requestResources = 15;
        //5 nodes to make the coordination phase with delayed messages harder
        int numberOfNodes = 5;
        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);
        Client client = new Client(ports, nodes, Client.Mode.TEST);
        client.setRequestMode(Client.MessageDistributionMode.SPECIFIC_NODES);
        nodes.get(1).setAddMessageDelay(true);
        nodes.get(2).setAddMessageDelay(true);
        nodes.get(3).setAddMessageDelay(true);
        nodes.get(4).setAddMessageDelay(true);
        try {
            client.start();
            //every node has 3 resources, trigger the coordination phase with sending 5 requests to node 1
            for (int i = 0; i < 5; i++) {
                client.requestLimitedResource(1);
            }
            //node 1 starts coordinating phase, but the messages are delayed by 5 seconds
            //try to get more resources in the meantime
            for (int i = 0; i < 5; i++) {
                client.requestLimitedResource(0);
                client.requestLimitedResource(1);
                client.requestLimitedResource(2);
            }
            Thread.sleep(10000); //give nodes time to answer to requests, this long because coordination messages are delayed by 5 seconds
            //try to get more resources
            for (int i = 0; i < 5; i++) {
                client.requestLimitedResource(0);
                client.requestLimitedResource(1);
                client.requestLimitedResource(2);
                client.requestLimitedResource(3);
                client.requestLimitedResource(4);
            }
            Thread.sleep(10000); //give nodes time to answer to requests, this long because coordination messages are delayed by 5 seconds
            assertEquals("Should not get more resources than available", 15, client.getResourcesReceived());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();
        }
    }

}