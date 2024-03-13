package test;

import main.Client;
import main.Config;
import main.Node;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ClientTest {

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

    @Test
    public void testSystemWithHeaveLoadOnLeader() {
        int requestResources = 30;
        int additionalRequests = 10;
        int numberOfNodes = 3;

        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);

        Client client = new Client(ports, nodes, requestResources + additionalRequests, 10,Client.Mode.TEST);
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

    @Test
    //only one follower and one leader, the follower delays the coordination messages for 2 seconds each
    public void testSystemWithMessageDelay() {
        int requestResources = 30;
        int additionalRequests = 10;
        int numberOfNodes = 2;

        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, requestResources);

        Client client = new Client(ports, nodes, requestResources + additionalRequests, 10, Client.Mode.TEST);

        // Delay coordination messages from this node
        nodes.get(numberOfNodes - 1).setAddMessageDelay(true);
        // Send all requests to same node to have more coordiantion phases
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

}