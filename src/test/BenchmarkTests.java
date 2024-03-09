package test;

import main.Client;
import main.Config;
import main.Node;
import main.utils.Message;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests to benchmark the performance of the system.
 * Run benchmarks for 3,5,10 nodes respectively.
 * Run benchmarks for 1,000, 100,000, 1,000,000 resources. Make 10% calls than resources available.
 *
 * Run tests for the following 3 systesm:
 * 1. Limited Resource CRDTS with workload spread across all nodes randomly.
 * 2. Limited Resource CRDTS with workload focus on one node.
 * 3. Coordination phase/consensus for every requested resource.
 */
public class BenchmarkTests {

    @Test
    public void testSystemRandomWorkload() throws UnknownHostException {
        int numberOfResources = 1 * 1000;
        int additionalRequests = (int) (numberOfResources * 0.01);
        int numberOfNodes = 25;

        long runtimeSum = runTestIteration(numberOfNodes, numberOfResources, additionalRequests, 0,
                false, Client.Mode.RANDOM);

        System.out.println("Average time taken for testSystemWithRandomLoad: " + runtimeSum + "ms");
    }

    @Test
    public void testSystemWorkloadHeavyNode() throws UnknownHostException {
        int numberOfResources = 1 * 1000;
        int additionalRequests = (int) (numberOfResources * 0.01);
        int numberOfNodes = 25;

        long runtimeSum = runTestIteration(numberOfNodes, numberOfResources, additionalRequests, 0,
                false, Client.Mode.ONLY_FOLLOWER);

        System.out.println("Average time taken for testSystemWithRandomLoad: " + runtimeSum + "ms");
    }

    @Test
    public void testSystemWithCoordinationForEveryNode() throws UnknownHostException {
        int numberOfResources = 1 * 1000;
        int additionalRequests = (int) (numberOfResources * 0.01);
        int numberOfNodes = 25;

        long runtimeSum = runTestIteration(numberOfNodes, numberOfResources, additionalRequests, 0,
                true, Client.Mode.EXCLUDE_LEADER);

        System.out.println("Average time taken for testSystemWithRandomLoad: " + runtimeSum + "ms");
    }

    private long runTestIteration(int numberOfNodes, int numberOfResources, int additionalRequests, int iteration, boolean coordinationOfEveryRequest, Client.Mode mode) throws UnknownHostException {
        List<Node> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, numberOfResources, iteration, coordinationOfEveryRequest);


        Message message = new Message(InetAddress.getByName("localhost"), 5000, "decrement");
        if (mode == Client.Mode.ONLY_FOLLOWER) {
            Node follower = nodes.get(1);

            for (int i = 0; i < numberOfResources + additionalRequests; i++) {
                follower.operationMessageQueue.add(message);
            }
        } else if (mode == Client.Mode.RANDOM) {

            for (int i = 0; i < numberOfResources + additionalRequests; i++) {
                int indexOfNode = (int) (Math.random() * ports.size());
                nodes.get(indexOfNode).operationMessageQueue.add(message);
            }
        } else if (mode == Client.Mode.EXCLUDE_LEADER) {
            for (int i = 0; i < numberOfResources + additionalRequests; i++) {
                int indexOfNode = (int) (Math.random() * (ports.size() - 1)) + 1;
                nodes.get(indexOfNode).operationMessageQueue.add(message);
            }
        }

        Client client = new Client(ports, nodes, numberOfResources + additionalRequests, 0, 5000 + iteration);
        client.setPrintReceivedMessages(false);
        client.setRequestMode(Client.Mode.ONLY_FOLLOWER);
        long runtime = 0l;
        try {
            client.start();

            // Start nodes
            long startTime = System.currentTimeMillis();
            for (Node node : nodes) {
                node.init(true);
            }
            client.join();
            long endTime = System.currentTimeMillis();


            runtime = endTime - startTime;
            System.out.println("Time taken for testSystemWithRandomLoad: " + runtime + "ms");

            System.out.println("Resources requested: " + (numberOfResources + additionalRequests)  + " Resources received: " + client.getResourcesReceived() + " Resources denied: " + client.getResourcesDenied());
            System.out.println("Limited resource CRDT end state:" + nodes.get(0).getLimitedResourceCrdt());

            assertEquals("Should get all requested resources", numberOfResources, client.getResourcesReceived());
            assertEquals("Additional requests should be denied", additionalRequests, client.getResourcesDenied());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();
        }

        return runtime;
    }

    private void setUpNodes(List<Node> nodes, List<Integer> ports, int numberOfNodes, int requestResources, int iteration, boolean coordinationOfEveryRequest) {
        Config config = new Config(100, 5, 2, 5);

        // Set ports
        for (int i = 0; i < numberOfNodes; i++) {
            ports.add(8000 + numberOfNodes * iteration + i);
        }

        // Create nodes
        Node node;
        // Spread the resources across all nodes
        int leaftoverResources = requestResources % numberOfNodes;
        for (int i = 0; i < numberOfNodes; i++) {
            node = new Node(ports.get(i), ports, config);

            if (coordinationOfEveryRequest) {
                node.setCoordinateForEveryResource(coordinationOfEveryRequest);
                node.getLimitedResourceCrdt().setUpper(i, i == 0 ? requestResources : 0);
            } else {
                node.getLimitedResourceCrdt().setUpper(i, requestResources / numberOfNodes + (leaftoverResources > 0 ? 1 : 0));
            }
            node.setLeaderPort(ports.get(0));
            nodes.add(node);
            leaftoverResources--;
        }
    }

}