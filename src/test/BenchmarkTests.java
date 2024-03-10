package test;

import main.Client;
import main.Config;
import main.Node;
import main.utils.Message;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests to benchmark the performance of the system.
 * Run benchmarks for 3,5,10 nodes respectively.
 * Run benchmarks for 1,000, 10,000, 100,000 resources. Make 10% calls than resources available.
 *
 * Run tests for the following 3 systems:
 * 1. Limited Resource CRDTS with workload spread across all nodes randomly.
 * 2. Limited Resource CRDTS with workload focus on one node.
 * 3. Coordination phase/consensus for every requested resource.
 */
public class BenchmarkTests {

    private static final int NUMBER_OF_NODES = 10;
    private static final int NUMBER_OF_RESOURCES = 1 * 1000;
    private static final int NUMBER_OF_ITERATIONS = 1;

    @Test
    public void testSystemRandomWorkload() throws UnknownHostException, InterruptedException {
        int additionalRequests = (int) (NUMBER_OF_RESOURCES * 0.01);

        long runtimeAverage = testSystemXTimes(NUMBER_OF_RESOURCES, additionalRequests, NUMBER_OF_NODES, NUMBER_OF_ITERATIONS, Client.Mode.RANDOM, false);

        System.out.println("------------------------");
        System.out.println("Average time taken: " + runtimeAverage + "ms");
        writeTestResults("testSystemRandomWorkload", NUMBER_OF_NODES, NUMBER_OF_RESOURCES, runtimeAverage);

    }

    @Test
    public void testSystemWorkloadHeavyNode() throws UnknownHostException, InterruptedException {
        int additionalRequests = (int) (NUMBER_OF_RESOURCES * 0.01);

        long runtimeAverage = testSystemXTimes(NUMBER_OF_RESOURCES, additionalRequests, NUMBER_OF_NODES, NUMBER_OF_ITERATIONS, Client.Mode.ONLY_FOLLOWER, false);

        System.out.println("------------------------");
        System.out.println("Average time taken: " + runtimeAverage + "ms");
        writeTestResults("testSystemWorkloadHeavyNode", NUMBER_OF_NODES, NUMBER_OF_RESOURCES, runtimeAverage);


    }

    @Test
    public void testSystemWithCoordinationForEveryNode() throws UnknownHostException, InterruptedException {
        int additionalRequests = (int) (NUMBER_OF_RESOURCES * 0.01);

        long runtimeAverage = testSystemXTimes(NUMBER_OF_RESOURCES, additionalRequests, NUMBER_OF_NODES, NUMBER_OF_ITERATIONS, Client.Mode.EXCLUDE_LEADER, true);

        System.out.println("------------------------");
        System.out.println("Average time taken: " + runtimeAverage + "ms");
        writeTestResults("testSystemWithCoordinationForEveryNode", NUMBER_OF_NODES, NUMBER_OF_RESOURCES, runtimeAverage);


    }

    public long testSystemXTimes(int numberOfResources, int additionalRequests, int numberOfNodes, int numberOfIterations, Client.Mode mode, boolean coordinationOfEveryRequest) throws UnknownHostException, InterruptedException {
        long runtimeAverage = 0l;
        for (int i = 0; i < numberOfIterations; i++) {
            System.out.println("Start new iteration: " + i);
            long runtimeSum = runTestIteration(numberOfNodes, numberOfResources, additionalRequests, i,
                    coordinationOfEveryRequest, mode);
            System.out.println("Time taken: " + runtimeSum + "ms");
            runtimeAverage += runtimeSum;
            Thread.sleep(2 * 1000);
        }

        return runtimeAverage / numberOfIterations;

    }

    private long runTestIteration(int numberOfNodes, int numberOfResources, int additionalRequests, int iteration, boolean coordinationOfEveryRequest, Client.Mode mode) throws UnknownHostException {
        List<Integer> ports = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();
        setUpNodes(nodes, ports, numberOfNodes, numberOfResources, iteration, coordinationOfEveryRequest);


        Message message = new Message(InetAddress.getByName("localhost"), 5000 + iteration, "decrement");
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

            //assertEquals("Should get all requested resources", numberOfResources, client.getResourcesReceived());
            //assertEquals("Additional requests should be denied", additionalRequests, client.getResourcesDenied());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stopProcesses();

            for (Node node : nodes) {
                node.kill();
            }
        }


        return runtime;
    }

    private void setUpNodes(List<Node> nodes, List<Integer> ports, int numberOfNodes, int requestResources, int iteration, boolean coordinationOfEveryRequest) {
        Config config = new Config(100, 5, 2, 5);

        // Set ports
        for (int i = 0; i < numberOfNodes; i++) {
            ports.add(8001 + numberOfNodes * iteration + i);
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

    public void writeTestResults(String testName, int numberOfNodes, int numberOfResources, long time) {
        String data = numberOfNodes + ", " + numberOfResources + ", " + time;
        String fileName = testName + ".txt";

        try (FileWriter fw = new FileWriter(fileName, true);
             PrintWriter pw = new PrintWriter(fw)) {
            pw.println(data);
            System.out.println("Successfully wrote data to file: " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        AtomicInteger lineCount = new AtomicInteger();
        try {
            Files.lines(Paths.get(fileName)).forEach(line -> lineCount.incrementAndGet());
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Did " + testName + " now " + lineCount.get() + " times");
    }

}