package test;

import main.Config;
import main.crdt.LimitedResourceCrdt;
import main.utils.MessageHandler;
import main.jobs.MessageProcessor;
import org.junit.jupiter.api.Test;
import main.Node;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

class MessageProcessorTest {

    @Test
    void testReassignLeaseBaseCase() throws SocketException, UnknownHostException {
        Config config = new Config(100,5, 2, 5);
        Node node = new Node(8080, asList(8080, 8082, 8083), config);
        MessageHandler messageHandler = new MessageHandler(node, new DatagramSocket(), 8080);

        MessageProcessor messageProcessor = new MessageProcessor(node, messageHandler);
        messageProcessor.setStatesReceivedFrom(new HashSet(asList(8082, 8083)));
        messageProcessor.setLeaderMergedCrdt(new LimitedResourceCrdt(3));
        messageProcessor.reassignLeases();

        //todo
    }

    /**
     * This case cause an issue: [1000;999;998],[998;998;998] -> [1000;999;999],[998;998;998]
     * This gave too many resources free
     * Should be: [1001;1001;1001],[1000;1000;1000]
     */
    @Test
    void testReassignLeaseLeaderNeedsToRevokeOwn() throws SocketException, UnknownHostException {
        Config config = new Config(100,5, 2, 5);
        Node node = new Node(8080, asList(8080, 8082, 8083), config);
        MessageHandler messageHandler = new MessageHandler(node, new DatagramSocket(), 8080);

        MessageProcessor messageProcessor = new MessageProcessor(node, messageHandler);
        messageProcessor.setStatesReceivedFrom(new HashSet(asList(8082, 8083)));

        LimitedResourceCrdt leaderCrdt = new LimitedResourceCrdt(3);
        leaderCrdt.setUpper(0, 1000);
        leaderCrdt.setUpper(1, 999);
        leaderCrdt.setUpper(2, 998);
        leaderCrdt.setLower(0, 998);
        leaderCrdt.setLower(1, 998);
        leaderCrdt.setLower(2, 998);
        assertEquals("[1000;999;998],[998;998;998]", leaderCrdt.toString());
        assertEquals(3, (int) leaderCrdt.query());
        messageProcessor.setLeaderMergedCrdt(leaderCrdt);
        messageProcessor.reassignLeases();
        assertEquals("[1001;1001;1001],[1000;1000;1000]", messageProcessor.getLeaderMergedCrdt().toString());
        assertEquals(3, (int) messageProcessor.getLeaderMergedCrdt().query());
    }

}