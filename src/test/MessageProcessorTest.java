package test;

import main.utils.MessageHandler;
import main.jobs.MessageProcessor;
import org.junit.jupiter.api.Test;
import main.Node;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;

import static java.util.Arrays.asList;

class MessageProcessorTest {

    @Test
    void testReassignLeaseBaseCase() throws SocketException, UnknownHostException {
        Node node = new Node(8080, asList(8080, 8082, 8083));
        MessageHandler messageHandler = new MessageHandler(node, new DatagramSocket(), 8080);

        MessageProcessor messageProcessor = new MessageProcessor(node, messageHandler);
        messageProcessor.setStatesReceivedFrom(new HashSet(asList(8082, 8083)));
        messageProcessor.reassignLeases();
    }

}