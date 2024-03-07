package test;

import main.utils.Message;
import main.utils.MessageType;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.jupiter.api.Test;

import javax.naming.InitialContext;

import static org.junit.Assert.assertEquals;

public class MessageForwardTest {
    @Test
    public void testForwardMessageParsing() {
        InetAddress initAddress = null;
        try {
            initAddress = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        Message originalMessage = new Message(initAddress, 8000, MessageType.ELECTION_REQUEST.getTitle() + ":" + "foobar");
        System.out.println("originalMessage:     "+originalMessage);

        String forwardMessageString = MessageType.FORWARDED_TO_LEADER.getTitle() + ":" + originalMessage;
        System.out.println("forwardMessageString:     "+forwardMessageString);

        Message forwardMessage = new Message(initAddress, 8001, forwardMessageString);
        System.out.println("forwardMessage:     "+forwardMessage);

        Message extractedMessage = Message.messageFromForwaredMessage(forwardMessage.getContent());
        System.out.println("extractedMessage:     "+extractedMessage);


        assertEquals(originalMessage.getAddress(), extractedMessage.getAddress());
        assertEquals(originalMessage.getPort(), extractedMessage.getPort());
        assertEquals(originalMessage.getType(), extractedMessage.getType());
        assertEquals(originalMessage.getContent(), extractedMessage.getContent());
    }
}
