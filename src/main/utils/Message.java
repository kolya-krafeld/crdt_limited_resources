package main.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message class for sending messages between nodes.
 */
public class Message {

    private InetAddress address;
    private int port;
    private MessageType type;
    private String content = null;

    public Message(InetAddress address, int port, String messageStr) {
        this.address = address;
        this.port = port;

        // Some messages contain ':', others don't. Get message type from whole message string or only substring before ':'
        if (messageStr.contains(":")) {
            int indexOfFirstColon = messageStr.indexOf(":");
            this.type = MessageType.titleToMessageType(messageStr.substring(0, indexOfFirstColon));
            this.content = messageStr.substring(indexOfFirstColon + 1);
        } else {
            this.type = MessageType.titleToMessageType(messageStr);
        }
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public MessageType getType() {
        return type;
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "Message{" +
                "address=" + address +
                ", port=" + port +
                ", type=" + type +
                ", content='" + content + '\'' +
                '}';

    }

    public static Message messageFromForwaredMessage(String str) {
        Pattern pattern = Pattern.compile("Message\\{address=(.*), port=(\\d+), type=(.*), content='(.*)'\\}");
        Matcher matcher = pattern.matcher(str);
    
        if (matcher.find()) {
            InetAddress originalAddress = null;
            try {
                String addressStr = matcher.group(1).replaceFirst("/", "");
                originalAddress = InetAddress.getByName(addressStr);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            int originalPort = Integer.parseInt(matcher.group(2));
            String messageType = MessageType.valueOf(matcher.group(3)).getTitle();
            String messageContent = matcher.group(4);
            String messageString = messageType + ":"+ messageContent;
            return new Message(originalAddress, originalPort, messageString);
        } else {
            throw new RuntimeException("Error parsing message from forwarded message");
        }
    }

}
