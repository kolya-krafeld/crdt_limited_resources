package main.utils;

import java.net.InetAddress;

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
}
