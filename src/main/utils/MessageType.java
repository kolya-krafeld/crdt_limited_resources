package main.utils;

public enum MessageType {

    HEARTBEAT("heartbeat", false),
    MERGE("merge", false),
    INC("increment", false),
    DEC("decrement", false),
    REQL("request-lease", true),
    REQS("request-state", true),
    STATE("state", true),
    ACCEPT("accept", true),
    ACCEPTED("accepted", true),
    DECIDE("decide", true);


    private final String title;
    private final boolean coordinationMessage;

    private MessageType(String title, boolean coordinationMessage) {
        this.title = title;
        this.coordinationMessage = coordinationMessage;
    }

    /**
     * Some messages contain ':', others don't. Get message type from whole message string or only substring before ':'.
     */
    public static MessageType getMessageTypeFromMessageString(String messageStr) {
        if (messageStr.contains(":")) {
            String[] parts = messageStr.split(":");
            return MessageType.titleToMessageType(parts[0]);
        } else {
            return MessageType.titleToMessageType(messageStr);
        }
    }

    public static MessageType titleToMessageType(String title) {
        for (MessageType messageType : MessageType.values()) {
            if (messageType.title.equals(title)) {
                return messageType;
            }
        }
        return null;
    }

    public String getTitle() {
        return title;
    }

    public boolean isCoordinationMessage() {
        return coordinationMessage;
    }
}
