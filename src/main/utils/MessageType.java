package main.utils;

public enum MessageType {

    // Peer-to-peer messages
    MERGE("merge", false),
    REQL("request-lease", true),
    REQS("request-state", true),
    STATE("state", true),
    ACCEPT("accept", true),
    ACCEPTED("accepted", true),
    DECIDE("decide", true),

    // Client messages
    INC("increment", false),
    DEC("decrement", false),
    APPROVER("approve-resource", false),
    DENYR("deny-resource", false);


    private final String title;
    private final boolean coordinationMessage;

    private MessageType(String title, boolean coordinationMessage) {
        this.title = title;
        this.coordinationMessage = coordinationMessage;
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
