package main.utils;

public enum MessageType {

    // Ballot Leader Election messages
    HB_REQUEST("heartbeat-request", false, true),
    HB_REPLY("heartbeat-reply", false, true),
    ELECTION_REQUEST("election-request", false, true),
    ELECTION_REPLY("election-reply", false, true),
    ELECTION_RESULT("election-result", false, true),


    // Coordination messages,
    REQL("request-lease", true),
    REQS("request-state", true),
    STATE("state", true),
    ACCEPT("accept", true),
    ACCEPTED("accepted", true),
    DECIDE("decide", true),

    // Restarts
    REQUEST_SYNC("request-sync", true),
    ACCEPT_SYNC("accept-sync", true),

    // Leader Prepare phase
    PREPARE("prepare", true),
    PROMISE("promise", false, false, true),

    // CRDT Merge
    MERGE("merge", false),

    // Client messages
    INC("increment", false),
    DEC("decrement", false),
    APPROVE_RES("approve-resource", false),
    DENY_RES("deny-resource", false);


    private final String title;
    private final boolean coordinationMessage;
    private final boolean leaderElectionMessage;
    private final boolean preparePhaseMessage;

    MessageType(String title, boolean coordinationMessage) {
        this(title, coordinationMessage, false);
    }


    MessageType(String title, boolean coordinationMessage, boolean leaderElectionMessage) {
        this(title, coordinationMessage, leaderElectionMessage, false);
    }

    MessageType(String title, boolean coordinationMessage, boolean leaderElectionMessage, boolean preparePhaseMessage) {
        this.title = title;
        this.coordinationMessage = coordinationMessage;
        this.leaderElectionMessage = leaderElectionMessage;
        this.preparePhaseMessage = preparePhaseMessage;
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

    public boolean isLeaderElectionMessage() {
        return leaderElectionMessage;
    }

    public boolean isPreparePhaseMessage() {
        return preparePhaseMessage;
    }
}
