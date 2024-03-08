package main.utils;

public enum MessageType {

    //Find Leader messages
    FIND_LEADER_REQUEST("find-leader-request", false, false, false, true),
    FIND_LEADER_REPLY("find-leader-reply", false, false, false, true),

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
    MERGE_MONOTONIC("merge-monotonic", true),

    // Client messages
    INC("increment", false),
    DEC("decrement", false),
    APPROVE_RES("approve-resource", false),
    DENY_RES("deny-resource", false),

    /**
     * If a message that is supposed to be sent to the leader, reaches a follower (e.g. if the sender doesn't know the current leader),
     * the receiver will forward the message to the leader. As it is important for the leader to get the message with the original port
     * this message contains the original message.
     */
    FORWARDED_TO_LEADER("forwarded-to-leader", false);


    private final String title;
    private final boolean coordinationMessage;
    private final boolean heartbeatOrLeaderElectionMessage;
    private final boolean preparePhaseMessage;
    private final boolean findLeaderMessage;

    MessageType(String title, boolean coordinationMessage) {
        this(title, coordinationMessage, false, false);
    }


    MessageType(String title, boolean coordinationMessage, boolean heartbeatOrLeaderElectionMessage) {
        this(title, coordinationMessage, heartbeatOrLeaderElectionMessage, false, false);
    }

    MessageType(String title, boolean coordinationMessage, boolean heartbeatOrLeaderElectionMessage, boolean preparePhaseMessage) {
        this(title, coordinationMessage, heartbeatOrLeaderElectionMessage, preparePhaseMessage, false);
    }


    MessageType(String title, boolean coordinationMessage, boolean heartbeatOrLeaderElectionMessage, boolean preparePhaseMessage, boolean findLeaderMessage){
        this.title = title;
        this.coordinationMessage = coordinationMessage;
        this.heartbeatOrLeaderElectionMessage = heartbeatOrLeaderElectionMessage;
        this.preparePhaseMessage = preparePhaseMessage;
        this.findLeaderMessage = findLeaderMessage;
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

    public boolean heartbeatOrLeaderElectionMessage() {
        return heartbeatOrLeaderElectionMessage;
    }

    public boolean isFindLeaderMessage() {
        return findLeaderMessage;
    }

    public boolean isPreparePhaseMessage() {
        return preparePhaseMessage;
    }

    public boolean isForLeaderMessage() {
        return this == REQL || this == STATE || this == ACCEPTED || this ==PROMISE ||this == FORWARDED_TO_LEADER;
    }
}
