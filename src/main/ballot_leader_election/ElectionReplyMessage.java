package main.ballot_leader_election;

public class ElectionReplyMessage {

    public int rnd;
    public int ballotNumber;
    public boolean isQuorumConnected;

    public ElectionReplyMessage(int rnd, int ballotNumber, boolean isQourumConnected) {
        this.rnd = rnd;
        this.ballotNumber = ballotNumber;
        this.isQuorumConnected = isQourumConnected;
    }

    @Override
    public String toString() {
        return "rnd=" + rnd +
                ", ballotNumber=" + ballotNumber +
                ", isQourumConnected=" + isQuorumConnected;
    }

    public static ElectionReplyMessage fromString(String str) {
        String[] parts = str.split(",");
        int rnd = Integer.parseInt(parts[0].split("=")[1]);
        int ballotNumber = Integer.parseInt(parts[1].split("=")[1]);
        boolean isQourumConnected = Boolean.parseBoolean(parts[2].split("=")[1]);
        return new ElectionReplyMessage(rnd, ballotNumber, isQourumConnected);
    }
}



