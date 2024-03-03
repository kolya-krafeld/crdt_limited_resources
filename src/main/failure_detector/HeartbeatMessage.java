package main.failure_detector;

public class HeartbeatMessage {
    public boolean isQourumConnected;
    public int ballotNumber;
    public int time;

    public HeartbeatMessage(boolean isQourumConnected, int ballotNumber) {
        this.isQourumConnected = isQourumConnected;
        this.ballotNumber = ballotNumber;
    }

    @Override
    public String toString() {
        return "isQourumConnected=" + isQourumConnected +
                ", ballotNumber=" + ballotNumber;
    }

    public static HeartbeatMessage fromString(String str) {
        String[] parts = str.split(",");
        boolean isQourumConnected = Boolean.parseBoolean(parts[0].split("=")[1]);
        int ballotNumber = Integer.parseInt(parts[1].split("=")[1]);
        return new HeartbeatMessage(isQourumConnected, ballotNumber);
    }
}
    
