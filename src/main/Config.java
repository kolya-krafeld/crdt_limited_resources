package main;

public record Config(int heartbeatTimeout, int sendHeartbeatInterval) {
    public Config {
        if (heartbeatTimeout < 0 || sendHeartbeatInterval < 0) {
            throw new IllegalArgumentException("Timeout and interval must be non-negative");
        }
        if (sendHeartbeatInterval > heartbeatTimeout) {
            throw new IllegalArgumentException("Interval must be less than timeout");
        }
    }
}