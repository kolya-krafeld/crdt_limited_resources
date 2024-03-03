package main;

/**
 * Configuration for each node
 * @tickLength: the interval at which the LogicalClock ticks in ms
 * @sendHeartbeatInterval: the interval at which the node sends heartbeats in ticks
 * @heartbeatTimeout: the timeout for the heartbeat in ticks
 */
public record Config(int tickLength, int heartbeatTimeout, int sendHeartbeatInterval, int electionTimeout) {
    public Config {
        if (tickLength < 0) {
            throw new IllegalArgumentException("Tick must be non-negative");
        }
        if (heartbeatTimeout < 0 || sendHeartbeatInterval < 0 || electionTimeout < 0) {
            throw new IllegalArgumentException("Timeouts and interval must be non-negative");
        }
        if (sendHeartbeatInterval > heartbeatTimeout) {
            throw new IllegalArgumentException("Interval must be less than timeout");
        }
    }
}