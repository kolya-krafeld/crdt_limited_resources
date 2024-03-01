package main;

/**
 * Configuration for each node
 * @tick: the interval at which the LogicalClock ticks in ms
 * @sendHeartbeatInterval: the interval at which the node sends heartbeats in ticks
 * @heartbeatTimeout: the timeout for the heartbeat in ticks
 */
public record Config(int tick, int heartbeatTimeout, int sendHeartbeatInterval) {
    public Config {
        if (tick < 0) {
            throw new IllegalArgumentException("Tick must be non-negative");
        }
        if (heartbeatTimeout < 0 || sendHeartbeatInterval < 0) {
            throw new IllegalArgumentException("Timeout and interval must be non-negative");
        }
        if (sendHeartbeatInterval > heartbeatTimeout) {
            throw new IllegalArgumentException("Interval must be less than timeout");
        }
    }
}