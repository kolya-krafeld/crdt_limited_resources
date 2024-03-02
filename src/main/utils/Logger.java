package main.utils;

import main.Node;

/**
 * Custom logger class
 */
public class Logger {

    public enum LogLevel {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    private LogLevel logLevel;
    private Node node;

    public Logger(LogLevel logLevel, Node node) {
        this.logLevel = logLevel;
        this.node = node;
    }

    private String getNodeInfo() {
        return (node.getLeaderPort() == node.getOwnPort() ? "Leader" : "Follower") + " " + node.getOwnPort() + ": ";
    }



    public void info(String message) {
        if (logLevel.ordinal() <= LogLevel.INFO.ordinal()) {
            System.out.println("INFO: " + getNodeInfo() + message);
        }
    }

    public void debug(String message) {
        if (logLevel.ordinal() <= LogLevel.DEBUG.ordinal()) {
            System.out.println("DEBUG: " + getNodeInfo() + message);
        }
    }

    public void warn(String message) {
        if (logLevel.ordinal() <= LogLevel.WARN.ordinal()) {
            System.out.println("WARN: " + getNodeInfo() + message);
        }
    }

    public void error(String message) {
        if (logLevel.ordinal() <= LogLevel.ERROR.ordinal()) {
            System.out.println("ERROR: " + getNodeInfo() + message);
        }
    }
}
