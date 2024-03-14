package main.utils;


import main.Node;

import java.util.List;

/**
 * Class responsible for killing nodes in the network.
 * Provides methods to kill a random follower, a single follower or the leader.
 */
public class NodeKiller extends Thread {

    public enum NodeKillerType {
        RANDOM, RANDOM_FOLLOWER, SINGLE_FOLLOWER, LEADER
    }

    boolean revive;
    int reviveTime;
    List<Node> nodes;
    NodeKillerType type;

    public NodeKiller(NodeKillerType type, List<Node> nodes, boolean revive, int reviveTime) {
        this.revive = revive;
        this.reviveTime = reviveTime;
        this.nodes = nodes;
        this.type = type;
    }

    public void run() {
        try {
            Node target;
            switch (type) {
                case RANDOM:
                    target = nodes.get((int) (Math.random() * nodes.size()));
                    break;
                case RANDOM_FOLLOWER:
                    target = nodes.get((int) (Math.random() * (nodes.size() - 1)) + 1);
                    break;
                case SINGLE_FOLLOWER:
                    // Kill first follower in network
                    target = nodes.get(1);
                    break;
                case LEADER:
                    target = nodes.stream().filter(Node::isLeader).findFirst().get();
                    break;
                default:
                    throw new RuntimeException("Invalid type");
            }
            System.out.println("Killing node: " + target.getOwnPort());
            target.kill();
            if (revive) {
                try {
                    Thread.sleep(reviveTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                target.restart(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}