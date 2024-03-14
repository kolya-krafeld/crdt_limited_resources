package main.utils;


import main.Node;

import java.util.ArrayList;
import java.util.List;

//just for testing
public class NodeKiller extends Thread {
    int targetIndex;
    boolean revive;
    int reviveTime;
    List<Node> nodes;

    public NodeKiller(int targetIndex, List<Node> nodes, boolean revive, int reviveTime) {
        this.targetIndex = targetIndex;
        this.revive = revive;
        this.reviveTime = reviveTime;
        this.nodes = nodes;
    }

    public void run() {
        Node target = nodes.get(targetIndex);
        System.out.println("Killing node: " + target.getOwnPort());
        target.kill();
        if (revive) {
            try {
                Thread.sleep(reviveTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            target.restart();
        }
    }
}