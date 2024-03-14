package main.utils;


import main.Node;

import java.util.ArrayList;
import java.util.List;

//just for testing
public class NodeKiller extends Thread {
    int targetIndex;
    boolean revive;
    int reviveTime= 0;
    public int killTime=0;
    List<Node> nodes = new ArrayList<>();


    public NodeKiller (int targetIndex, int killTime, List<Node> nodes){
        this(targetIndex, killTime, nodes, false, 0);
    }

    public NodeKiller(int targetIndex, int killTime, List<Node> nodes,   boolean revive, int reviveTime) {
        this.targetIndex = targetIndex;
        this.revive = revive;
        this.reviveTime = reviveTime;
        this.killTime = killTime;
        this.nodes = nodes;
    }

    public void run() {
        Node target = nodes.get(targetIndex);
        try {
            Thread.sleep(killTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Killing node: " + target.getOwnPort());
        target.kill();
        if(revive) {
            try {
                Thread.sleep(reviveTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            target.restart();
        }
    }
}