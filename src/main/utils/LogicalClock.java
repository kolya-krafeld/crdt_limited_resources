package main.utils;

/**
 * Logical clock for ordering events in a distributed system.
 */
public class LogicalClock {
    private int time;

    public LogicalClock() {
        this.time = 0;
    }

    public void tick() {
        this.time += 1;
    }

    public int getTime() {
        return this.time;
    }

//    public boolean tickAndCheckTimeout() {
//        this.time += 1;
//        if (this.time == this.timeout) {
//            this.time = 0;
//            return true;
//        } else {
//            return false;
//        }
//    }
}