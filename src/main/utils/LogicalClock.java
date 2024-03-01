package main.utils;

public class LogicalClock {
    private int time;

    public LogicalClock() {
        this.time = 0;
    }

    public Runnable tick() {
        this.time += 1;
        return null;
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