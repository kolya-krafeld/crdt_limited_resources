public class LogicalClock {
    private long time;
    private long timeout;

    public LogicalClock(long timeout) {
        this.time = 0;
        this.timeout = timeout;
    }

    public boolean tickAndCheckTimeout() {
        this.time += 1;
        if (this.time == this.timeout) {
            this.time = 0;
            return true;
        } else {
            return false;
        }
    }
}