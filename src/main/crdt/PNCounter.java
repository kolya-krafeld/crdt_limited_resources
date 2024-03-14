package main.crdt;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Positive-Negative Counter CRDT.
 * Supports increment and decrement operations.
 */
public class PNCounter implements Crdt<Integer> {

    int numberOfProcesses;

    private List<Integer> inc = new ArrayList<>();
    private List<Integer> dec = new ArrayList<>();

    public PNCounter(int numberOfProcesses) {
        this.numberOfProcesses = numberOfProcesses;
        for (int i = 0; i < numberOfProcesses; i++) {
            inc.add(0);
            dec.add(0);
        }
    }

    /**
     * Create a CRDT from a string representation of the CRDT.
     * e.g. [10;10;10],[1;8;0]
     */
    public PNCounter(String crdtString) {
        String[] crdtStrings = crdtString.split(",");
        String upperString = crdtStrings[0].substring(1, crdtStrings[0].length() - 1);
        String lowerString = crdtStrings[1].substring(1, crdtStrings[1].length() - 1);

        String[] upperStrings = upperString.split(";");
        String[] lowerStrings = lowerString.split(";");
        for (String s : upperStrings) {
            inc.add(Integer.parseInt(s));
        }
        for (String s : lowerStrings) {
            dec.add(Integer.parseInt(s));
        }

        this.numberOfProcesses = inc.size();
    }

    /**
     * Queries the total counter value.
     */
    public Integer query() {
        int sum = 0;
        for (int i = 0; i < numberOfProcesses; i++) {
            sum += inc.get(i) - dec.get(i);
        }
        return sum;
    }

    public void increment(int index) {
        inc.set(index, inc.get(index) + 1);
    }

    public void decrement(int index) {
        dec.set(index, dec.get(index) + 1);
    }

    @Override
    public void merge(Crdt other) {
        PNCounter otherCrdt = (PNCounter) other;
        for (int i = 0; i < numberOfProcesses; i++) {
            inc.set(i, Math.max(inc.get(i), otherCrdt.inc.get(i)));
            dec.set(i, Math.max(dec.get(i), otherCrdt.dec.get(i)));
        }
    }

    /**
     * Returns a string representation of the CRDT to be sent over the network.
     * e.g. [10;10;10],[1;8;0]
     */
    @Override
    public String toString() {
        return "[" + inc.stream().map(e -> e.toString()).collect(Collectors.joining(";")) + "],["
                + dec.stream().map(e -> e.toString()).collect(Collectors.joining(";")) + "]";
    }
}
