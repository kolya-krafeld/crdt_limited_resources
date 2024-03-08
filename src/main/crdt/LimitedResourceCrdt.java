package main.crdt;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LimitedResourceCrdt implements Crdt {
    int numberOfProcesses;

    private List<Integer> upperCounter = new ArrayList<>();
    private List<Integer> lowerCounter = new ArrayList<>();

    public LimitedResourceCrdt(int numberOfProcesses) {
        this.numberOfProcesses = numberOfProcesses;
        for (int i = 0; i < numberOfProcesses; i++) {
            upperCounter.add(0);
            lowerCounter.add(0);
        }
    }

    /**
     * Create a CRDT from a string representation of the CRDT.
     * e.g. [10;10;10],[1;8;0]
     */
    public LimitedResourceCrdt(String crdtString) {
        String[] crdtStrings = crdtString.split(",");
        String upperString = crdtStrings[0].substring(1, crdtStrings[0].length() - 1);
        String lowerString = crdtStrings[1].substring(1, crdtStrings[1].length() - 1);

        String[] upperStrings = upperString.split(";");
        String[] lowerStrings = lowerString.split(";");
        for (String s : upperStrings) {
            upperCounter.add(Integer.parseInt(s));
        }
        for (String s : lowerStrings) {
            lowerCounter.add(Integer.parseInt(s));
        }

        this.numberOfProcesses = upperCounter.size();
    }

    /**
     * Return the number of resources that are still left across all processes.
     */
    public int query() {
        int sum = 0;
        for (int i = 0; i < numberOfProcesses; i++) {
            sum += upperCounter.get(i) - lowerCounter.get(i);
        }
        return sum;
    }

    /**
     * Return the number of resources that are still left for a specific process.
     */
    public int queryProcess(int processIndex) {
        return upperCounter.get(processIndex) - lowerCounter.get(processIndex);
    }

    public void increment(int index) {
        upperCounter.set(index, upperCounter.get(index) + 1);
    }

    public boolean setUpper(int index, int value) {
        if (value < upperCounter.get(index) || value < lowerCounter.get(index)) {
            return false;
        }

        upperCounter.set(index, value);
        return true;
    }

    /**
     * Decrement the lower counter at index if it smaller than the greater counter.
     * @return false if the lower counter is already >= the upper counter.
     */
    public boolean decrement(int index) {
        if (upperCounter.get(index) > lowerCounter.get(index)) {
            lowerCounter.set(index, lowerCounter.get(index) + 1);
            return true;
        }
        return false;
    }

    /**
     * Set the lower counter at index if it is smaller or equal to the upper counter.
     * @return false if the value is greater than the current upper counter.
     */
    public boolean setLower(int index, int value) {
        if (value > upperCounter.get(index)) {
            return false;
        }
        lowerCounter.set(index, value);
        return true;
    }

    @Override
    public void merge(Crdt other) {
        LimitedResourceCrdt otherCrdt = (LimitedResourceCrdt) other;
        for (int i = 0; i < numberOfProcesses; i++) {
            upperCounter.set(i, Math.max(upperCounter.get(i), otherCrdt.upperCounter.get(i)));
            lowerCounter.set(i, Math.max(lowerCounter.get(i), otherCrdt.lowerCounter.get(i)));
        }
    }

    public boolean compare(Crdt other) {
        LimitedResourceCrdt otherCrdt = (LimitedResourceCrdt) other;
        for (int i = 0; i < numberOfProcesses; i++) {
            // If one of the values is smaller than in other CRDT, return false
            if (upperCounter.get(i) < otherCrdt.upperCounter.get(i) || lowerCounter.get(i) < otherCrdt.lowerCounter.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a string representation of the CRDT to be send over the network.
     * e.g. [10;10;10],[1;8;0]
     */
    @Override
    public String toString() {
        return "[" + upperCounter.stream().map(e -> e.toString()).collect(Collectors.joining(";")) + "],["
                + lowerCounter.stream().map(e -> e.toString()).collect(Collectors.joining(";")) + "]";
    }

    public int getNumberOfProcesses() {
        return numberOfProcesses;
    }

    public List<Integer> getUpperCounter() {
        return upperCounter;
    }

    public List<Integer> getLowerCounter() {
        return lowerCounter;
    }
}
