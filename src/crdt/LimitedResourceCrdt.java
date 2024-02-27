package crdt;

import java.util.ArrayList;
import java.util.List;

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

    public void increment(int index) {
        upperCounter.set(index, upperCounter.get(index) + 1);
    }

    public void setUpper(int index, int value) {
        upperCounter.set(index, value);
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

    @Override
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
}
