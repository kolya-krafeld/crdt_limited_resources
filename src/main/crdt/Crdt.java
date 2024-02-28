package main.crdt;

/**
 * State-based CRDT interface.
 */
public interface Crdt {

    /**
     * Merge the state of this CRDT with the state of another CRDT.
     */
    public void merge(Crdt other);

    /**
     * Compare the state of this CRDT with the state of another CRDT.
     * @return true if this CRDT is greater or equal to other CRDT.
     */
    public boolean compare(Crdt other);
}
