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
     * Get string representation of the CRDT to be sent over the network.
     */
    @Override
    public String toString();
}
