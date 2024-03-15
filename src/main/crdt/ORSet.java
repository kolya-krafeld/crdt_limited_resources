package main.crdt;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Observed-Remove Set CRDT.
 * Supports adding and removing elements multiple times.
 */
public class ORSet<E> implements Crdt<Set<E>> {

    private Map<E, Set<String>> eset = new ConcurrentHashMap<>();
    private Map<E, Set<String>> tset = new ConcurrentHashMap<>();

    public ORSet() {
    }

    public ORSet(String crdtString) {
        String[] crdtStrings = crdtString.split(",");
        String esetString = crdtStrings[0].substring(1, crdtStrings[0].length() - 1);
        String tsetString = crdtStrings[1].substring(1, crdtStrings[1].length() - 1);

        String[] esetStrings = esetString.split(";");
        String[] tsetStrings = tsetString.split(";");
        for (String s : esetStrings) {
            if (s.isEmpty()) {
                continue;
            }

            String[] parts = s.split("\\(");
            E e = (E) parts[0];
            Set<String> uuids = new HashSet<>();
            if (parts.length > 1) {
                for (String uuid : parts[1].split("\\|")) {
                    if (uuid.contains(")")) {
                        uuid = uuid.substring(0, uuid.length() - 1);
                    }
                    uuids.add(uuid);
                }
            }
            eset.put(e, uuids);
        }
        for (String s : tsetStrings) {
            if (s.isEmpty()) {
                continue;
            }

            String[] parts = s.split("\\(");
            E e = (E) parts[0];

            Set<String> uuids = new HashSet<>();
            if (parts.length > 1) {
                for (String uuid : parts[1].split(",")) {
                    if (uuid.contains(")")) {
                        uuid = uuid.substring(0, uuid.length() - 1);
                    }
                    uuids.add(uuid);
                }
            }
            tset.put(e, uuids);
        }

    }

    /**
     * Returns the set of elements that are in the ESet but not in the TSet.
     */
    public Set<E> query() {
        Set<E> result = new HashSet<>();

        for (E e : eset.keySet()) {
            if (eset.containsKey(e) && !tset.containsKey(e)) {
                result.add(e);
            } else if (eset.containsKey(e) && tset.containsKey(e)) {
                if (eset.get(e).size() > tset.get(e).size()) {
                    result.add(e);
                }
            }
        }
        return result;
    }

    /**
     * Add element to set.
     */
    public void add(E element) {
        String unigueId = UUID.randomUUID().toString();

        if (!eset.containsKey(element)) {
            // Does not contain the element yet
            Set<String> uuids = new HashSet<>();
            uuids.add(unigueId);
            eset.put(element, uuids);
        } else {
            // ESet Contains the element

            if (!tset.containsKey(element)) {
                // TSet does not contain the element
                // Nothing to do
            } else {
                // TSet does also contain the element
                // Add new uuid to the ESet
                eset.get(element).add(unigueId);
            }
        }
    }

    /**
     * Remove element from set.
     */
    public void remove(E element) {
        // Only remove element if it exists in the ESet
        if (eset.containsKey(element)) {
            Set<String> uuids = eset.get(element);

            if (!tset.containsKey(element)) {
                // TSet does not contain the element yet
                // Add the element to the TSet
                tset.put(element, new HashSet<>(uuids));
            } else if (eset.get(element).size() > tset.get(element).size()) {
                // TSet contains the element but there are more uuids for element in the ESet
                tset.get(element).addAll(uuids);
            }
        }
    }

    @Override
    public void merge(Crdt other) {
        ORSet<E> otherSet = (ORSet<E>) other;

        for (E e : otherSet.eset.keySet()) {
            if (eset.containsKey(e)) {
                eset.get(e).addAll(otherSet.eset.get(e));
            } else {
                eset.put(e, new HashSet<>(otherSet.eset.get(e)));
            }
        }

        for (E e : otherSet.tset.keySet()) {
            if (tset.containsKey(e)) {
                tset.get(e).addAll(otherSet.tset.get(e));
            } else {
                tset.put(e, new HashSet<>(otherSet.tset.get(e)));
            }
        }
    }

    /**
     * Returns a string representation of the CRDT to be sent over the network.
     * e.g. [test(uuid|uuid|uuid);test2(uuid);test3(uuid)],[test(uuid,uuid)]
     */
    @Override
    public String toString() {
        StringBuilder output = new StringBuilder();
        if (!eset.isEmpty()) {
            output.append("[");
            output.append(eset.keySet().stream().map(e -> e + "(" + eset.get(e).stream().collect(Collectors.joining("|")) + ")").collect(Collectors.joining(";")));
            output.append("]");

            if (!tset.isEmpty()) {
                output.append(",[");
                output.append(tset.keySet().stream().map(e -> e + "(" + tset.get(e).stream().collect(Collectors.joining(",")) + ")").collect(Collectors.joining(";")));
                output.append("]");
            } else {
                output.append(",[]");
            }
        } else {
            output.append("[],[]");
        }
        return output.toString();
    }
}
