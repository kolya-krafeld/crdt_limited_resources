package test.crdt;

import main.crdt.ORSet;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class ORSetTest {

    @Test
    public void testAddAndRemove() {
        ORSet<String> set = new ORSet<>();
        set.add("a");
        set.add("b");
        set.add("c");

        Set<String> result = set.query();
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
        set.remove("b");
        result = set.query();
        assertFalse(result.contains("b"));

        set.add("b");
        result = set.query();
        assertTrue(result.contains("b"));

        set.remove("a");
        set.remove("b");
        set.remove("c");
        result = set.query();
        assertEquals(0, result.size());
    }

    @Test
    public void testMerge() {
        ORSet<String> set1 = new ORSet<>();
        ORSet<String> set2 = new ORSet<>();

        set1.add("a");
        set1.add("b");
        set1.add("c");

        set2.add("b");
        set2.add("c");
        set2.add("d");

        set1.merge(set2);

        Set<String> result = set1.query();
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
        assertTrue(result.contains("d"));
    }

    @Test
    public void testMergeWithRemovals() {
        ORSet<String> set1 = new ORSet<String>();
        ORSet<String> set2 = new ORSet<String>();

        set1.add("a");
        set1.add("b");
        set1.add("c");

        set2.add("b");
        set2.add("c");
        set2.add("d");

        set1.remove("a");
        set1.remove("b");
        set1.remove("c");

        set2.remove("b");
        set2.remove("c");
        set2.remove("d");

        set1.merge(set2);

        Set<String> result = set1.query();
        assertEquals(0, result.size());
    }

    @Test
    public void testToString() {
        ORSet<String> set = new ORSet<>();
        set.add("a");
        set.add("b");
        set.add("c");
        String crdtString = set.toString();
        ORSet<String> set2 = new ORSet<>(crdtString);

        assertEquals(set2.toString(), set.toString());


        set.remove("b");
        set.remove("c");
        crdtString = set.toString();
        set2 = new ORSet<String>(crdtString);
        assertEquals(set2.toString(), set.toString());
    }

}