package test;

import main.crdt.Crdt;
import main.crdt.PNCounter;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static main.jobs.CrdtMerger.generateMonotonicMergeMessage;
import static org.junit.Assert.*;

public class CrdtMergerTest {

    @Test
    public void testGenerateMonotonicMergeMessage() {
        Map<String, Crdt> crdtMap = new HashMap<>();
        crdtMap.put("1", new PNCounter(3));

        String expected = "merge-monotonic:1:pncounter:[0;0;0],[0;0;0]";
        String actual = generateMonotonicMergeMessage(crdtMap.entrySet().iterator().next());
        assertEquals(expected, actual);
    }

}