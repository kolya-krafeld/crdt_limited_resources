package test.crdt;

import main.crdt.LimitedResourceCrdt;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LimitedResourceCrdtTest {

    @Test
    void testCreatingCrdtFromString() {
        String crdtString = "[10;10;10],[1;8;0]";
        LimitedResourceCrdt crdt = new LimitedResourceCrdt(crdtString);
        assertEquals(List.of(10, 10, 10), crdt.getUpperCounter());
        assertEquals(List.of(1, 8, 0), crdt.getLowerCounter());
        assertEquals(3, crdt.getNumberOfProcesses());

        assertEquals(crdtString, crdt.toString());
    }

    @Test
    void testMerge() {
        LimitedResourceCrdt crdt1 = new LimitedResourceCrdt(3);
        LimitedResourceCrdt crdt2 = new LimitedResourceCrdt(3);

        crdt1.setUpper(0, 10);
        crdt1.setUpper(1, 10);
        crdt1.setUpper(2, 10);

        crdt2.merge(crdt1);
        assertTrue(crdt2.compare(crdt1));
    }

}