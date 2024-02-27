import crdt.LimitedResourceCrdt;

public class Main {
    public static void main(String[] args) {
        LimitedResourceCrdt crdt1 = new LimitedResourceCrdt(3);
        LimitedResourceCrdt crdt2 = new LimitedResourceCrdt(3);

        crdt1.setUpper(0, 10);
        crdt1.setUpper(1, 10);
        crdt1.setUpper(2, 10);

        crdt2.merge(crdt1);
        System.out.println("Compare both crdts:" + crdt2.compare(crdt1));

        System.out.println("Try to set lower counter above legal value: " + crdt2.setLower(0, 12));

    }
}