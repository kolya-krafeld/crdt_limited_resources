package main.utils;

import main.Node;
import main.crdt.LimitedResourceCrdt;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.Optional;

/**
 * Persists state of nodes to a local file. Can read state from file after a restart.
 */
public class Persister {

    private static final String FILEPATH = "states/node%s.txt";

    private Node node;

    public Persister(Node node) {
        this.node = node;
    }

    public void persistState(LimitedResourceCrdt state, Optional<LimitedResourceCrdt> acceptedState) {
        String output = "state:" + state.toString();
        if (acceptedState.isPresent()) {
            output += "\nacceptedState:" + acceptedState.get().toString();
        }

        try {
            FileOutputStream outputStream = new FileOutputStream(String.format(FILEPATH, node.getOwnPort()));
            byte[] strToBytes = output.getBytes();
            outputStream.write(strToBytes);

            outputStream.close();

        } catch (Exception e) {
            System.out.println("Error writing to file");
            throw new RuntimeException(e);
        }
    }
}
