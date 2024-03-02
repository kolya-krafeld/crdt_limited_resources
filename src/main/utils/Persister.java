package main.utils;

import main.Node;
import main.crdt.LimitedResourceCrdt;

import java.io.*;
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

    /**
     * Persists current state of the node to a local file.
     */
    public void persistState(boolean inCoordinationPhase, int roundNumber, LimitedResourceCrdt state, Optional<LimitedResourceCrdt> acceptedState) {
        String output = "inCoordinationPhase:" + inCoordinationPhase + "\n";
        output += "roundNumber:" + roundNumber + "\n";
        output += "state:" + state.toString();
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

    /**
     * Reads state from file and sets it in the node.
     */
    public void loadState() {
        // Read state from file
        try {
            File file = new File(String.format(FILEPATH, node.getOwnPort()));

            BufferedReader br = new BufferedReader(new FileReader(file));

            // Read every line one by one
            String line;
            while ((line = br.readLine()) != null) {
                setLoadedState(line);
            }
        } catch (Exception e) {
            System.out.println("Error reading from file");
            throw new RuntimeException(e);
        }
    }

    public void setLoadedState(String line) {
        String[] keyValue = line.split(":");
        switch(keyValue[0]) {
            case "inCoordinationPhase":
                node.setInCoordinationPhase(Boolean.parseBoolean(keyValue[1]));
                break;
            case "roundNumber":
                node.setRoundNumber(Integer.parseInt(keyValue[1]));
                break;
            case "state":
                node.setCrdt(new LimitedResourceCrdt(keyValue[1]));
                break;
            case "acceptedState":
                node.setAcceptedCrdt(new LimitedResourceCrdt(keyValue[1]));
                break;
        }
    }


}
