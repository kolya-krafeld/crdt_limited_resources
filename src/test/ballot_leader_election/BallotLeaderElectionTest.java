package test.ballot_leader_election;

import main.Config;
import main.Node;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BallotLeaderElectionTest {

    @Test
    public void testBallotLeaderElection() throws InterruptedException {


        Config config = new Config(1000, 3, 1, 5);

        List<Integer> ports = List.of(8000, 8001, 8002, 8003, 8004);
        List<Node> nodes = new ArrayList<>();
        ports.forEach(port -> {
            Node node = new Node(port, ports, config);
            try {
                node.init(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            nodes.add(node);
        });


        Thread.sleep(6000);
        nodes.get(0).kill();
        Thread.sleep(60000);
    }


}
