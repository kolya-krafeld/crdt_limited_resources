package main.ballot_leader_election;

import main.Node;
import main.utils.Logger;
import main.utils.Message;
import main.utils.MessageType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class BallotLeaderElection {
    private final Logger logger;
    public int rnd;
    private Node node;
    private int timeout;
    private Set<BallotEntry> ballotEntries = new LinkedHashSet<>();

    public BallotLeaderElection(Node node, int timeout) {
        this.logger = node.logger;
        this.node = node;
        this.timeout = timeout;

    }

    public void addBallotEntry(int ballotNumber, Boolean quorumConnected, int id) {
        ballotEntries.add(new BallotEntry(ballotNumber, quorumConnected, id));
    }

    public void start() {
        while (true) {
            if (roundOfLeaderElection()) {
                break;
            }
        }
    }

    private boolean roundOfLeaderElection() {
        String message = MessageType.ELECTION_REQUEST.getTitle() + ":" + rnd;
        node.messageHandler.broadcastIncludingSelf(message);
        int startTime = node.getTime();
        while (node.getTime() - startTime < timeout) {
            if (ballotEntries.size() == this.node.getNodesPorts().size()) {
                logger.info("LeaderElection: got all election messages");
                break;
            }
        }
        logger.info("LeaderElection: got " + ballotEntries.size() + " out of " + this.node.getNodesPorts().size() + " election messages");
        if (ballotEntries.size() >= this.node.getQuorumSize()) {
            logger.info("LeaderElection: got quorum");
            if (checkLeader()) {
                return true;
            } else {
                logger.info("LeaderElection: No leader found, starting new election");
            }
        } else {
            logger.info("LeaderElection: Not connected to quorum");
            node.setQuorumConnected(false);
        }
        ballotEntries.clear();
        rnd++;
        return false;
    }

    private boolean checkLeader() {
        LinkedHashSet candidates = ballotEntries.stream().filter(BallotEntry::quorumConnected).collect(Collectors.toCollection(LinkedHashSet::new));
        if (candidates.isEmpty()) {
            logger.info("LeaderElection: No quorum connected candidate");
            return false;
        } else {
            BallotEntry maxCandidate = getMaxCandidate(candidates);
            logger.info("LeaderElection: Highest candidate: ID " + maxCandidate.id + " with ballot number " + maxCandidate.ballotNumber);
            if (maxCandidate.ballotNumber <= this.node.leaderBallotNumber) {
                this.node.ballotNumber = this.node.leaderBallotNumber + 1;
                logger.info("LeaderElection: No candidate with higher ballot number, increasing own ballot number to " + (this.node.ballotNumber) + " and starting new election");
                this.node.setQuorumConnected(true);
                return false;
            } else if (maxCandidate.ballotNumber > this.node.leaderBallotNumber) {
                this.node.leaderBallotNumber = maxCandidate.ballotNumber;
                this.node.logger.info("LeaderElection: FOUND LEADER: ID " + maxCandidate.id + " with ballot number " + maxCandidate.ballotNumber);
                String message = MessageType.ELECTION_RESULT.getTitle() + ":" + maxCandidate.id + "," + maxCandidate.ballotNumber;
                node.messageHandler.broadcastIncludingSelf(message);
                return true;
            }
        }
        return false;
    }

    private BallotEntry getMaxCandidate(Set<BallotEntry> candidates) {
        ArrayList<BallotEntry> sortedCandidates = new ArrayList<>(candidates);
        sortedCandidates.sort(Comparator.comparing(BallotEntry::ballotNumber)
                .thenComparing(BallotEntry::id));
        return sortedCandidates.get(sortedCandidates.size() - 1);
    }

    public void handleElectionReply(Message message) {
        ElectionReplyMessage electionReply = ElectionReplyMessage.fromString(message.getContent());
        if (electionReply.rnd == this.rnd) {
            addBallotEntry(electionReply.ballotNumber, electionReply.isQuorumConnected, message.getPort());
        }
    }

    public record BallotEntry(int ballotNumber, Boolean quorumConnected, int id) {
    }

}
