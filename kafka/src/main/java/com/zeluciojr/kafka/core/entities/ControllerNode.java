package com.zeluciojr.kafka.core.entities;

import com.cae.mapped_exceptions.specifics.InputMappedException;
import com.cae.mapped_exceptions.specifics.InternalMappedException;
import com.zeluciojr.kafka.adapters.autofeatures.autolog.LoggerAdapter;
import com.zeluciojr.kafka.core.entities.factories.ControllerNodeHeartbeatExecutorFactory;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class ControllerNode extends Node{

    private static final Long HEARTBEAT_THRESHOLD = 200L;

    private RaftStates raftState;
    private List<ControllerNode> partners;
    private Long currentTerm;
    private Long currentTermTimeoutThreshold;
    private final ScheduledExecutorService scheduledTermTimeoutExecutor = Executors.newScheduledThreadPool(1);
    private Future<?> termTimeoutTask;
    private AtomicInteger votesGranted;
    private AtomicInteger totalVotes;
    private Boolean alreadyLost = false;
    private Long lastTermVoted;
    private final ScheduledExecutorService scheduledHeartbeatExecutor = Executors.newScheduledThreadPool(1);
    private final ExecutorService heartbeatExecutor = ControllerNodeHeartbeatExecutorFactory.createOrGetExecutor();
    private Future<?> heartbeatTask;

    public static ControllerNode ofNew() {
        var newControllerNode = new ControllerNode();
        newControllerNode.setId(UUID.randomUUID());
        newControllerNode.setType(NodeTypes.CONTROLLER);
        newControllerNode.setCurrentTerm(0L);
        newControllerNode.setLastTermVoted(-1L);
        newControllerNode.setVotesGranted(new AtomicInteger(0));
        newControllerNode.setTotalVotes(new AtomicInteger(0));
        newControllerNode.setRaftState(RaftStates.FOLLOWER);
        return newControllerNode;
    }

    public void initializeRaft(List<Node> partners){
        LoggerAdapter.SINGLETON.logInfo("Initialized: " + this);
        this.setPartners(partners);
        this.startTimeoutCount();
    }

    public void setPartners(List<Node> partners){
        this.partners = partners.stream()
                .filter(partner -> partner instanceof ControllerNode)
                .map(partner -> (ControllerNode) partner)
                .toList();
        if (this.partners.isEmpty())
            throw new InputMappedException("No list of controllers set. Make sure every instance is of type ControllerNode.");
    }

    private void generateRandomThresholdForTermTimeout() {
        this.setCurrentTermTimeoutThreshold(
                ThreadLocalRandom.current().nextLong(HEARTBEAT_THRESHOLD + 50, 1000)
        );
    }

    private void startTimeoutCount() {
        this.generateRandomThresholdForTermTimeout();
        this.termTimeoutTask = this.scheduledTermTimeoutExecutor.schedule(
                this::startNewTermElection,
                this.currentTermTimeoutThreshold,
                TimeUnit.MILLISECONDS
        );
    }

    private void startNewTermElection(){
        this.becomeCandidate();
        this.increaseCurrentTerm();
        this.receiveVote(true);
        LoggerAdapter.SINGLETON.logInfo("Started new term election: " + this);
        this.getPartners().forEach(partner -> partner.receiveCandidateRequest(this));
    }

    private void becomeCandidate() {
        this.raftState = RaftStates.CANDIDATE;
    }

    private void increaseCurrentTerm(){
        this.currentTerm += 1;
    }

    protected void receiveVote(Boolean approved){
        this.totalVotes.incrementAndGet();
        if (approved)
            this.votesGranted.incrementAndGet();
        this.checkResults();
    }

    private void checkResults(){
        if (this.hasWonElectionForCurrentTerm()){
            this.becomeLeader();
            this.cleanupTransientElectionData();
        }
        else if (!this.alreadyLost && this.majorityHasVoted()){
            this.alreadyLost = true;
            this.becomeFollower();
            this.startTimeoutCount();
        }
        if (this.alreadyLost)
            this.cleanupTransientElectionData();
    }

    private void becomeFollower() {
        this.raftState = RaftStates.FOLLOWER;
        LoggerAdapter.SINGLETON.logInfo("Became follower: " + this);
    }

    private boolean hasWonElectionForCurrentTerm() {
        return this.getQuorumSize() < this.votesGranted.get();
    }

    private void becomeLeader() {
        this.raftState = RaftStates.LEADER;
        this.heartbeatTask = this.scheduledHeartbeatExecutor.scheduleAtFixedRate(
                this::sendHeartbeat,
                0L,
                HEARTBEAT_THRESHOLD,
                TimeUnit.MILLISECONDS
        );
        LoggerAdapter.SINGLETON.logInfo("Leadership shifted: " + this);
    }

    private void sendHeartbeat() {
        LoggerAdapter.SINGLETON.logInfo("Heartbeats getting sent by " + this);
        if (this.isLeader()){
            this.getPartners()
                .forEach(partner ->
                    CompletableFuture.runAsync(() -> partner.receiveLeaderHeartbeat(this.getCurrentTerm()), this.heartbeatExecutor)
                );
        }
    }

    private boolean isLeader() {
        return Optional.ofNullable(this.raftState)
                .orElseThrow(() -> new InternalMappedException("Couldn't check whether it is the leader", "No state set"))
                .isLeader();
    }

    private boolean majorityHasVoted() {
        return this.getQuorumSize() < this.totalVotes.get();
    }

    private Integer getQuorumSize(){
        return (this.getPartners().size() + 1) / 2;
    }

    private void cleanupTransientElectionData() {
        this.alreadyLost = false;
        this.votesGranted.set(0);
        this.totalVotes.set(0);
    }

    protected void receiveLeaderHeartbeat(Long allegedlyLeadersCurrentTerm){
        LoggerAdapter.SINGLETON.logInfo("Heartbeat received at " + this);
        if (this.getCurrentTerm() < allegedlyLeadersCurrentTerm)
            this.setCurrentTerm(allegedlyLeadersCurrentTerm);
        if (this.getCurrentTerm().equals(allegedlyLeadersCurrentTerm)){
            var timeoutCanceled = this.termTimeoutTask.cancel(false);
            if (timeoutCanceled)
                this.startTimeoutCount();
            this.cancelPossibleHeartbeats();
            this.enforceCorrectFollowerState();
        }
    }

    private void enforceCorrectFollowerState() {
        if (this.isLeader()) this.becomeFollower();
    }

    private void cancelPossibleHeartbeats() {
        Optional.ofNullable(this.heartbeatTask).ifPresent(task -> task.cancel(false));
    }

    protected void receiveCandidateRequest(ControllerNode candidate){
        if (this.lastTermVoted < candidate.getCurrentTerm()){
            candidate.receiveVote(this.getCurrentTerm() <= candidate.getCurrentTerm());
            this.lastTermVoted = candidate.getCurrentTerm();
        }
    }

    @Override
    public String toString() {
        return "Controller " + this.getId().toString() + " (" + this.raftState + ") ";
    }
}
