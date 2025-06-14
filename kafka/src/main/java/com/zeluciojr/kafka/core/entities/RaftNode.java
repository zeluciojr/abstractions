package com.zeluciojr.kafka.core.entities;

import com.cae.mapped_exceptions.specifics.InputMappedException;
import com.cae.mapped_exceptions.specifics.InternalMappedException;
import com.zeluciojr.kafka.adapters.autofeatures.autolog.LoggerAdapter;
import com.zeluciojr.kafka.core.entities.factories.ControllerNodeHeartbeatExecutorFactory;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class RaftNode extends Node{

    private static final Long HEARTBEAT_THRESHOLD = 200L;

    private RaftStates raftState;
    private List<ControllerNode> partners;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicLong currentTermTimeoutThreshold = new AtomicLong(0);
    private final ScheduledExecutorService scheduledTermTimeoutExecutor = Executors.newScheduledThreadPool(1);
    private Future<?> termTimeoutTask;
    private final AtomicInteger votesGranted = new AtomicInteger();
    private final AtomicInteger totalVotes = new AtomicInteger();
    private final AtomicBoolean alreadyLost = new AtomicBoolean(false);
    private final AtomicLong lastTermVoted = new AtomicLong(0);
    private final ScheduledExecutorService scheduledHeartbeatExecutor = Executors.newScheduledThreadPool(1);
    private final ExecutorService heartbeatExecutor = ControllerNodeHeartbeatExecutorFactory.createOrGetExecutor();
    private Future<?> heartbeatTask;

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
        var randomValue = ThreadLocalRandom.current().nextLong(HEARTBEAT_THRESHOLD + 50, 1000);
        this.currentTermTimeoutThreshold.set(randomValue);
    }

    private void startTimeoutCount() {
        this.generateRandomThresholdForTermTimeout();
        this.termTimeoutTask = this.scheduledTermTimeoutExecutor.schedule(
                this::startNewTermElection,
                this.currentTermTimeoutThreshold.get(),
                TimeUnit.MILLISECONDS
        );
    }

    private void startNewTermElection(){
        this.becomeCandidate();
        this.increaseCurrentTerm();
        LoggerAdapter.SINGLETON.logInfo("Started new term election: " + this);
        this.receiveCandidateRequest(this);
        this.getPartners().forEach(partner -> partner.receiveCandidateRequest(this));
    }

    private void becomeCandidate() {
        this.raftState = RaftStates.CANDIDATE;
    }

    private void increaseCurrentTerm(){
        this.currentTerm.incrementAndGet();
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
        else if (!this.alreadyLost.get() && this.majorityHasVoted()){
            this.alreadyLost.set(true);
            this.becomeFollower();
            this.startTimeoutCount();
        }
        if (this.alreadyLost.get())
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
                            CompletableFuture.runAsync(() -> partner.receiveLeaderHeartbeat(this.getCurrentTerm().get()), this.heartbeatExecutor)
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
        this.alreadyLost.set(false);
        this.votesGranted.set(0);
        this.totalVotes.set(0);
    }

    protected void receiveLeaderHeartbeat(Long allegedlyLeadersCurrentTerm){
        LoggerAdapter.SINGLETON.logInfo("Heartbeat received at " + this);
        if (this.getCurrentTerm().get() < allegedlyLeadersCurrentTerm)
            this.currentTerm.set(allegedlyLeadersCurrentTerm);
        if (this.getCurrentTerm().get() == (allegedlyLeadersCurrentTerm)){
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

    protected void receiveCandidateRequest(RaftNode candidate){
        if (this.lastTermVoted.get() < candidate.getCurrentTerm().get()){
            var vote = this.getCurrentTerm().get() <= candidate.getCurrentTerm().get();
            candidate.receiveVote(vote);
            this.lastTermVoted.set(candidate.getCurrentTerm().get());
            LoggerAdapter.SINGLETON.logInfo(this + " is voting " + vote + " for " + candidate);
        }
    }

}
