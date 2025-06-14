package com.zeluciojr.kafka.core.entities;

public enum RaftStates {

    FOLLOWER,
    CANDIDATE,
    LEADER;

    public boolean isLeader(){
        return this.equals(LEADER);
    }

    public boolean isFollower() {
        return this.equals(FOLLOWER);
    }
}
