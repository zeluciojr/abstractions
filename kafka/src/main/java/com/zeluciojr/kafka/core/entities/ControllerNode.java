package com.zeluciojr.kafka.core.entities;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class ControllerNode extends RaftNode{

    public static ControllerNode ofNew() {
        var newControllerNode = new ControllerNode();
        newControllerNode.setId(UUID.randomUUID());
        newControllerNode.setType(NodeTypes.CONTROLLER);
        newControllerNode.setRaftState(RaftStates.FOLLOWER);
        return newControllerNode;
    }

    @Override
    public String toString() {
        return "Controller " + this.getId().toString() + " (" + this.getRaftState() + " for term " + this.getCurrentTerm() + ")";
    }
}
