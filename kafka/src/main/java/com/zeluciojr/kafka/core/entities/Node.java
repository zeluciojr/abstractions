package com.zeluciojr.kafka.core.entities;

import com.cae.entities.Entity;
import com.cae.mapped_exceptions.specifics.InternalMappedException;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Getter
@Setter
public class Node implements Entity {

    public static Node of(NodeTypes type){
        var newNode = new Node();
        newNode.setId(UUID.randomUUID());
        newNode.setType(type);
        return newNode;
    }

    private UUID id;
    private NodeTypes type;
    private Map<Topic, Partition> assignedPartitions = new HashMap<>();

    public boolean isController(){
        return Optional.ofNullable(this.getType())
            .orElseThrow(() -> new InternalMappedException(
                "Couldn't check whether node is a controller",
                "Its type was null"
            ))
            .isController();
    }

}
