package com.zeluciojr.kafka.core.entities;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum NodeTypes {

    CONTROLLER("C"),
    BROKER("B"),
    CONTROLLER_BROKER("CB");

    private final String name;

    public boolean isController() {
        return this.equals(NodeTypes.CONTROLLER) || this.equals(NodeTypes.CONTROLLER_BROKER);
    }
}
