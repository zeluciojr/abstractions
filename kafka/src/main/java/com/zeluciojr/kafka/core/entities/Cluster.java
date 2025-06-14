package com.zeluciojr.kafka.core.entities;

import com.cae.entities.Entity;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Builder
public class Cluster implements Entity {

    public static Cluster of(List<Node> controllerNodes, List<Node> brokerNodes){
        var nodes = new ArrayList<Node>();
        nodes.addAll(controllerNodes);
        nodes.addAll(brokerNodes);
        return Cluster.builder()
                .controllerNodes(controllerNodes)
                .allNodes(nodes)
                .build();
    }

    private List<Node> controllerNodes;
    private List<Node> allNodes;

}
