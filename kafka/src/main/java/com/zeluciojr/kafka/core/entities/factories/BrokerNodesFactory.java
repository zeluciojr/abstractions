package com.zeluciojr.kafka.core.entities.factories;

import com.zeluciojr.kafka.core.entities.Node;

import java.util.ArrayList;
import java.util.List;

public class BrokerNodesFactory {

    public static List<Node> create(Integer amount){
        return new ArrayList<>();
    }

}
