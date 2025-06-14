package com.zeluciojr.kafka;

import com.zeluciojr.kafka.core.entities.Cluster;
import com.zeluciojr.kafka.core.entities.factories.BrokerNodesFactory;
import com.zeluciojr.kafka.core.entities.factories.ControllerNodesFactory;

public class App {

    public static void main(String[] args) {
        var cluster = Cluster.of(ControllerNodesFactory.create(4), BrokerNodesFactory.create(8));
    }

}
