package com.zeluciojr.kafka.core.entities.factories;

import com.zeluciojr.kafka.adapters.autofeatures.autolog.LoggerAdapter;
import com.zeluciojr.kafka.core.entities.ControllerNode;
import com.zeluciojr.kafka.core.entities.Node;

import java.util.ArrayList;
import java.util.List;

public class ControllerNodesFactory {

    public static List<Node> create(Integer desiredAmount){
        desiredAmount = ControllerNodesFactory.makeDesiredAmountUneven(desiredAmount);
        var counter = 0;
        var allControllersBeingCreated = new ArrayList<Node>();
        while (counter < desiredAmount){
            var newNode = ControllerNode.ofNew();
            allControllersBeingCreated.add(newNode);
            counter++;
        }
        allControllersBeingCreated.stream()
            .map(node -> (ControllerNode) node)
            .forEach(controller -> {
                var allOfItsPartners = allControllersBeingCreated.stream()
                        .filter(node -> node.getId() != controller.getId())
                        .toList();
                controller.setPartners(allOfItsPartners);
        });
        var aa = allControllersBeingCreated.stream().map(a -> (ControllerNode) a).toList();
        aa.forEach(ControllerNode::initializeRaft);
        return allControllersBeingCreated;
    }

    private static Integer makeDesiredAmountUneven(Integer desiredAmount) {
        return isEven(desiredAmount)? desiredAmount + 1 : desiredAmount;
    }

    private static Boolean isEven(Integer desiredAmount){
        var isEven = desiredAmount % 2 == 0;
        if (isEven)
            LoggerAdapter.SINGLETON.logInfo(desiredAmount + " was even. For that reason we are increasing it");
        return isEven;
    }

}
