package com.zeluciojr.kafka.core.entities.factories;

import com.zeluciojr.kafka.adapters.autofeatures.autolog.LoggerAdapter;
import com.zeluciojr.kafka.core.entities.ControllerNode;
import com.zeluciojr.kafka.core.entities.Node;

import java.util.ArrayList;
import java.util.List;

public class ControllerNodesFactory {

    public static List<Node> create(Integer desiredAmount){
        desiredAmount = ControllerNodesFactory.makeDesiredAmountUneven(desiredAmount);
        var controllers = createTheControllers(desiredAmount);
        ControllerNodesFactory.connectAllControllersToEachOther(controllers);
        ControllerNodesFactory.initializeControllers(controllers);
        return controllers;
    }

    private static List<Node> createTheControllers(Integer desiredAmount) {
        var counter = 0;
        var controllers = new ArrayList<Node>();
        while (counter < desiredAmount){
            var newNode = ControllerNode.ofNew();
            controllers.add(newNode);
            counter++;
        }
        return controllers;
    }

    private static void connectAllControllersToEachOther(List<Node> allControllersBeingCreated) {
        allControllersBeingCreated.stream()
            .map(node -> (ControllerNode) node)
            .forEach(controller -> {
                var allOfItsPartners = allControllersBeingCreated.stream()
                    .filter(node -> node.getId() != controller.getId())
                    .toList();
                controller.setPartners(allOfItsPartners);
            });
    }

    private static void initializeControllers(List<Node> controllers) {
        controllers.stream()
                .map(a -> (ControllerNode) a)
                .forEach(ControllerNode::initializeRaft);
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
