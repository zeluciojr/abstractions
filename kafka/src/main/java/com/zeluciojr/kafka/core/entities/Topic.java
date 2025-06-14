package com.zeluciojr.kafka.core.entities;

import com.cae.entities.Entity;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Builder
public class Topic implements Entity {

    private UUID id;
    private String name;
    private List<Partition> partitions;

}
