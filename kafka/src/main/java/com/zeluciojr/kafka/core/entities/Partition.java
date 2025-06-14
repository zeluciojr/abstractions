package com.zeluciojr.kafka.core.entities;

import com.cae.entities.Entity;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@Builder
public class Partition implements Entity {

    private UUID id;
    private Boolean replica;
    private UUID original;

}
