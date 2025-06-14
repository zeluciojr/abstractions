package com.zeluciojr.kafka.core.entities.factories;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ControllerNodeHeartbeatExecutorFactory {

    private static ExecutorService executorService;

    public static ExecutorService createOrGetExecutor(){
        return Optional.ofNullable(executorService).orElseGet(() -> {
            executorService = new ThreadPoolExecutor(
                    10,
                    100,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1000),
                    new ThreadPoolExecutor.AbortPolicy()
            );
            return executorService;
        });
    }

}
