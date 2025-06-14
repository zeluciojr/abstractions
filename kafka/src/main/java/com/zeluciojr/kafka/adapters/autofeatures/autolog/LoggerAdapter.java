package com.zeluciojr.kafka.adapters.autofeatures.autolog;

import com.cae.autolog.Logger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggerAdapter implements Logger {

    public static final Logger SINGLETON = new LoggerAdapter();

    @Override
    public void logInfo(String s) {
        log.info(s);
    }

    @Override
    public void logError(String s) {
        log.error(s);
    }

    @Override
    public void logDebug(String s) {
        log.debug(s);
    }
}
