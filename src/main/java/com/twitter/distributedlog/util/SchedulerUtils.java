package com.twitter.distributedlog.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class SchedulerUtils {

    static final Logger logger = LoggerFactory.getLogger(SchedulerUtils.class);

    public static void shutdownScheduler(ExecutorService service, long timeout, TimeUnit timeUnit) {
        if (null == service) {
            return;
        }
        service.shutdown();
        try {
            service.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            logger.warn("Interrupted when shutting down scheduler : ", e);
        }
        service.shutdownNow();
    }
}
