package com.twitter.distributedlog.stats;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to raise alert when we detect an event that should never happen in production
 */
public class AlertStatsLogger {
    private static final Logger logger = LoggerFactory.getLogger(AlertStatsLogger.class);

    public static final String ALERT_STAT = "dl_alert";

    private final StatsLogger globalStatsLogger;
    private final StatsLogger scopedStatsLogger;
    private final String scope;
    private Counter globalCounter = null;
    private Counter scopedCounter = null;

    public AlertStatsLogger(StatsLogger globalStatsLogger, String scope) {
        this.globalStatsLogger = globalStatsLogger;
        this.scope = scope;
        this.scopedStatsLogger = globalStatsLogger.scope(scope);
    }

    private String format(String msg) {
        return msg.startsWith("ALERT!: ") ? msg : ("ALERT!: " + "(" + scope + "):" + msg);
    }

    /**
     * Report an alertable condition". Prefixes "ALERT!: " if not already prefixed.
     */
    public void raise(String msg, Object... args) {
        if (null == globalCounter) {
            globalCounter = globalStatsLogger.getCounter(ALERT_STAT);
        }

        if (null == scopedCounter) {
            scopedCounter = scopedStatsLogger.getCounter(ALERT_STAT);
        }

        globalCounter.inc();
        scopedCounter.inc();
        logger.error(format(msg), args);
        logger.error("fake exception to generate stack trace", new Exception());
    }
}