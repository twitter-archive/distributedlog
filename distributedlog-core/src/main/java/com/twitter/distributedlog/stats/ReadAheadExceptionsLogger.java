package com.twitter.distributedlog.stats;

import org.apache.bookkeeper.stats.StatsLogger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReadAheadExceptionsLogger {

    private final StatsLogger statsLogger;
    private StatsLogger parentExceptionStatsLogger;
    private final ConcurrentMap<String, BKExceptionStatsLogger> exceptionStatsLoggers =
            new ConcurrentHashMap<String, BKExceptionStatsLogger>();

    public ReadAheadExceptionsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
    }

    public BKExceptionStatsLogger getBKExceptionStatsLogger(String phase) {
        // initialize the parent exception stats logger lazily
        if (null == parentExceptionStatsLogger) {
            parentExceptionStatsLogger = statsLogger.scope("exceptions");
        }
        BKExceptionStatsLogger exceptionStatsLogger = exceptionStatsLoggers.get(phase);
        if (null == exceptionStatsLogger) {
            exceptionStatsLogger = new BKExceptionStatsLogger(parentExceptionStatsLogger.scope(phase));
            BKExceptionStatsLogger oldExceptionStatsLogger =
                    exceptionStatsLoggers.putIfAbsent(phase, exceptionStatsLogger);
            if (null != oldExceptionStatsLogger) {
                exceptionStatsLogger = oldExceptionStatsLogger;
            }
        }
        return exceptionStatsLogger;
    }
}
