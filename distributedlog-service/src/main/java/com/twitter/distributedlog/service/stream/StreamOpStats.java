package com.twitter.distributedlog.service.stream;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Encapsulate stream op stats construction to make it easier to access stream
 * op stats consistently from different scopes.
 */
public class StreamOpStats {
    private final StatsLogger requestStatsLogger;
    private final StatsLogger recordsStatsLogger;
    private final StatsLogger requestDeniedStatsLogger;
    private final StatsLogger streamStatsLogger;

    public StreamOpStats(StatsLogger statsLogger,
                         StatsLogger perStreamStatsLogger) {
        this.requestStatsLogger = statsLogger.scope("request");
        this.recordsStatsLogger = statsLogger.scope("records");
        this.requestDeniedStatsLogger = statsLogger.scope("denied");
        this.streamStatsLogger = perStreamStatsLogger;
    }

    public OpStatsLogger requestLatencyStat(String opName) {
        return requestStatsLogger.getOpStatsLogger(opName);
    }

    public StatsLogger requestScope(String scopeName) {
        return requestStatsLogger.scope(scopeName);
    }

    public Counter scopedRequestCounter(String opName, String counterName) {
        return requestScope(opName).getCounter(counterName);
    }

    public Counter requestCounter(String counterName) {
        return requestStatsLogger.getCounter(counterName);
    }

    public Counter requestPendingCounter(String counterName) {
        return requestCounter(counterName);
    }

    public Counter requestDeniedCounter(String counterName) {
        return requestDeniedStatsLogger.getCounter(counterName);
    }

    public Counter recordsCounter(String counterName) {
        return recordsStatsLogger.getCounter(counterName);
    }

    public StatsLogger streamRequestStatsLogger(String streamName) {
        return streamStatsLogger.scope(streamName);
    }

    public StatsLogger streamRequestScope(String streamName, String scopeName) {
        return streamRequestStatsLogger(streamName).scope(scopeName);
    }

    public OpStatsLogger streamRequestLatencyStat(String streamName, String opName) {
        return streamRequestStatsLogger(streamName).getOpStatsLogger(opName);
    }

    public Counter streamRequestCounter(String streamName, String opName, String counterName) {
        return streamRequestScope(streamName, opName).getCounter(counterName);
    }
}
