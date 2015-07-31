package com.twitter.distributedlog.client.stats;

import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;

/**
 * Stats Logger per operation type
 */
public class OpStatsLogger {

    private final Stat successLatencyStat;
    private final Stat failureLatencyStat;
    private final Stat redirectStat;

    public OpStatsLogger(StatsReceiver statsReceiver) {
        StatsReceiver latencyStatReceiver = statsReceiver.scope("latency");
        successLatencyStat = latencyStatReceiver.stat0("success");
        failureLatencyStat = latencyStatReceiver.stat0("failure");
        StatsReceiver redirectStatReceiver = statsReceiver.scope("redirects");
        redirectStat = redirectStatReceiver.stat0("times");
    }

    public void completeRequest(long micros, int numTries) {
        successLatencyStat.add(micros);
        redirectStat.add(numTries);
    }

    public void failRequest(long micros, int numTries) {
        failureLatencyStat.add(micros);
        redirectStat.add(numTries);
    }

}
