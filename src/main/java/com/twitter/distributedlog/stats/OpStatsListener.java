package com.twitter.distributedlog.stats;

import com.google.common.base.Stopwatch;
import com.twitter.util.FutureEventListener;
import org.apache.bookkeeper.stats.OpStatsLogger;
import java.util.concurrent.TimeUnit;

public class OpStatsListener<T> implements FutureEventListener<T> {
    OpStatsLogger opStatsLogger;
    Stopwatch stopwatch;

    public OpStatsListener(OpStatsLogger opStatsLogger, Stopwatch stopwatch) {
        this.opStatsLogger = opStatsLogger;
        if (null == stopwatch) {
            this.stopwatch = Stopwatch.createStarted();
        } else {
            this.stopwatch = stopwatch;
        }
    }

    public OpStatsListener(OpStatsLogger opStatsLogger) {
        this(opStatsLogger, null);
    }

    @Override
    public void onSuccess(T value) {
        opStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
    }

    @Override
    public void onFailure(Throwable cause) {
        opStatsLogger.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
    }
}