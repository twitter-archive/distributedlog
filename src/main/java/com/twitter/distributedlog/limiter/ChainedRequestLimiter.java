package com.twitter.distributedlog.limiter;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import com.twitter.distributedlog.exceptions.OverCapacityException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Chain request limiters for easier management of multi limiter policy.
 */
public class ChainedRequestLimiter<Request> implements RequestLimiter<Request> {
    private final ImmutableList<RequestLimiter<Request>> limiters;
    private final OpStatsLogger applyTime;

    public static class Builder<Request> {
        private final ImmutableList.Builder<RequestLimiter<Request>> limitersBuilder;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;

        public Builder() {
            this.limitersBuilder = new ImmutableList.Builder<RequestLimiter<Request>>();
        }

        public Builder addLimiter(RequestLimiter<Request> limiter) {
            this.limitersBuilder.add(limiter);
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public ChainedRequestLimiter<Request> build() {
            return new ChainedRequestLimiter<Request>(limitersBuilder.build(), statsLogger);
        }
    }

    private ChainedRequestLimiter(ImmutableList<RequestLimiter<Request>> limiters,
                                  StatsLogger statsLogger) {
        this.limiters = limiters;
        this.applyTime = statsLogger.getOpStatsLogger("apply");
    }

    public void apply(Request request) throws OverCapacityException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            for (RequestLimiter limiter : limiters) {
                limiter.apply(request);
            }
        } finally {
            applyTime.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
        }
    }
}
