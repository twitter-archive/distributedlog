package com.twitter.distributedlog.util;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePermitLimiter implements PermitLimiter {

    static final Logger LOG = LoggerFactory.getLogger(SimplePermitLimiter.class);
    Counter acquireFailureCounter;
    OpStatsLogger permitsMetric;

    AtomicInteger permits;
    int permitsMax;
    boolean darkmdode;

    public SimplePermitLimiter(boolean darkmdode, int permitsMax, StatsLogger statsLogger, boolean singleton) {
        this.permits = new AtomicInteger(0);
        this.permitsMax = permitsMax;
        this.darkmdode = darkmdode;

        // stats
        if (singleton) {
            statsLogger.registerGauge("permits", new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }
                @Override
                public Number getSample() {
                    return permits.get();
                }
            });
        }
        acquireFailureCounter = statsLogger.getCounter("acquireFailure");
        permitsMetric = statsLogger.getOpStatsLogger("permits");
    }

    @Override
    public boolean acquire() {
        permitsMetric.registerSuccessfulEvent(permits.get());
        if (permits.incrementAndGet() <= permitsMax || darkmdode) {
            return true;
        } else {
            acquireFailureCounter.inc();
            permits.decrementAndGet();
            return false;
        }
    }

    @Override
    public void release() {
        permits.decrementAndGet();
    }

    @VisibleForTesting
    public int getPermits() {
        return permits.get();
    }
}
