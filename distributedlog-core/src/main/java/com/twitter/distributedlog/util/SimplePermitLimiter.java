package com.twitter.distributedlog.util;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePermitLimiter implements PermitLimiter {

    static final Logger LOG = LoggerFactory.getLogger(SimplePermitLimiter.class);
    final Counter acquireFailureCounter;
    final OpStatsLogger permitsMetric;
    final AtomicInteger permits;
    final int permitsMax;
    final boolean darkmode;
    final Feature disableWriteLimitFeature;

    public SimplePermitLimiter(boolean darkmode, int permitsMax, StatsLogger statsLogger,
                               boolean singleton, Feature disableWriteLimitFeature) {
        this.permits = new AtomicInteger(0);
        this.permitsMax = permitsMax;
        this.darkmode = darkmode;
        this.disableWriteLimitFeature = disableWriteLimitFeature;

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

    public boolean isDarkmode() {
        return darkmode || disableWriteLimitFeature.isAvailable();
    }

    @Override
    public boolean acquire() {
        permitsMetric.registerSuccessfulEvent(permits.get());
        if (permits.incrementAndGet() <= permitsMax || isDarkmode()) {
            return true;
        } else {
            acquireFailureCounter.inc();
            permits.decrementAndGet();
            return false;
        }
    }

    @Override
    public void release(int permitsToRelease) {
        permits.addAndGet(-permitsToRelease);
    }

    @VisibleForTesting
    public int getPermits() {
        return permits.get();
    }
}
