package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.AbstractRequestLimiter;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Proxy service requests rate limiter which records soft limit stats when the limit is hit.
 */
public class RpsSoftLimiter extends RpsRequestLimiter {
    final Counter softLimitCounter;

    public RpsSoftLimiter(int softLimit, StatsLogger statsLogger) {
        super(softLimit);
        this.softLimitCounter = statsLogger.scope("limiter").scope("rps").getCounter("soft_limit_hit");
        LOG.debug("Init RpsSoftLimiter softLimit={}", softLimit);
    }

    @Override
    protected void overlimit() throws OverCapacityException {
        softLimitCounter.inc();
    }
}
