package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.service.DistributedLogServiceImpl;
import com.twitter.distributedlog.limiter.AbstractRequestLimiter;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Proxy service bytes request limiter which records hard limit stats when the limit is hit.
 */
public class BpsSoftLimiter extends BpsRequestLimiter {
    final Counter softLimitCounter;

    public BpsSoftLimiter(int softLimit, StatsLogger statsLogger) {
        super(softLimit);
        this.softLimitCounter = statsLogger.scope("limiter").scope("bps").getCounter("soft_limit_hit");
        LOG.debug("Init BpsSoftLimiter softLimit={}", softLimit);
    }

    @Override
    protected void overlimit() throws OverCapacityException {
        softLimitCounter.inc();
    }
}
