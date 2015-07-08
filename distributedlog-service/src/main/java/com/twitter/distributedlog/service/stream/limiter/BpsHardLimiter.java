package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.service.DistributedLogServiceImpl;
import com.twitter.distributedlog.limiter.AbstractRequestLimiter;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Proxy service bytes request limiter which throws an overcap exception and records hard limit
 * stats when the limit is hit.
 */
public class BpsHardLimiter extends BpsRequestLimiter {
    final String name;
    final Counter hardLimitCounter;

    public BpsHardLimiter(int hardLimit, StatsLogger statsLogger, String name) {
        super(hardLimit);
        this.name = name;
        this.hardLimitCounter = statsLogger.scope("limiter").scope("bps").getCounter("hard_limit_hit");
        LOG.debug("Init BpsHardLimiter hardLimit={}", hardLimit);
    }

    @Override
    protected void overlimit() throws OverCapacityException {
        hardLimitCounter.inc();
        throw new OverCapacityException("BPS limit exceeded for stream " + name);
    }
}
