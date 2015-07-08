package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.AbstractRequestLimiter;
import com.twitter.distributedlog.service.DistributedLogServiceImpl;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Proxy service requests rate limiter which throws an overcap exception and records hard limit
 * stats when the limit is hit.
 */
public class RpsHardLimiter extends RpsRequestLimiter {
    final String name;
    final Counter hardLimitCounter;

    public RpsHardLimiter(int hardLimit, StatsLogger statsLogger, String name) {
        super(hardLimit);
        this.name = name;
        this.hardLimitCounter = statsLogger.scope("limiter").scope("rps").getCounter("hard_limit_hit");
        LOG.debug("Init RpsHardLimiter hardLimit={}", hardLimit);
    }

    @Override
    protected void overlimit() throws OverCapacityException {
        hardLimitCounter.inc();
        throw new OverCapacityException("RPS limit exceeded for stream " + name);
    }
}
