package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.AbstractRequestLimiter;
import com.twitter.distributedlog.service.stream.StreamOp;
import com.twitter.distributedlog.service.stream.WriteOpWithPayload;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Request limiter for requests per second, defines cost function for rps limiter.
 */
public abstract class RpsRequestLimiter extends AbstractRequestLimiter<StreamOp> {
    public RpsRequestLimiter(int limit) {
        super(limit);
    }

    @Override
    protected int cost(StreamOp op) {
        if (op instanceof WriteOpWithPayload) {
            return 1;
        } else {
            return 0;
        }
    }
}
