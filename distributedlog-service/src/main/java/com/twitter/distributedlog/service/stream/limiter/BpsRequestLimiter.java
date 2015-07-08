package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.AbstractRequestLimiter;
import com.twitter.distributedlog.service.stream.StreamOp;
import com.twitter.distributedlog.service.stream.WriteOpWithPayload;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Request limiter for bytes per second, defines cost function for bps limiter.
 */
public abstract class BpsRequestLimiter extends AbstractRequestLimiter<StreamOp> {

    public BpsRequestLimiter(int limit) {
        super(limit);
    }

    @Override
    protected int cost(StreamOp op) {
        if (op instanceof WriteOpWithPayload) {
            WriteOpWithPayload writeOp = (WriteOpWithPayload) op;
            return (int) Math.min(writeOp.getPayloadSize(), Integer.MAX_VALUE);
        } else {
            return 0;
        }
    }
}
