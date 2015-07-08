package com.twitter.distributedlog.limiter;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.GuavaRateLimiter;
import com.twitter.distributedlog.limiter.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collect rate limiter implementation, cost(Request), overlimit, etc.
 * behavior.
 */
public abstract class AbstractRequestLimiter<Request> implements RequestLimiter<Request> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractRequestLimiter.class);

    private volatile RateLimiter limiter;
    private final int limit;

    public AbstractRequestLimiter(int limit) {
        if (limit == 0) {
            this.limiter = RateLimiter.REJECT;
        } else if (limit < 0) {
            this.limiter = RateLimiter.ACCEPT;
        } else {
            this.limiter = new GuavaRateLimiter(limit);
        }
        this.limit = limit;
    }

    @Override
    public void apply(Request request) throws OverCapacityException {
        int permits = cost(request);
        if (!limiter.acquire(permits)) {
            overlimit();
        }
    }

    @VisibleForTesting
    public void setRateLimiter(RateLimiter.Builder builder) {
        this.limiter = builder.setLimit(limit).build();
    }

    /**
     * Cost function for the the request. Will be charged against the rate
     * limit.
     *
     * @param rate the new rate.
     */
    protected abstract int cost(Request request);

    /**
     * Hard limit action, just a way to define exception text.
     */
    protected abstract void overlimit() throws OverCapacityException;
}
