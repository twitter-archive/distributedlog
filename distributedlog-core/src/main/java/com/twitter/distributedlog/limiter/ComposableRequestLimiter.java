package com.twitter.distributedlog.limiter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.GuavaRateLimiter;
import com.twitter.distributedlog.limiter.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collect rate limiter implementation, cost(Request), overlimit, etc. behavior.
 */
public class ComposableRequestLimiter<Request> implements RequestLimiter<Request> {
    protected static final Logger LOG = LoggerFactory.getLogger(ComposableRequestLimiter.class);

    private final RateLimiter limiter;
    private final OverlimitFunction<Request> overlimitFunction;
    private final CostFunction<Request> costFunction;

    static public interface OverlimitFunction<Request> {
        void apply(Request request) throws OverCapacityException;
    }
    static public interface CostFunction<Request> {
        int apply(Request request);
    }

    public ComposableRequestLimiter(
            RateLimiter limiter,
            OverlimitFunction<Request> overlimitFunction,
            CostFunction<Request> costFunction) {
        Preconditions.checkNotNull(limiter);
        Preconditions.checkNotNull(overlimitFunction);
        Preconditions.checkNotNull(costFunction);
        this.limiter = limiter;
        this.overlimitFunction = overlimitFunction;
        this.costFunction = costFunction;
    }

    @Override
    public void apply(Request request) throws OverCapacityException {
        int permits = costFunction.apply(request);
        if (!limiter.acquire(permits)) {
            overlimitFunction.apply(request);
        }
    }
}
