package com.twitter.distributedlog.service.stream.limiter;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.ComposableRequestLimiter;
import com.twitter.distributedlog.limiter.ComposableRequestLimiter.OverlimitFunction;
import com.twitter.distributedlog.limiter.ComposableRequestLimiter.CostFunction;
import com.twitter.distributedlog.limiter.GuavaRateLimiter;
import com.twitter.distributedlog.limiter.RateLimiter;
import com.twitter.distributedlog.limiter.RequestLimiter;
import com.twitter.distributedlog.service.stream.StreamOp;
import com.twitter.distributedlog.service.stream.WriteOpWithPayload;

public class RequestLimiterBuilder {
    private OverlimitFunction<StreamOp> overlimitFunction = NOP_OVERLIMIT_FUNCTION;
    private RateLimiter limiter;
    private CostFunction<StreamOp> costFunction;

    public static final CostFunction<StreamOp> RPS_COST_FUNCTION = new CostFunction<StreamOp>() {
        @Override
        public int apply(StreamOp op) {
            if (op instanceof WriteOpWithPayload) {
                return 1;
            } else {
                return 0;
            }
        }
    };

    public static final CostFunction<StreamOp> BPS_COST_FUNCTION = new CostFunction<StreamOp>() {
        @Override
        public int apply(StreamOp op) {
            if (op instanceof WriteOpWithPayload) {
                WriteOpWithPayload writeOp = (WriteOpWithPayload) op;
                return (int) Math.min(writeOp.getPayloadSize(), Integer.MAX_VALUE);
            } else {
                return 0;
            }
        }
    };

    public static final OverlimitFunction<StreamOp> NOP_OVERLIMIT_FUNCTION = new OverlimitFunction<StreamOp>() {
        @Override
        public void apply(StreamOp op) throws OverCapacityException {
            return;
        }
    };

    public RequestLimiterBuilder limit(int limit) {
        this.limiter = GuavaRateLimiter.of(limit);
        return this;
    }

    public RequestLimiterBuilder overlimit(OverlimitFunction<StreamOp> overlimitFunction) {
        this.overlimitFunction = overlimitFunction;
        return this;
    }

    public RequestLimiterBuilder cost(CostFunction<StreamOp> costFunction) {
        this.costFunction = costFunction;
        return this;
    }

    public static RequestLimiterBuilder newRpsLimiterBuilder() {
        return new RequestLimiterBuilder().cost(RPS_COST_FUNCTION);
    }

    public static RequestLimiterBuilder newBpsLimiterBuilder() {
        return new RequestLimiterBuilder().cost(BPS_COST_FUNCTION);
    }

    public RequestLimiter<StreamOp> build() {
        Preconditions.checkNotNull(limiter);
        Preconditions.checkNotNull(overlimitFunction);
        Preconditions.checkNotNull(costFunction);
        return new ComposableRequestLimiter(limiter, overlimitFunction, costFunction);
    }
}
