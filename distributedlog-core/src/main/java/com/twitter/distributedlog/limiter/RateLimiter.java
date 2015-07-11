package com.twitter.distributedlog.limiter;

/**
 * Simple interface for a rate limiter used by RequestLimiter.
 */
public interface RateLimiter {

    public static final RateLimiter REJECT = new RateLimiter() {
        @Override
        public boolean acquire(int permits) {
            return false;
        }
    };

    public static final RateLimiter ACCEPT = new RateLimiter() {
        @Override
        public boolean acquire(int permits) {
            return true;
        }
    };

    public static abstract class Builder {
        protected int limit;
        public Builder setLimit(int limit) {
            this.limit = limit;
            return this;
        }
        public abstract RateLimiter build();
    }

    /**
     * Try to acquire a certain number of permits.
     *
     * @param permits number of permits to acquire
     */
    boolean acquire(int permits);
}
