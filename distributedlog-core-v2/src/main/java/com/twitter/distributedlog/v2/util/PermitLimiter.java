package com.twitter.distributedlog.v2.util;

/**
 * A simple limiter interface which tracks acquire/release of permits, for
 * example for tracking outstanding writes.
 */
public interface PermitLimiter {

    public static PermitLimiter NULL_PERMIT_LIMITER = new PermitLimiter() {
        @Override
        public boolean acquire() {
            return true;
        }
        @Override
        public void release(int permits) {
        }
    };

    /**
     * Acquire a permit.
     *
     * @return true if successfully acquire a permit, otherwise false.
     */
    boolean acquire();

    /**
     * Release a permit.
     */
    void release(int permits);
}
