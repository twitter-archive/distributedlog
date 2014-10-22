package com.twitter.distributedlog.util;

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
        public void release() {
        }
    };

    boolean acquire();
    void release();
}