package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.v2.exceptions.OverCapacityException;
import com.twitter.distributedlog.v2.util.PermitLimiter;

class WriteLimiter {

    String streamName;
    final PermitLimiter streamLimiter;
    final PermitLimiter globalLimiter;

    public WriteLimiter(String streamName, PermitLimiter streamLimiter, PermitLimiter globalLimiter) {
        this.streamName = streamName;
        this.streamLimiter = streamLimiter;
        this.globalLimiter = globalLimiter;
    }

    void acquire() throws OverCapacityException {
        if (!streamLimiter.acquire()) {
            throw new OverCapacityException(String.format("Stream write capacity exceeded for stream %s", streamName));
        }
        try {
            if (!globalLimiter.acquire()) {
                throw new OverCapacityException("Global write capacity exceeded");
            }
        } catch (OverCapacityException ex) {
            streamLimiter.release(1);
            throw ex;
        }
    }

    void release(int permits) {
        streamLimiter.release(permits);
        globalLimiter.release(permits);
    }

    void release() {
        release(1);
    }
}
