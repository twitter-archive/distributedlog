package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.util.PermitLimiter;

class WriteLimiter {

    String streamName;
    boolean streamDarkmode;
    boolean globalDarkmode;
    final PermitLimiter streamLimiter;
    final PermitLimiter globalLimiter;

    public WriteLimiter(String streamName, boolean streamDarkmode, boolean globalDarkmode, 
        PermitLimiter streamLimiter, PermitLimiter globalLimiter) {

        this.streamName = streamName;
        this.streamDarkmode = streamDarkmode;
        this.globalDarkmode = globalDarkmode;
        this.streamLimiter = streamLimiter;
        this.globalLimiter = globalLimiter;
    }

    void acquire() throws OverCapacityException {
        if (!streamLimiter.acquire() && !streamDarkmode) {
            throw new OverCapacityException(String.format("Stream write capacity exceeded for stream %s", streamName));
        }
        try {
            if (!globalLimiter.acquire() && !globalDarkmode) {
                throw new OverCapacityException("Global write capacity exceeded");
            }
        } catch (OverCapacityException ex) {
            streamLimiter.release();
            throw ex;
        }
    }

    void release() {
        streamLimiter.release();
        globalLimiter.release();
    }
}