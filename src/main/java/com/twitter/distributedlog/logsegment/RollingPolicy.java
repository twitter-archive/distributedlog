package com.twitter.distributedlog.logsegment;

import com.twitter.distributedlog.util.Sizable;

public interface RollingPolicy {
    /**
     * Determines if a rollover may be appropriate at this time.
     *
     * @param sizable
     *          Any object that is sizable.
     * @param lastRolloverTimeMs
     *          last rolling time in millis.
     * @return true if a rollover is required. otherwise, false.
     */
    boolean shouldRollover(Sizable sizable, long lastRolloverTimeMs);
}
