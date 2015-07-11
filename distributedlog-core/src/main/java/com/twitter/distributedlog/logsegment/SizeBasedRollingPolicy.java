package com.twitter.distributedlog.logsegment;

import com.twitter.distributedlog.util.Sizable;

public class SizeBasedRollingPolicy implements RollingPolicy {

    final long maxSize;

    public SizeBasedRollingPolicy(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public boolean shouldRollover(Sizable sizable, long lastRolloverTimeMs) {
        return sizable.size() > maxSize;
    }
}
