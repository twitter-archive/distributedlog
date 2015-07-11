package com.twitter.distributedlog.logsegment;

import com.twitter.distributedlog.util.Sizable;
import com.twitter.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedRollingPolicy implements RollingPolicy {

    final static Logger LOG = LoggerFactory.getLogger(TimeBasedRollingPolicy.class);

    final long rollingIntervalMs;

    public TimeBasedRollingPolicy(long rollingIntervalMs) {
        this.rollingIntervalMs = rollingIntervalMs;
    }

    @Override
    public boolean shouldRollover(Sizable sizable, long lastRolloverTimeMs) {
        long elapsedMs = Utils.elapsedMSec(lastRolloverTimeMs);
        boolean shouldSwitch = elapsedMs > rollingIntervalMs;
        if (shouldSwitch) {
            LOG.debug("Last Finalize Time: {} elapsed time (MSec): {}", lastRolloverTimeMs,
                      elapsedMs);
        }
        return shouldSwitch;
    }

}
