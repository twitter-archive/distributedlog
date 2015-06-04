package com.twitter.distributedlog;

import com.twitter.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TimeBasedRollingPolicy implements RollingPolicy {

    final static Logger LOG = LoggerFactory.getLogger(TimeBasedRollingPolicy.class);

    final long rollingIntervalMs;

    TimeBasedRollingPolicy(DistributedLogConfiguration conf) {
        rollingIntervalMs = (long)conf.getLogSegmentRollingIntervalMinutes() * 60 * 1000;
    }

    @Override
    public boolean shouldRollLog(BKPerStreamLogWriter writer, long lastLedgerRollingTimeMillis) {
        boolean shouldSwitch = Utils.elapsedMSec(lastLedgerRollingTimeMillis) > rollingIntervalMs;
        if (shouldSwitch) {
            LOG.debug("Last Finalize Time: {} elapsed time (MSec): {}", lastLedgerRollingTimeMillis,
                      Utils.elapsedMSec(lastLedgerRollingTimeMillis));
        }
        return shouldSwitch;
    }

}
