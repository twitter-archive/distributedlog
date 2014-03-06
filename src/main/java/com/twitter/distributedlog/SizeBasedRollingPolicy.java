package com.twitter.distributedlog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SizeBasedRollingPolicy implements RollingPolicy {

    final static Logger LOG = LoggerFactory.getLogger(TimeBasedRollingPolicy.class);

    final long maxLogSegmentBytes;

    SizeBasedRollingPolicy(DistributedLogConfiguration conf) {
        if (conf.getMaxLogSegmentBytes() <= 0) {
            maxLogSegmentBytes = DistributedLogConfiguration.BKDL_MAX_LOGSEGMENT_BYTES_DEFAULT;
        } else {
            maxLogSegmentBytes = conf.getMaxLogSegmentBytes();
        }
    }

    @Override
    public boolean shouldRollLog(BKPerStreamLogWriter writer, long lastLedgerRollingTimeMillis) {
        return writer.lh.getLength() > maxLogSegmentBytes;
    }
}
