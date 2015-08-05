package com.twitter.distributedlog.util;

import com.twitter.distributedlog.DistributedLogConstants;

/**
 * Time based sequencer. It generated non-decreasing transaction id using milliseconds.
 * It isn't thread-safe. The caller takes the responsibility on synchronization.
 */
public class TimeSequencer implements Sequencer {

    private long lastId = DistributedLogConstants.INVALID_TXID;

    public void setLastId(long lastId) {
        this.lastId = lastId;
    }

    @Override
    public long nextId() {
        lastId = Math.max(lastId, System.currentTimeMillis());
        return lastId;
    }
}
