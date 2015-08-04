package com.twitter.distributedlog.util;

/**
 * Time based sequencer. It generated transaction id using milliseconds.
 */
public class TimeSequencer implements Sequencer {
    @Override
    public long nextId() {
        return System.currentTimeMillis();
    }
}
