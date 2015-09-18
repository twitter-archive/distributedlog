package com.twitter.distributedlog.selector;

import com.twitter.distributedlog.LogRecordWithDLSN;

/**
 * Save the last record processed.
 */
public class LastRecordSelector implements LogRecordSelector {

    LogRecordWithDLSN lastRecord;

    @Override
    public void process(LogRecordWithDLSN record) {
        lastRecord = record;
    }

    @Override
    public LogRecordWithDLSN result() {
        return lastRecord;
    }
}
