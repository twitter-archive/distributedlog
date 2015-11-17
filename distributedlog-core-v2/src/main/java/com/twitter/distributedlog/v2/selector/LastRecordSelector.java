package com.twitter.distributedlog.v2.selector;

import com.twitter.distributedlog.v2.LogRecordWithDLSN;

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
