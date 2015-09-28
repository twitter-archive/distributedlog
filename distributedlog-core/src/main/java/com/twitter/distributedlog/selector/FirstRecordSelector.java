package com.twitter.distributedlog.selector;

import com.twitter.distributedlog.LogRecordWithDLSN;

/**
 * Save the first record processed
 */
public class FirstRecordSelector implements LogRecordSelector {

    final boolean includeControl;
    LogRecordWithDLSN firstRecord;

    public FirstRecordSelector(boolean includeControl) {
        this.includeControl = includeControl;
    }

    @Override
    public void process(LogRecordWithDLSN record) {
        if (null == this.firstRecord
                && (includeControl || !record.isControl())) {
            this.firstRecord = record;
        }
    }

    @Override
    public LogRecordWithDLSN result() {
        return this.firstRecord;
    }
}