package com.twitter.distributedlog.selector;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecordWithDLSN;

/**
 * Save the first record with a dlsn not less than the dlsn provided.
 */
public class FirstDLSNNotLessThanSelector implements LogRecordSelector {

    LogRecordWithDLSN result;
    final DLSN dlsn;

    public FirstDLSNNotLessThanSelector(DLSN dlsn) {
        this.dlsn = dlsn;
    }

    @Override
    public void process(LogRecordWithDLSN record) {
        if ((record.getDlsn().compareTo(dlsn) >= 0) && (null == result)) {
            this.result = record;
        }
    }

    @Override
    public LogRecordWithDLSN result() {
        return this.result;
    }
}
