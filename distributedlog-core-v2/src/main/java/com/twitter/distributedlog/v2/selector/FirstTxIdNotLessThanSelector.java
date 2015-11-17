package com.twitter.distributedlog.v2.selector;

import com.twitter.distributedlog.v2.LogRecordWithDLSN;

/**
 * Save the first record with transaction id not less than the provided transaction id.
 * If all records' transaction id is less than provided transaction id, save the last record.
 */
public class FirstTxIdNotLessThanSelector implements LogRecordSelector {

    LogRecordWithDLSN result;
    final long txId;
    boolean found = false;

    public FirstTxIdNotLessThanSelector(long txId) {
        this.txId = txId;
    }

    @Override
    public void process(LogRecordWithDLSN record) {
        if (found) {
            return;
        }
        this.result = record;
        if (record.getTransactionId() >= txId) {
            found = true;
        }
    }

    @Override
    public LogRecordWithDLSN result() {
        return this.result;
    }
}
