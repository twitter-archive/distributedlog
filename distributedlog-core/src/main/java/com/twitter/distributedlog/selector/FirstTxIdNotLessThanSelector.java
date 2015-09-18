package com.twitter.distributedlog.selector;

import com.twitter.distributedlog.LogRecordWithDLSN;

/**
 * Save the first record with transaction id not less than the provided transaction id.
 */
public class FirstTxIdNotLessThanSelector implements LogRecordSelector {

    LogRecordWithDLSN result;
    final long txId;

    public FirstTxIdNotLessThanSelector(long txId) {
        this.txId = txId;
    }

    @Override
    public void process(LogRecordWithDLSN record) {
        if ((record.getTransactionId() >= txId) && (null == result)) {
            this.result = record;
        }
    }

    @Override
    public LogRecordWithDLSN result() {
        return this.result;
    }
}
