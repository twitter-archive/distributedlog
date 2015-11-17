package com.twitter.distributedlog.v2.selector;

import com.twitter.distributedlog.v2.LogRecordWithDLSN;

/**
 * Visitor interface to process a set of records, and return some result.
 */
public interface LogRecordSelector {
    /**
     * Process a given <code>record</code>.
     *
     * @param record
     *          log record to process
     */
    void process(LogRecordWithDLSN record);

    /**
     * Returned the selected log record after processing a set of records.
     *
     * @return the selected log record.
     */
    LogRecordWithDLSN result();
}
