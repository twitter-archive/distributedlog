package com.twitter.distributedlog;

/**
 * Visitor interface to process a set of records, and return some result.
 */
interface LogRecordSelector {
    void process(LogRecordWithDLSN record);
    LogRecordWithDLSN result();
}

/**
 * Save the last record processed.
 */
class LastRecordSelector implements LogRecordSelector {

    LogRecordWithDLSN lastRecord;

    public void process(LogRecordWithDLSN record) {
        lastRecord = record;
    }

    public LogRecordWithDLSN result() {
        return lastRecord;
    }
}

/**
 * Save the first record with a dlsn larger than the dlsn provided.
 */
class FirstDLSNGreaterThanSelector implements LogRecordSelector {

    LogRecordWithDLSN result;
    final DLSN dlsn;

    public FirstDLSNGreaterThanSelector(DLSN dlsn) {
        this.dlsn = dlsn;
    }

    public void process(LogRecordWithDLSN record) {
        if ((record.getDlsn().compareTo(dlsn) >= 0) && (null == result)) {
            this.result = record;
        }
    }

    public LogRecordWithDLSN result() {
        return this.result;
    }
}
