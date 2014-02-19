package com.twitter.distributedlog;

public class LogRecordWithDLSN extends LogRecord {
    private final DLSN dlsn;

    /**
     * This empty constructor can only be called from Reader#readOp.
     */
    LogRecordWithDLSN(DLSN dlsn) {
        super();
        this.dlsn = dlsn;
    }

    public DLSN getDlsn() {
        return dlsn;
    }

    @Override
    public String toString() {
        return "LogRecordWithDLSN{" +
            "dlsn=" + dlsn +
            ", txid=" + getTransactionId() +
            ", isControl=" + isControl() +
            ", isEndOfStream=" + isEndOfStream() +
            '}';
    }
}
