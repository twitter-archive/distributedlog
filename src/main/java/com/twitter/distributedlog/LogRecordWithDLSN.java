package com.twitter.distributedlog;

public class LogRecordWithDLSN extends LogRecord {
    private DLSN dlsn;

    /**
     * This empty constructor can only be called from Reader#readOp.
     */
    LogRecordWithDLSN() {
        super();
        this.dlsn = DLSN.InvalidDLSN;
    }

    public void setDlsn(DLSN dlsn) {
        this.dlsn = dlsn;
    }

    public DLSN getDlsn() {
        return dlsn;
    }
}
