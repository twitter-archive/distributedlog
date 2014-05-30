package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class FlushException extends DLException {

    private final long lastTxIdWritten;
    private final long lastTxIdAcknowledged;

    private static final long serialVersionUID = -9060360360261130489L;

    public FlushException(String message, long lastTxIdWritten, long lastTxIdAcknowledged) {
        super(StatusCode.FLUSH_TIMEOUT, message);
        this.lastTxIdWritten = lastTxIdWritten;
        this.lastTxIdAcknowledged = lastTxIdAcknowledged;
    }

    public FlushException(String message, long lastTxIdWritten, long lastTxIdAcknowledged, Throwable cause) {
        super(StatusCode.FLUSH_TIMEOUT, message, cause);
        this.lastTxIdWritten = lastTxIdWritten;
        this.lastTxIdAcknowledged = lastTxIdAcknowledged;
    }

    public long getLastTxIdWritten() {
        return lastTxIdWritten;
    }

    public long getLastTxIdAcknowledged() {
        return lastTxIdAcknowledged;
    }
}
