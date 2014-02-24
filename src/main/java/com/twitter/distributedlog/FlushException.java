package com.twitter.distributedlog;

import java.io.IOException;

public class FlushException extends IOException {

    private final long lastTxIdWritten;
    private final long lastTxIdAcknowledged;

    private static final long serialVersionUID = 1L;

    public FlushException(String message, long lastTxIdWritten, long lastTxIdAcknowledged) {
        super(message);
        this.lastTxIdWritten = lastTxIdWritten;
        this.lastTxIdAcknowledged = lastTxIdAcknowledged;
    }

    public FlushException(String message, long lastTxIdWritten, long lastTxIdAcknowledged, Throwable cause) {
        super(message, cause);
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
