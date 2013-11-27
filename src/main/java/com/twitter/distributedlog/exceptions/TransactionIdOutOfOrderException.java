package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class TransactionIdOutOfOrderException extends IOException {

    private final long lastTxnId;

    public TransactionIdOutOfOrderException(long smallerTxnId, long lastTxnId) {
        super("Received smaller txn id " + smallerTxnId + ", last txn id is " + lastTxnId);
        this.lastTxnId = lastTxnId;
    }

    public long getLastTxnId() {
        return lastTxnId;
    }
}
