package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class TransactionIdOutOfOrderException extends DLException {

    private static final long serialVersionUID = -6239322552103630036L;
    private final long lastTxnId;

    public TransactionIdOutOfOrderException(long smallerTxnId, long lastTxnId) {
        super(StatusCode.TRANSACTION_OUT_OF_ORDER,
              "Received smaller txn id " + smallerTxnId + ", last txn id is " + lastTxnId);
        this.lastTxnId = lastTxnId;
    }

    public long getLastTxnId() {
        return lastTxnId;
    }
}
