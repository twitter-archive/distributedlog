package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class TransactionIdOutOfOrderException extends DLException {

    private static final long serialVersionUID = -6239322552103630036L;
    // TODO: copied from DistributedLogConstants (we should think about how to separated common constants)
    public static final long INVALID_TXID = -999;
    private final long lastTxnId;

    public TransactionIdOutOfOrderException(long smallerTxnId, long lastTxnId) {
        super(StatusCode.TRANSACTION_OUT_OF_ORDER,
              "Received smaller txn id " + smallerTxnId + ", last txn id is " + lastTxnId);
        this.lastTxnId = lastTxnId;
    }

    public TransactionIdOutOfOrderException(long invalidTxnId) {
        super(StatusCode.TRANSACTION_OUT_OF_ORDER,
            "The txn id " + invalidTxnId + " is invalid and will break the sequence");
        lastTxnId = INVALID_TXID;
    }

    public long getLastTxnId() {
        return lastTxnId;
    }
}
