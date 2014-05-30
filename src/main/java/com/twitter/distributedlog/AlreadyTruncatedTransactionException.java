package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

/**
 * Thrown when the transaction Id specified in the API is in the range that has already been
 * truncated
 */
public class AlreadyTruncatedTransactionException extends DLException {

    private static final long serialVersionUID = 4287238797065959977L;

    public AlreadyTruncatedTransactionException(String message) {
        super(StatusCode.TRUNCATED_TRANSACTION, message);
    }
}
