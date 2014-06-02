package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class RetryableReadException extends DLException {

    private static final long serialVersionUID = 2803207702150642330L;

    public RetryableReadException (String streamName, String message) {
        super(StatusCode.RETRYABLE_READ, String.format("Reader on %s failed with %s", streamName, message));
    }

    public RetryableReadException (String streamName, String message, Throwable cause) {
        super(StatusCode.RETRYABLE_READ, String.format("Reader on %s failed with %s", streamName, message), cause);
    }
}
