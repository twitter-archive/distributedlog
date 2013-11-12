package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class RetryableReadException extends IOException {
    private static final long serialVersionUID = 1L;

    public RetryableReadException (String streamName, String message) {
        super(String.format("Reader on {} failed with {}", streamName, message));
    }

    public RetryableReadException (String streamName, String message, Throwable cause) {
        super(String.format("Reader on {} failed with {}", streamName, message), cause);
    }
}
