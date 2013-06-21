package com.twitter.distributedlog;

import java.io.IOException;

/**
 * Thrown when the transaction Id specified in the API is in the range that has already been
 * truncated
 */
public class AlreadyClosedException extends IOException {

    private static final long serialVersionUID = 1L;

    public AlreadyClosedException(String message) {
        super(message);
    }
}
