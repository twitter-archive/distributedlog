package com.twitter.distributedlog;

import java.io.IOException;

/**
 * Thrown when the distributed log manager has already been closed
 * (connections have been torn down)
 */
public class AlreadyClosedException extends IOException {

    private static final long serialVersionUID = 1L;

    public AlreadyClosedException(String message) {
        super(message);
    }
}
