package com.twitter.distributedlog;

import java.io.IOException;

/**
 * Thrown when the send to bookkeeper fails
 * This is thrown by the next attempt to write, send or flush
 */
public class BKTransmitException extends IOException {

    private static final long serialVersionUID = 1L;

    public BKTransmitException(String message) {
        super(message);
    }
}
