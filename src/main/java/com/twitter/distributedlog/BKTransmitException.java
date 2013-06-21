package com.twitter.distributedlog;

import java.io.IOException;

public class BKTransmitException extends IOException {

    private static final long serialVersionUID = 1L;

    public BKTransmitException(String message) {
        super(message);
    }
}
