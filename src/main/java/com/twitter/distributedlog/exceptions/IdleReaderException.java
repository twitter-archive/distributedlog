package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class IdleReaderException extends IOException {
    private static final long serialVersionUID = 1L;

    public IdleReaderException(String message) {
        super(message);
    }
}
