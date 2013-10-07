package com.twitter.distributedlog;

import java.io.IOException;

public class FlushException extends IOException {

    private static final long serialVersionUID = 1L;

    public FlushException(String message) {
        super(message);
    }

    public FlushException(String message, Throwable cause) {
        super(message, cause);
    }
}
