package com.twitter.distributedlog;

import java.io.IOException;

public class LockingException extends IOException {

    private static final long serialVersionUID = 1L;

    public LockingException(String message) {
        super(message);
    }

    public LockingException(String message, Throwable cause) {
        super(message, cause);
    }
}
