package com.twitter.distributedlog;

import java.io.IOException;

public class LockingException extends IOException {

    private static final long serialVersionUID = 1L;

    public LockingException(String lockPath, String message) {
        super(String.format("LockPath - %s: %s", lockPath, message));
    }

    public LockingException(String lockPath, String message, Throwable cause) {
        super(String.format("LockPath - %s: %s", lockPath, message), cause);
    }
}
