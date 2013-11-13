package com.twitter.distributedlog;

import java.io.IOException;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class LockingException extends DLException {

    private static final long serialVersionUID = 1L;

    public LockingException(String lockPath, String message) {
        super(StatusCode.LOCKING_EXCEPTION, String.format("LockPath - %s: %s", lockPath, message));
    }

    public LockingException(String lockPath, String message, Throwable cause) {
        super(StatusCode.LOCKING_EXCEPTION, String.format("LockPath - %s: %s", lockPath, message), cause);
    }
}
