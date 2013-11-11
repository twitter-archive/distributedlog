package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class LockingException extends DLException {

    private static final long serialVersionUID = 1L;

    public LockingException(String lockPath, String message) {
        this(StatusCode.LOCKING_EXCEPTION, lockPath, message);
    }

    public LockingException(String lockPath, String message, Throwable cause) {
        this(StatusCode.LOCKING_EXCEPTION, lockPath, message, cause);
    }

    protected LockingException(StatusCode code, String lockPath, String message) {
        super(code, String.format("LockPath - %s: %s", lockPath, message));
    }

    protected LockingException(StatusCode code, String lockPath, String message, Throwable cause) {
        super(code, String.format("LockPath - %s: %s", lockPath, message), cause);
    }
}
