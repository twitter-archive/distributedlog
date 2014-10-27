package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.LockingException;

public class LockCancelledException extends LockingException {

    public LockCancelledException(String lockPath, String message, Throwable cause) {
        super(lockPath, message, cause);
    }

}
