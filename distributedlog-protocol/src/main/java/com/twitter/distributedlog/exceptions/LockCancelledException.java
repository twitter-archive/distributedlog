package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.LockingException;

public class LockCancelledException extends LockingException {

    private static final long serialVersionUID = -148795017092861106L;

    public LockCancelledException(String lockPath, String message, Throwable cause) {
        super(lockPath, message, cause);
    }

}
