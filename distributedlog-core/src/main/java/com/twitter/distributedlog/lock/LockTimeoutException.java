package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.LockingException;

import java.util.concurrent.TimeUnit;

/**
 * Exception thrown when acquiring lock timeout
 */
public class LockTimeoutException extends LockingException {

    private static final long serialVersionUID = -3837638877423323820L;

    LockTimeoutException(String lockPath, long timeout, TimeUnit unit) {
        super(lockPath, "Locking " + lockPath + " timeout in " + timeout + " " + unit);
    }
}
