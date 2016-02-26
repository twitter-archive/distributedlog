package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.lock.ZKDistributedLock.State;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Exception indicates that the lock was closed (unlocked) before the lock request could complete.
 */
public class LockClosedException extends LockingException {

    private static final long serialVersionUID = 8775257025963470331L;

    public LockClosedException(String lockPath, Pair<String, Long> lockId, State currentState) {
        super(lockPath, "lock at path " + lockPath + " with id " + lockId + " closed early in state : " + currentState);
    }
}
