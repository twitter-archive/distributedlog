package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.lock.ZKSessionLock.State;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Exception indicates that the lock's zookeeper session was expired before the lock request could complete.
 */
public class LockSessionExpiredException extends LockingException {

    private static final long serialVersionUID = 8775253025963470331L;

    public LockSessionExpiredException(String lockPath, Pair<String, Long> lockId, State currentState) {
        super(lockPath, "lock at path " + lockPath + " with id " + lockId + " expired early in state : " + currentState);
    }
}
