package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.LockingException;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Exception thrown when lock state changed
 */
public class LockStateChangedException extends LockingException {

    private static final long serialVersionUID = -3770866789942102262L;

    LockStateChangedException(String lockPath, Pair<String, Long> lockId,
                              DistributedLock.State expectedState, DistributedLock.State currentState) {
        super(lockPath, "Lock state of " + lockId + " for " + lockPath + " has changed : expected "
                + expectedState + ", but " + currentState);
    }
}
