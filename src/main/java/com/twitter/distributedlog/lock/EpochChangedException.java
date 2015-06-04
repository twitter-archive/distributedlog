package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.LockingException;

/**
 * Exception indicates that epoch already changed when executing a given
 * {@link LockAction}.
 */
public class EpochChangedException extends LockingException {

    private static final long serialVersionUID = 8775257025963870331L;

    public EpochChangedException(String lockPath, int expectedEpoch, int currentEpoch) {
        super(lockPath, "lock " + lockPath + " already moved to epoch " + currentEpoch + ", expected " + expectedEpoch);
    }
}
