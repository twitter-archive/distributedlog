package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;

import java.util.concurrent.TimeUnit;

/**
 * One time lock.
 * <p>The lock is only alive during a given period. It should
 * be not usable if the lock is expired.
 * <p>Listener could be registered by {@link #setLockListener(LockListener)}
 * to receive state changes of the lock.
 */
public interface DistributedLock {

    /**
     * Set lock listener for lock state changes.
     * <p>Typically a listener should be set before try locking.
     *
     * @param lockListener
     *          lock listener for state changes.
     */
    DistributedLock setLockListener(LockListener lockListener);

    /**
     * Try locking for a given time period.
     * <p>
     * Calling this method will attempt to acquire the lock. If the lock
     * is already acquired by others, the caller will wait for <i>timeout</i>
     * period. If the caller could claim the lock within <i>timeout</i> period,
     * the caller acquire the lock. Otherwise, it would fail with {@link OwnershipAcquireFailedException}.
     * <p>
     * {@link #unlock()} should be called to unlock a claimed lock. The caller
     * doesn't need to unlock to clean up resources if <i>tryLock</i> fails.
     *
     * @param timeout
     *          timeout period to wait for claiming ownership
     * @param unit
     *          unit of timeout period
     * @throws OwnershipAcquireFailedException if the lock is already acquired by others
     * @throws LockingException when encountered other lock related issues.
     */
    void tryLock(long timeout, TimeUnit unit)
            throws OwnershipAcquireFailedException, LockingException;

    /**
     * Unlock a claimed lock.
     *
     * @see #tryLock(long, TimeUnit)
     */
    void unlock();

}
