package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

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
     * Whether the lock is held or not?
     *
     * @return true if the lock is held, otherwise false.
     */
    boolean isLockHeld();

    /**
     * Whether the lock is expired or not?
     * <p>If a lock is expired, it will not be reusable any more. Because it is an one-time lock.
     *
     * @return true if the lock is expired, otherwise false.
     */
    boolean isLockExpired();

    /**
     * Acquire the lock if it is free within given waiting time.
     * <p>
     * Calling this method will attempt to acquire the lock. If the lock
     * is already acquired by others, the caller will wait for <i>timeout</i>
     * period. If the caller could claim the lock within <i>timeout</i> period,
     * the caller acquire the lock. Otherwise, it would fail with {@link OwnershipAcquireFailedException}.
     * <p>
     * {@link #unlock()} should be called to unlock a claimed lock. The caller
     * doesn't need to unlock to clean up resources if <i>tryLock</i> fails.
     * <p>
     * <i>tryLock</i> here is effectively the combination of following asynchronous calls.
     * <pre>
     *     DistributedLock lock = ...;
     *     Future<LockWaiter> attemptFuture = lock.asyncTryLock(...);
     *
     *     boolean acquired = waiter.waitForAcquireQuietly();
     *     if (acquired) {
     *         // ...
     *     } else {
     *         // ...
     *     }
     * </pre>
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
     * Acquire the lock in asynchronous way.
     * <p>
     * Calling this method will attempt to place a lock waiter to acquire this lock.
     * The future returned by this method represents the result of this attempt. It doesn't mean
     * the caller acquired the lock or not. The application should check {@link LockWaiter#getAcquireFuture()}
     * to see if it acquired the lock or not.
     *
     * @param timeout
     *          timeout period to wait for claiming ownership
     * @param unit
     *          unit of timeout period
     * @return lock waiter representing this attempt of acquiring lock.
     * @see #tryLock(long, TimeUnit)
     */
    Future<LockWaiter> asyncTryLock(long timeout, TimeUnit unit);

    /**
     * Release a claimed lock.
     *
     * @see #tryLock(long, TimeUnit)
     */
    void unlock();

    /**
     * Release a claimed lock in the asynchronous way.
     *
     * @return future representing the result of unlock operation.
     * @see #unlock()
     */
    Future<BoxedUnit> asyncUnlock();

}
