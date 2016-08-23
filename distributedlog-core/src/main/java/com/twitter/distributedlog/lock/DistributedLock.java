package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.exceptions.LockingException;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.util.Future;

/**
 * Interface for distributed locking
 */
public interface DistributedLock extends AsyncCloseable {

    /**
     * Asynchronously acquire the lock.
     *
     * @return future represents the acquire result.
     */
    Future<? extends DistributedLock> asyncAcquire();

    /**
     * Check if hold lock. If it doesn't, then re-acquire the lock.
     *
     * @throws LockingException if the lock attempt fails
     * @see #checkOwnership()
     */
    void checkOwnershipAndReacquire() throws LockingException;

    /**
     * Check if the lock is held. If not, error out and do not re-acquire.
     * Use this in cases where there are many waiters by default and re-acquire
     * is unlikely to succeed.
     *
     * @throws LockingException if we lost the ownership
     * @see #checkOwnershipAndReacquire()
     */
    void checkOwnership() throws LockingException;

}
