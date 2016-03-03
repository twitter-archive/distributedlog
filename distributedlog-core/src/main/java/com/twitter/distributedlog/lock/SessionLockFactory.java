package com.twitter.distributedlog.lock;

import com.twitter.util.Future;

/**
 * Factory to create {@link SessionLock}
 */
public interface SessionLockFactory {

    /**
     * Create a lock with lock path.
     *
     * @param lockPath
     *          lock path
     * @param context
     *          lock context
     * @return future represents the creation result.
     */
    Future<SessionLock> createLock(String lockPath, DistributedLockContext context);

}
