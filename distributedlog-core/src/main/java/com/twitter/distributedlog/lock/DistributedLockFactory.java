package com.twitter.distributedlog.lock;

import com.twitter.util.Future;

/**
 * Factory to create {@link DistributedLock}
 */
public interface DistributedLockFactory {

    /**
     * Create a lock with lock path.
     *
     * @param lockPath
     *          lock path
     * @param context
     *          lock context
     * @return future represents the creation result.
     */
    Future<DistributedLock> createLock(String lockPath, DistributedLockContext context);

}
