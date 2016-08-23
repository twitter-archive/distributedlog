package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.exceptions.LockingException;
import com.twitter.util.Future;

/**
 * An implementation of {@link DistributedLock} which does nothing.
 */
public class NopDistributedLock implements DistributedLock {

    public static final DistributedLock INSTANCE = new NopDistributedLock();

    private NopDistributedLock() {}

    @Override
    public Future<? extends DistributedLock> asyncAcquire() {
        return Future.value(this);
    }

    @Override
    public void checkOwnershipAndReacquire() throws LockingException {
        // no-op
    }

    @Override
    public void checkOwnership() throws LockingException {
        // no-op
    }

    @Override
    public Future<Void> asyncClose() {
        return Future.Void();
    }
}
