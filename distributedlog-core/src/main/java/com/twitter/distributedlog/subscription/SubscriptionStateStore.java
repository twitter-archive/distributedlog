package com.twitter.distributedlog.subscription;

import java.io.Closeable;

import scala.runtime.BoxedUnit;

import com.twitter.distributedlog.DLSN;
import com.twitter.util.Future;

public interface SubscriptionStateStore extends Closeable {
    /**
     * Get the last committed position stored for this subscription
     *
     * @return
     */
    public Future<DLSN> getLastCommitPosition();

    /**
     * Advances the position associated with the subscriber
     *
     * @param newPosition - new commit position
     * @return
     */
    public Future<BoxedUnit> advanceCommitPosition(DLSN newPosition);
}
