package com.twitter.distributedlog.subscription;

import com.twitter.distributedlog.DLSN;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

import java.io.Closeable;
import java.util.Map;

/**
 * Store to manage subscriptions
 */
public interface SubscriptionsStore extends Closeable {

    /**
     * Get the last committed position stored for <i>subscriberId</i>.
     *
     * @param subscriberId
     *          subscriber id
     * @return future representing last committed position.
     */
    public Future<DLSN> getLastCommitPosition(String subscriberId);

    /**
     * Get the last committed positions for all subscribers.
     *
     * @return future representing last committed positions for all subscribers.
     */
    public Future<Map<String, DLSN>> getLastCommitPositions();

    /**
     * Advance the last committed position for <i>subscriberId</i>.
     *
     * @param subscriberId
     *          subscriber id.
     * @param newPosition
     *          new committed position.
     * @return future representing advancing result.
     */
    public Future<BoxedUnit> advanceCommitPosition(String subscriberId, DLSN newPosition);

}
