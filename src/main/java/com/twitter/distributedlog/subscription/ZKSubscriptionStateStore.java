package com.twitter.distributedlog.subscription;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import com.google.common.base.Charsets;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.Utils;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.util.Future;
import com.twitter.util.Promise;

public class ZKSubscriptionStateStore implements SubscriptionStateStore {

    static final Logger logger = LoggerFactory.getLogger(ZKSubscriptionStateStore.class);

    private final ZooKeeperClient zooKeeperClient;
    private final String zkPath;
    private AtomicReference<DLSN> lastCommittedPosition = new AtomicReference<DLSN>(null);

    public ZKSubscriptionStateStore(ZooKeeperClient zooKeeperClient, String zkPath) {
        this.zooKeeperClient = zooKeeperClient;
        this.zkPath = zkPath;
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * Get the last committed position stored for this subscription
     */
    @Override
    public Future<DLSN> getLastCommitPosition() {
        if (null != lastCommittedPosition.get()) {
            return Future.value(lastCommittedPosition.get());
        } else {
            return getLastCommitPositionFromZK();
        }
    }

    Future<DLSN> getLastCommitPositionFromZK() {
        final Promise<DLSN> result = new Promise<DLSN>();
        try {
            logger.debug("Reading last commit position from path {}", zkPath);
            zooKeeperClient.get().getData(zkPath, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    logger.debug("Read last commit position from path {}: rc = {}", zkPath, rc);
                    if (KeeperException.Code.NONODE.intValue() == rc) {
                        result.setValue(DLSN.NonInclusiveLowerBound);
                    } else if (KeeperException.Code.OK.intValue() != rc) {
                        result.setException(KeeperException.create(KeeperException.Code.get(rc), path));
                    } else {
                        try {
                            DLSN dlsn = DLSN.deserialize(new String(data, Charsets.UTF_8));
                            result.setValue(dlsn);
                        } catch (Exception t) {
                            logger.warn("Invalid last commit position found from path {}", zkPath, t);
                            // invalid dlsn recorded in subscription state store
                            result.setValue(DLSN.NonInclusiveLowerBound);
                        }
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            result.setException(zkce);
        } catch (InterruptedException ie) {
            result.setException(new DLInterruptedException("getLastCommitPosition was interrupted", ie));
        }
        return result;
    }

    /**
     * Advances the position associated with the subscriber
     *
     * @param newPosition - new commit position
     */
    @Override
    public Future<BoxedUnit> advanceCommitPosition(DLSN newPosition) {
        if (null == lastCommittedPosition.get() ||
            (newPosition.compareTo(lastCommittedPosition.get()) > 0)) {
            lastCommittedPosition.set(newPosition);
            return Utils.zkAsyncCreateFullPathOptimisticAndSetData(zooKeeperClient,
                zkPath, newPosition.serialize().getBytes(Charsets.UTF_8),
                zooKeeperClient.getDefaultACL(),
                CreateMode.PERSISTENT);
        } else {
            return Future.Done();
        }
    }
}
