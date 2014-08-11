package com.twitter.distributedlog.subscription;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

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
        final Promise<DLSN> result = new Promise<DLSN>();
        if (null != lastCommittedPosition.get()) {
            result.setValue(lastCommittedPosition.get());
        } else {
            try {
                zooKeeperClient.get().getData(zkPath, false, new AsyncCallback.DataCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                        if (KeeperException.Code.NONODE.intValue() == rc) {
                            result.setValue(DLSN.InitialDLSN);
                        } else if (KeeperException.Code.OK.intValue() != rc) {
                            result.setException(KeeperException.create(KeeperException.Code.get(rc), path));
                        } else {
                            DLSN dlsn = DLSN.deserialize(new String(data, Charsets.UTF_8));
                            result.setValue(dlsn);
                        }
                    }
                }, null);
            } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
                result.setException(zkce);
            } catch (InterruptedException ie) {
                result.setException(new DLInterruptedException("getLastCommitPosition was interrupted", ie));
            }
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
