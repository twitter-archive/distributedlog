package com.twitter.distributedlog.util;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.ZooKeeperClient;
import scala.runtime.BoxedUnit;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.Promise;

import static com.google.common.base.Charsets.UTF_8;

public class Utils {
    private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

    public static byte[] ledgerId2Bytes(long ledgerId) {
        return Long.toString(ledgerId).getBytes(UTF_8);
    }

    public static long bytes2LedgerId(byte[] data) {
        return Long.valueOf(new String(data, UTF_8));
    }

    /**
     * Current time from some arbitrary time base in the past, counting in
     * nanoseconds, and not affected by settimeofday or similar system clock
     * changes. This is appropriate to use when computing how much longer to
     * wait for an interval to expire.
     *
     * @return current time in nanoseconds.
     */
    public static long nowInNanos() {
        return System.nanoTime();
    }

    /**
     * Current time from some fixed base time - so useful for cross machine
     * comparison
     *
     * @return current time in milliseconds.
     */
    public static long nowInMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Milliseconds elapsed since the time specified, the input is nanoTime
     * the only conversion happens when computing the elapsed time
     *
     * @param startMsecTime the start of the interval that we are measuring
     * @return elapsed time in milliseconds.
     */
    public static long elapsedMSec(long startMsecTime) {
        return (System.currentTimeMillis() - startMsecTime);
    }

    public static boolean randomPercent(double percent) {
        return (Math.random() * 100.0) <= percent;
    }

    public static void zkCreateFullPathOptimistic(
        ZooKeeperClient zkc,
        String path,
        byte[] data,
        final List<ACL> acl,
        final CreateMode createMode)
        throws ZooKeeperClient.ZooKeeperConnectionException, KeeperException, InterruptedException {
        try {
            Await.result(zkAsyncCreateFullPathOptimistic(zkc, path, data, acl, createMode));
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            throw zkce;
        } catch (KeeperException ke) {
            throw ke;
        } catch (InterruptedException ie) {
            throw ie;
        } catch (RuntimeException rte) {
            throw rte;
        } catch (Exception exc) {
            throw new RuntimeException("Unexpected Exception", exc);
        }
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param parentPathShouldNotCreate The recursive creation should stop if this path doesn't exist
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     * @param callback Callback
     * @param ctx Context object
     */
    public static void zkAsyncCreateFullPathOptimisticRecursive(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final Optional<String> parentPathShouldNotCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode,
        final AsyncCallback.StringCallback callback,
        final Object ctx) {
        try {
            zkc.get().create(pathToCreate, data, acl, createMode, new AsyncCallback.StringCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, String name) {

                    if (rc != KeeperException.Code.NONODE.intValue()) {
                        callback.processResult(rc, path, ctx, name);
                        return;
                    }

                    // Since we got a nonode, it means that my parents may not exist
                    // ephemeral nodes can't have children so Create mode is always
                    // persistent parents
                    int lastSlash = pathToCreate.lastIndexOf('/');
                    if (lastSlash <= 0) {
                        callback.processResult(rc, path, ctx, name);
                        return;
                    }
                    String parent = pathToCreate.substring(0, lastSlash);
                    if (parentPathShouldNotCreate.isPresent() && Objects.equal(parentPathShouldNotCreate.get(), parent)) {
                        // we should stop here
                        callback.processResult(rc, path, ctx, name);
                        return;
                    }
                    zkAsyncCreateFullPathOptimisticRecursive(zkc, parent, parentPathShouldNotCreate, new byte[0], acl,
                        CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            if (rc == KeeperException.Code.OK.intValue() || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                                // succeeded in creating the parent, now create the original path
                                zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate, parentPathShouldNotCreate,
                                        data, acl, createMode, callback, ctx);
                            } else {
                                callback.processResult(rc, path, ctx, name);
                            }
                        }
                    }, ctx);
                }
            }, ctx);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            callback.processResult(DistributedLogConstants.ZK_CONNECTION_EXCEPTION_RESULT_CODE, zkce.getMessage(), ctx, pathToCreate);
        } catch (InterruptedException ie) {
            callback.processResult(DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE, ie.getMessage(), ctx, pathToCreate);
        }
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     */
    public static Future<BoxedUnit> zkAsyncCreateFullPathOptimistic(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode) {
        final Promise<BoxedUnit> result = new Promise<BoxedUnit>();

        Optional<String> parentPathShouldNotCreate = Optional.absent();
        zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate, parentPathShouldNotCreate,
                data, acl, createMode, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                Promise<BoxedUnit> result = (Promise<BoxedUnit>)ctx;
                handleKeeperExceptionCode(rc, path, result);
            }
        }, result);

        return result;
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically.
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     */
    public static Future<BoxedUnit> zkAsyncCreateFullPathOptimisticAndSetData(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode) {
        final Promise<BoxedUnit> result = new Promise<BoxedUnit>();

        try {
            zkc.get().setData(pathToCreate, data, -1, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    Promise<BoxedUnit> result = (Promise<BoxedUnit>)ctx;
                    if (rc != KeeperException.Code.NONODE.intValue()) {
                        handleKeeperExceptionCode(rc, path, result);
                        return;
                    }

                    Optional<String> parentPathShouldNotCreate = Optional.absent();
                    zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate, parentPathShouldNotCreate,
                            data, acl, createMode, new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            Promise<BoxedUnit> result = (Promise<BoxedUnit>)ctx;
                            handleKeeperExceptionCode(rc, path, result);
                        }
                    }, result);
                }
            }, result);
        } catch (Exception exc) {
            result.setException(exc);
        }

        return result;
    }

    private static void handleKeeperExceptionCode(int rc, String pathOrMessage, Promise<BoxedUnit> result) {
        if (KeeperException.Code.OK.intValue() == rc) {
            result.setValue(BoxedUnit.UNIT);
        } else if (DistributedLogConstants.ZK_CONNECTION_EXCEPTION_RESULT_CODE == rc) {
            result.setException(new ZooKeeperClient.ZooKeeperConnectionException(pathOrMessage));
        } else if (DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE == rc) {
            result.setException(new DLInterruptedException(pathOrMessage));
        } else {
            result.setException(KeeperException.create(KeeperException.Code.get(rc), pathOrMessage));
        }
    }


    /**
     * Simple watcher to notify when zookeeper has connected
     */
    public static class ZkConnectionWatcher implements Watcher {
        CountDownLatch zkConnectLatch;

        public ZkConnectionWatcher(CountDownLatch zkConnectLatch) {
            this.zkConnectLatch = zkConnectLatch;
        }

        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                zkConnectLatch.countDown();
            }
        }
    }
}
