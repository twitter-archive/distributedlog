/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.function.VoidFunctions;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

/**
 * Basic Utilities.
 */
public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

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

    /**
     * Synchronously create zookeeper path recursively and optimistically.
     *
     * @see {@link #zkAsyncCreateFullPathOptimistic(ZooKeeperClient, String, byte[], List, CreateMode)}
     * @param zkc Zookeeper client
     * @param path Zookeeper full path
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     * @throws ZooKeeperClient.ZooKeeperConnectionException
     * @throws KeeperException
     * @throws InterruptedException
     */
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
        Optional<String> parentPathShouldNotCreate = Optional.absent();
        return zkAsyncCreateFullPathOptimistic(
                zkc,
                pathToCreate,
                parentPathShouldNotCreate,
                data,
                acl,
                createMode);
    }

    /**
     * Asynchronously create zookeeper path recursively and optimistically
     *
     * @param zkc Zookeeper client
     * @param pathToCreate  Zookeeper full path
     * @param parentPathShouldNotCreate zookeeper parent path should not be created
     * @param data Zookeeper data
     * @param acl Acl of the zk path
     * @param createMode Create mode of zk path
     */
    public static Future<BoxedUnit> zkAsyncCreateFullPathOptimistic(
        final ZooKeeperClient zkc,
        final String pathToCreate,
        final Optional<String> parentPathShouldNotCreate,
        final byte[] data,
        final List<ACL> acl,
        final CreateMode createMode) {
        final Promise<BoxedUnit> result = new Promise<BoxedUnit>();

        zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate, parentPathShouldNotCreate,
                data, acl, createMode, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
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
                    if (rc != KeeperException.Code.NONODE.intValue()) {
                        handleKeeperExceptionCode(rc, path, result);
                        return;
                    }

                    Optional<String> parentPathShouldNotCreate = Optional.absent();
                    zkAsyncCreateFullPathOptimisticRecursive(zkc, pathToCreate, parentPathShouldNotCreate,
                            data, acl, createMode, new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
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

    public static Future<Versioned<byte[]>> zkGetData(ZooKeeperClient zkc, String path, boolean watch) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return Future.exception(FutureUtils.zkException(e, path));
        } catch (InterruptedException e) {
            return Future.exception(FutureUtils.zkException(e, path));
        }
        return zkGetData(zk, path, watch);
    }

    /**
     * Retrieve data from zookeeper <code>path</code>.
     *
     * @param path
     *          zookeeper path to retrieve data
     * @param watch
     *          whether to watch the path
     * @return future representing the versioned value. null version or null value means path doesn't exist.
     */
    public static Future<Versioned<byte[]>> zkGetData(ZooKeeper zk, String path, boolean watch) {
        final Promise<Versioned<byte[]>> promise = new Promise<Versioned<byte[]>>();
        zk.getData(path, watch, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    if (null == stat) {
                        promise.setValue(new Versioned<byte[]>(null, null));
                    } else {
                        promise.setValue(new Versioned<byte[]>(data, new ZkVersion(stat.getVersion())));
                    }
                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                    promise.setValue(new Versioned<byte[]>(null, null));
                } else {
                    promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                }
            }
        }, null);
        return promise;
    }

    public static Future<ZkVersion> zkSetData(ZooKeeperClient zkc, String path, byte[] data, ZkVersion version) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return Future.exception(FutureUtils.zkException(e, path));
        } catch (InterruptedException e) {
            return Future.exception(FutureUtils.zkException(e, path));
        }
        return zkSetData(zk, path, data, version);
    }

    /**
     * Set <code>data</code> to zookeeper <code>path</code>.
     *
     * @param zk
     *          zookeeper client
     * @param path
     *          path to set data
     * @param data
     *          data to set
     * @param version
     *          version used to set data
     * @return future representing the version after this operation.
     */
    public static Future<ZkVersion> zkSetData(ZooKeeper zk, String path, byte[] data, ZkVersion version) {
        final Promise<ZkVersion> promise = new Promise<ZkVersion>();
        zk.setData(path, data, version.getZnodeVersion(), new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    promise.updateIfEmpty(new Return<ZkVersion>(new ZkVersion(stat.getVersion())));
                    return;
                }
                promise.updateIfEmpty(new Throw<ZkVersion>(
                        KeeperException.create(KeeperException.Code.get(rc))));
                return;
            }
        }, null);
        return promise;
    }

    public static Future<Void> zkDelete(ZooKeeperClient zkc, String path, ZkVersion version) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return Future.exception(FutureUtils.zkException(e, path));
        } catch (InterruptedException e) {
            return Future.exception(FutureUtils.zkException(e, path));
        }
        return zkDelete(zk, path, version);
    }

    /**
     * Delete the given <i>path</i> from zookeeper.
     *
     * @param zk
     *          zookeeper client
     * @param path
     *          path to delete
     * @param version
     *          version used to set data
     * @return future representing the version after this operation.
     */
    public static Future<Void> zkDelete(ZooKeeper zk, String path, ZkVersion version) {
        final Promise<Void> promise = new Promise<Void>();
        zk.delete(path, version.getZnodeVersion(), new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    promise.updateIfEmpty(new Return<Void>(null));
                    return;
                }
                promise.updateIfEmpty(new Throw<Void>(
                        KeeperException.create(KeeperException.Code.get(rc))));
                return;
            }
        }, null);
        return promise;
    }

    public static Future<Void> asyncClose(@Nullable AsyncCloseable closeable,
                                          boolean swallowIOException) {
        if (null == closeable) {
            return Future.Void();
        } else if (swallowIOException) {
            return FutureUtils.ignore(closeable.asyncClose());
        } else {
            return closeable.asyncClose();
        }
    }

    /**
     * Sync zookeeper client on given <i>path</i>.
     *
     * @param zkc
     *          zookeeper client
     * @param path
     *          path to sync
     * @return zookeeper client after sync
     * @throws IOException
     */
    public static ZooKeeper sync(ZooKeeperClient zkc, String path) throws IOException {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on checking if log " + path + " exists", e);
        }
        final CountDownLatch syncLatch = new CountDownLatch(1);
        final AtomicInteger syncResult = new AtomicInteger(0);
        zk.sync(path, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                syncResult.set(rc);
                syncLatch.countDown();
            }
        }, null);
        try {
            syncLatch.await();
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on syncing zookeeper connection", e);
        }
        if (KeeperException.Code.OK.intValue() != syncResult.get()) {
            throw new ZKException("Error syncing zookeeper connection ",
                    KeeperException.Code.get(syncResult.get()));
        }
        return zk;
    }

    /**
     * Close a closeable.
     *
     * @param closeable
     *          closeable to close
     */
    public static void close(@Nullable Closeable closeable) {
        if (null == closeable) {
            return;
        }
        try {
            Closeables.close(closeable, true);
        } catch (IOException e) {
            // no-op. the exception is swallowed.
        }
    }

    /**
     * Close an async closeable.
     *
     * @param closeable
     *          closeable to close
     */
    public static void close(@Nullable AsyncCloseable closeable)
            throws IOException {
        if (null == closeable) {
            return;
        }
        FutureUtils.result(closeable.asyncClose());
    }

    /**
     * Close an async closeable.
     *
     * @param closeable
     *          closeable to close
     */
    public static void closeQuietly(@Nullable AsyncCloseable closeable) {
        if (null == closeable) {
            return;
        }
        try {
            FutureUtils.result(closeable.asyncClose());
        } catch (IOException e) {
            // no-op. the exception is swallowed.
        }
    }

    /**
     * Close the closeables in sequence.
     *
     * @param closeables
     *          closeables to close
     * @return future represents the close future
     */
    public static Future<Void> closeSequence(ExecutorService executorService,
                                             AsyncCloseable... closeables) {
        return closeSequence(executorService, false, closeables);
    }

    /**
     * Close the closeables in sequence and ignore errors during closing.
     *
     * @param executorService executor to execute closeable
     * @param ignoreCloseError whether to ignore errors during closing
     * @param closeables list of closeables
     * @return future represents the close future.
     */
    public static Future<Void> closeSequence(ExecutorService executorService,
                                             boolean ignoreCloseError,
                                             AsyncCloseable... closeables) {
        List<AsyncCloseable> closeableList = Lists.newArrayListWithExpectedSize(closeables.length);
        for (AsyncCloseable closeable : closeables) {
            if (null == closeable) {
                closeableList.add(AsyncCloseable.NULL);
            } else {
                closeableList.add(closeable);
            }
        }
        return FutureUtils.processList(
                closeableList,
                ignoreCloseError ? AsyncCloseable.CLOSE_FUNC_IGNORE_ERRORS : AsyncCloseable.CLOSE_FUNC,
                executorService).map(VoidFunctions.LIST_TO_VOID_FUNC);
    }

}
