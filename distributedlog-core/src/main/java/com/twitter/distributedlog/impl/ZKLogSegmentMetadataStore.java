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
package com.twitter.distributedlog.impl;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.callback.LogSegmentNamesListener;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.distributedlog.zk.DefaultZKOp;
import com.twitter.distributedlog.zk.ZKOp;
import com.twitter.distributedlog.zk.ZKTransaction;
import com.twitter.distributedlog.zk.ZKVersionedSetOp;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Charsets.UTF_8;

/**
 * ZooKeeper based log segment metadata store.
 */
public class ZKLogSegmentMetadataStore implements LogSegmentMetadataStore, Watcher, Children2Callback {

    private static final Logger logger = LoggerFactory.getLogger(ZKLogSegmentMetadataStore.class);

    private static class ReadLogSegmentsTask implements Runnable, FutureEventListener<List<String>> {

        private final String logSegmentsPath;
        private final ZKLogSegmentMetadataStore store;
        private int currentZKBackOffMs;

        ReadLogSegmentsTask(String logSegmentsPath,
                            ZKLogSegmentMetadataStore metadataStore) {
            this.logSegmentsPath = logSegmentsPath;
            this.store = metadataStore;
            this.currentZKBackOffMs = store.minZKBackoffMs;
        }

        @Override
        public void onSuccess(final List<String> segments) {
            // reset the back off after a successful operation
            currentZKBackOffMs = store.minZKBackoffMs;
            final Set<LogSegmentNamesListener> listenerSet = store.listeners.get(logSegmentsPath);
            if (null != listenerSet) {
                store.submitTask(logSegmentsPath, new Runnable() {
                    @Override
                    public void run() {
                        for (LogSegmentNamesListener listener : listenerSet) {
                            listener.onSegmentsUpdated(segments);
                        }
                    }
                });
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            int backoffMs = store.minZKBackoffMs;
            if ((cause instanceof KeeperException)) {
                KeeperException ke = (KeeperException) cause;
                if (KeeperException.Code.NONODE == ke.code()) {
                    // the log segment has been deleted, remove all the registered listeners
                    store.listeners.remove(logSegmentsPath);
                    return;
                }
                backoffMs = currentZKBackOffMs;
                currentZKBackOffMs = Math.min(2 * currentZKBackOffMs, store.maxZKBackoffMs);
            }
            store.scheduleTask(logSegmentsPath, this, backoffMs);
        }

        @Override
        public void run() {
            if (null != store.listeners.get(logSegmentsPath)) {
                store.getLogSegmentNames(logSegmentsPath, store).addEventListener(this);
            } else {
                logger.debug("Log segments listener for {} has been removed.", logSegmentsPath);
            }
        }
    }

    final DistributedLogConfiguration conf;
    // settings
    final int minZKBackoffMs;
    final int maxZKBackoffMs;
    final boolean skipMinVersionCheck;

    final ZooKeeperClient zkc;
    // log segment listeners
    final ConcurrentMap<String, Set<LogSegmentNamesListener>> listeners;
    // scheduler
    final OrderedScheduler scheduler;
    final ReentrantReadWriteLock closeLock;
    boolean closed = false;

    public ZKLogSegmentMetadataStore(DistributedLogConfiguration conf,
                                     ZooKeeperClient zkc,
                                     OrderedScheduler scheduler) {
        this.conf = conf;
        this.zkc = zkc;
        this.listeners = new ConcurrentHashMap<String, Set<LogSegmentNamesListener>>();
        this.scheduler = scheduler;
        this.closeLock = new ReentrantReadWriteLock();
        // settings
        this.minZKBackoffMs = conf.getZKRetryBackoffStartMillis();
        this.maxZKBackoffMs = conf.getZKRetryBackoffMaxMillis();
        this.skipMinVersionCheck = conf.getDLLedgerMetadataSkipMinVersionCheck();
    }

    protected void scheduleTask(Object key, Runnable r, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            scheduler.schedule(key, r, delayMs, TimeUnit.MILLISECONDS);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    protected void submitTask(Object key, Runnable r) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            scheduler.submit(key, r);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    // max sequence number and max transaction id

    @Override
    public void storeMaxLogSegmentSequenceNumber(Transaction<Object> txn,
                                                 String path,
                                                 Versioned<Long> lssn,
                                                 Transaction.OpListener<Version> listener) {
        Version version = lssn.getVersion();
        assert(version instanceof ZkVersion);

        ZkVersion zkVersion = (ZkVersion) version;
        byte[] data = DLUtils.serializeLogSegmentSequenceNumber(lssn.getValue());
        Op setDataOp = Op.setData(path, data, zkVersion.getZnodeVersion());
        ZKOp zkOp = new ZKVersionedSetOp(setDataOp, listener);
        txn.addOp(zkOp);
    }

    @Override
    public void storeMaxTxnId(Transaction<Object> txn,
                              String path,
                              Versioned<Long> transactionId,
                              Transaction.OpListener<Version> listener) {
        Version version = transactionId.getVersion();
        assert(version instanceof ZkVersion);

        ZkVersion zkVersion = (ZkVersion) version;
        byte[] data = DLUtils.serializeTransactionId(transactionId.getValue());
        Op setDataOp = Op.setData(path, data, zkVersion.getZnodeVersion());
        ZKOp zkOp = new ZKVersionedSetOp(setDataOp, listener);
        txn.addOp(zkOp);
    }

    // updates

    @Override
    public Transaction<Object> transaction() {
        return new ZKTransaction(zkc);
    }

    @Override
    public void createLogSegment(Transaction<Object> txn, LogSegmentMetadata segment) {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        Op createOp = Op.create(
                segment.getZkPath(),
                finalisedData,
                zkc.getDefaultACL(),
                CreateMode.PERSISTENT);
        txn.addOp(DefaultZKOp.of(createOp));
    }

    @Override
    public void deleteLogSegment(Transaction<Object> txn, LogSegmentMetadata segment) {
        Op deleteOp = Op.delete(
                segment.getZkPath(),
                -1);
        txn.addOp(DefaultZKOp.of(deleteOp));
    }

    @Override
    public void updateLogSegment(Transaction<Object> txn, LogSegmentMetadata segment) {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        Op updateOp = Op.setData(segment.getZkPath(), finalisedData, -1);
        txn.addOp(DefaultZKOp.of(updateOp));
    }

    // reads

    /**
     * Process the watched events for registered listeners
     */
    @Override
    public void process(WatchedEvent event) {
        if (Event.EventType.None == event.getType()
                && Event.KeeperState.Expired == event.getState()) {
            Set<String> keySet = new HashSet<String>(listeners.keySet());
            for (String logSegmentsPath : keySet) {
                scheduleTask(logSegmentsPath, new ReadLogSegmentsTask(logSegmentsPath, this), 0L);
            }
            return;
        }
        String path = event.getPath();
        if (null == path) {
            return;
        }
        switch (event.getType()) {
            case NodeDeleted:
                listeners.remove(path);
                break;
            case NodeChildrenChanged:
                new ReadLogSegmentsTask(path, this).run();
                break;
            default:
                break;
        }
    }

    @Override
    public Future<LogSegmentMetadata> getLogSegment(String logSegmentPath) {
        return LogSegmentMetadata.read(zkc, logSegmentPath, skipMinVersionCheck);
    }

    @Override
    public Future<List<String>> getLogSegmentNames(String logSegmentsPath) {
        return getLogSegmentNames(logSegmentsPath, null);
    }

    Future<List<String>> getLogSegmentNames(String logSegmentsPath, Watcher watcher) {
        Promise<List<String>> result = new Promise<List<String>>();
        try {
            zkc.get().getChildren(logSegmentsPath, watcher, this, result);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            result.setException(FutureUtils.zkException(e, logSegmentsPath));
        } catch (InterruptedException e) {
            result.setException(FutureUtils.zkException(e, logSegmentsPath));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        Promise<List<String>> result = ((Promise<List<String>>) ctx);
        if (KeeperException.Code.OK.intValue() == rc) {
            result.setValue(children);
        } else {
            result.setException(KeeperException.create(KeeperException.Code.get(rc)));
        }
    }

    @Override
    public void registerLogSegmentListener(String logSegmentsPath,
                                           LogSegmentNamesListener listener) {
        if (null == listener) {
            return;
        }
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            Set<LogSegmentNamesListener> listenerSet = listeners.get(logSegmentsPath);
            if (null == listenerSet) {
                Set<LogSegmentNamesListener> newListenerSet = new HashSet<LogSegmentNamesListener>();
                Set<LogSegmentNamesListener> oldListenerSet = listeners.putIfAbsent(logSegmentsPath, newListenerSet);
                if (null != oldListenerSet) {
                    listenerSet = oldListenerSet;
                } else {
                    listenerSet = newListenerSet;
                }
            }
            synchronized (listenerSet) {
                listenerSet.add(listener);
                if (!listeners.containsKey(logSegmentsPath)) {
                    // listener set has been removed, add it back
                    listeners.put(logSegmentsPath, listenerSet);
                }
            }
            new ReadLogSegmentsTask(logSegmentsPath, this).run();
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void unregisterLogSegmentListener(String logSegmentsPath,
                                             LogSegmentNamesListener listener) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            Set<LogSegmentNamesListener> listenerSet = listeners.get(logSegmentsPath);
            if (null == listenerSet) {
                return;
            }
            synchronized (listenerSet) {
                listenerSet.remove(listener);
                if (listenerSet.isEmpty()) {
                    listeners.remove(logSegmentsPath, listenerSet);
                }
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
    }

}
