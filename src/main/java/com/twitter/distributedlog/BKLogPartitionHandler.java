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
package com.twitter.distributedlog;

import com.twitter.distributedlog.metadata.BKDLConfig;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;

/**
 * BookKeeper Distributed Log Manager
 * <p/>
 * The URI format for bookkeeper is distributed://[zkEnsemble]/[rootZnode]
 * [zookkeeper ensemble] is a list of semi-colon separated, zookeeper host:port
 * pairs. In the example above there are 3 servers, in the ensemble,
 * zk1, zk2 &amp; zk3, each one listening on port 2181.
 * <p/>
 * [root znode] is the path of the zookeeper znode, under which the editlog
 * information will be stored.
 * <p/>
 * Other configuration options are:
 * <ul>
 * <li><b>output-buffer-size</b>
 * Number of bytes a bookkeeper journal stream will buffer before
 * forcing a flush. Default is 1024.</li>
 * <li><b>ensemble-size</b>
 * Number of bookkeeper servers in edit log ledger ensembles. This
 * is the number of bookkeeper servers which need to be available
 * for the ledger to be writable. Default is 3.</li>
 * <li><b>quorum-size</b>
 * Number of bookkeeper servers in the write quorum. This is the
 * number of bookkeeper servers which must have acknowledged the
 * write of an entry before it is considered written.
 * Default is 2.</li>
 * <li><b>digestPw</b>
 * Password to use when creating ledgers. </li>
 * </ul>
 */
abstract class BKLogPartitionHandler implements Watcher  {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionHandler.class);

    private static final int LAYOUT_VERSION = -1;

    protected final String name;
    protected final String streamIdentifier;
    protected final ZooKeeperClient zooKeeperClient;
    protected final DistributedLogConfiguration conf;
    protected final BookKeeperClient bookKeeperClient;
    protected final String partitionRootPath;
    protected final String ledgerPath;
    protected final String digestpw;
    protected long lastLedgerRollingTimeMillis = -1;
    protected final ScheduledExecutorService executorService;
    protected final StatsLogger statsLogger;
    private AtomicBoolean ledgerListWatchSet = new AtomicBoolean(false);
    private AtomicReference<Watcher> chainedWatcher = new AtomicReference<Watcher>();

    // Maintain the list of ledgers
    protected final Map<String, LogSegmentLedgerMetadata> logSegments =
        new HashMap<String, LogSegmentLedgerMetadata>();
    protected volatile GetLedgersTask firstGetLedgersTask = null;

    static class GetLedgersTask implements BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>> {
        final String path;
        final boolean allowEmpty;
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        int rc = BKException.Code.InterruptedException;

        GetLedgersTask(String path, boolean allowEmpty) {
            this.path = path;
            this.allowEmpty = allowEmpty;
        }

        @Override
        public void operationComplete(int rc, List<LogSegmentLedgerMetadata > logSegmentLedgerMetadatas) {
            this.rc = rc;
            if (BKException.Code.OK == rc) {
                LOG.debug("Updated ledgers list : {}", path, logSegmentLedgerMetadatas);
            }
            countDownLatch.countDown();
        }

        void waitForFinish() throws IOException {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new IOException("Interrupted on getting ledgers list for " + path, e);
            }
            if ((KeeperException.Code.OK.intValue() != rc) &&
                ((KeeperException.Code.NONODE.intValue() != rc) || (!allowEmpty))) {
                throw new IOException("Error getting ledgers list for " + path);
            }
        }
    }

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogPartitionHandler(String name,
                          String streamIdentifier,
                          DistributedLogConfiguration conf,
                          URI uri,
                          ZooKeeperClientBuilder zkcBuilder,
                          BookKeeperClientBuilder bkcBuilder,
                          ScheduledExecutorService executorService,
                          StatsLogger statsLogger) throws IOException {
        this.name = name;
        this.streamIdentifier = streamIdentifier;
        this.conf = conf;
        this.executorService = executorService;
        this.statsLogger = statsLogger;
        partitionRootPath = String.format("%s/%s/%s", uri.getPath(), name, streamIdentifier);

        ledgerPath = partitionRootPath + "/ledgers";
        digestpw = conf.getBKDigestPW();

        try {
            if (null == zkcBuilder) {
                zkcBuilder = ZooKeeperClientBuilder.newBuilder()
                        .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                        .uri(uri).buildNew(false);
            }
            this.zooKeeperClient = zkcBuilder.build();
            LOG.debug("Using ZK Path {}", partitionRootPath);
            if (null == bkcBuilder) {
                // resolve uri
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(this.zooKeeperClient, uri);
                bkcBuilder = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(conf).bkdlConfig(bkdlConfig)
                        .name(String.format("%s:shared", name)).statsLogger(statsLogger);
            }
            this.bookKeeperClient = bkcBuilder.build();
        } catch (Exception e) {
            throw new IOException("Error initializing zk", e);
        }
    }

    protected void scheduleGetLedgersTask(boolean watch, boolean allowEmpty) {
        if (!watch) {
            ledgerListWatchSet.set(true);
        }
        firstGetLedgersTask = new GetLedgersTask(getFullyQualifiedName(), allowEmpty);
        asyncGetLedgerListInternal(LogSegmentLedgerMetadata.COMPARATOR, watch ? this : null, firstGetLedgersTask);
        LOG.info("Scheduled get ledgers task for {}, watch = {}.", getFullyQualifiedName(), watch);
    }

    protected void waitFirstGetLedgersTaskToFinish() throws IOException {
        GetLedgersTask task = firstGetLedgersTask;
        if (null != task) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Wait first getting ledgers task to finish for {}.", getFullyQualifiedName());
            }
            task.waitForFinish();
        }
    }

    public long getLastTxId(boolean recover) throws IOException {
        checkLogStreamExists();
        List<LogSegmentLedgerMetadata> ledgerList = getLedgerListDesc(true, true);

        // The ledger list should at least have one element
        // The last TxId is valid if the ledger is already completed else we must recover
        // the last TxId
        if (ledgerList.get(0).isInProgress()) {
            long lastTxId = recoverLastTxId(ledgerList.get(0), recover);
            if (((DistributedLogConstants.INVALID_TXID == lastTxId) ||
                (DistributedLogConstants.EMPTY_LEDGER_TX_ID == lastTxId)) &&
                (ledgerList.size() > 1)) {
                lastTxId = ledgerList.get(1).getLastTxId();
            }
            return lastTxId;
        } else {
            return ledgerList.get(0).getLastTxId();
        }
    }

    public long getFirstTxId() throws IOException {
        checkLogStreamExists();
        List<LogSegmentLedgerMetadata> ledgerList = getLedgerList(true);

        // The ledger list should at least have one element
        // First TxId is populated even for in progress ledgers
        return ledgerList.get(0).getFirstTxId();
    }


    private void checkLogStreamExists() throws IOException {
        try {
            if (null == zooKeeperClient.get().exists(ledgerPath, false)) {
                throw new LogEmptyException("Log " + name + ":" + getFullyQualifiedName() + " is empty");
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while reading {}", ledgerPath, ie);
            throw new LogEmptyException("Log " + name + ":" + getFullyQualifiedName() + " is empty");
        } catch (KeeperException ke) {
            LOG.error("Error reading {} entry in zookeeper", ledgerPath, ke);
            throw new LogEmptyException("Log " + name + ":" + getFullyQualifiedName() + " is empty");
        }
    }


    public long getTxIdNotLaterThan(long thresholdTxId)
        throws IOException {
        checkLogStreamExists();
        LedgerHandleCache handleCachePriv = new LedgerHandleCache(bookKeeperClient, digestpw);
        LedgerDataAccessor ledgerDataAccessorPriv = new LedgerDataAccessor(handleCachePriv, statsLogger);
        List<LogSegmentLedgerMetadata> ledgerListDesc = getLedgerListDesc(true);
        for (LogSegmentLedgerMetadata l : ledgerListDesc) {
            LOG.debug("Inspecting Ledger: {}", l);
            if (thresholdTxId < l.getFirstTxId()) {
                continue;
            }

            if (l.isInProgress()) {
                try {
                    long lastTxId = recoverLastTxId(l, false);
                    if ((lastTxId != DistributedLogConstants.EMPTY_LEDGER_TX_ID) &&
                        (lastTxId != DistributedLogConstants.INVALID_TXID) &&
                        (lastTxId < thresholdTxId)) {
                        return lastTxId;
                    }
                } catch (Exception exc) {
                    LOG.info("Optimistic Transaction Id recovery failed.", exc);
                }
            }

            try {
                LedgerDescriptor ledgerDescriptor = handleCachePriv.openLedger(l, !l.isInProgress());
                BKPerStreamLogReader s
                    = new BKPerStreamLogReader(ledgerDescriptor, l, 0, ledgerDataAccessorPriv, false);

                LogRecord prevRecord = null;
                LogRecord currRecord = s.readOp();
                while ((null != currRecord) && (currRecord.getTransactionId() <= thresholdTxId)) {
                    prevRecord = currRecord;
                    currRecord = s.readOp();
                }

                if (null != prevRecord) {
                    if (prevRecord.getTransactionId() < 0) {
                        LOG.info("getTxIdNotLaterThan returned negative value {} for input {}", prevRecord.getTransactionId(), thresholdTxId);
                    }
                    return prevRecord.getTransactionId();
                }

            } catch (Exception e) {
                throw new IOException("Could not open ledger for " + thresholdTxId, e);
            }

        }

        if (ledgerListDesc.size() == 0) {
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
        }

        throw new AlreadyTruncatedTransactionException("Records prior to" + thresholdTxId +
            " have already been deleted for log " + getFullyQualifiedName());
    }

    protected void checkLogExists() throws IOException {
        /*
            try {
                if (null == zooKeeperClient.exists(ledgerPath, false)) {
                    throw new IOException("Log does not exist or has been deleted");
                }
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while deleting " + ledgerPath, ie);
                throw new IOException("Log does not exist or has been deleted");
            } catch (KeeperException ke) {
                LOG.error("Error deleting" + ledgerPath + "entry in zookeeper", ke);
                throw new IOException("Log does not exist or has been deleted");
            }
        */
    }

    public void close() throws IOException {
        try {
            bookKeeperClient.release();
            zooKeeperClient.close();
        } catch (Exception e) {
            throw new IOException("Couldn't close zookeeper client", e);
        }
    }

    /**
     * Find the id of the last edit log transaction writen to a edit log
     * ledger.
     */
    protected long recoverLastTxId(LogSegmentLedgerMetadata l, boolean fence)
        throws IOException {
        try {
            LedgerHandleCache handleCachePriv = new LedgerHandleCache(bookKeeperClient, digestpw);
            LedgerDataAccessor ledgerDataAccessorPriv = new LedgerDataAccessor(handleCachePriv, statsLogger);
            boolean trySmallLedger = true;
            long scanStartPoint = 0;
            LedgerDescriptor ledgerDescriptor = null;
            ledgerDescriptor = handleCachePriv.openLedger(l, fence);
            scanStartPoint = handleCachePriv.getLastAddConfirmed(ledgerDescriptor);

            if (scanStartPoint < 0) {
                // Ledger is empty
                return DistributedLogConstants.EMPTY_LEDGER_TX_ID;
            }

            if (fence) {
                LOG.debug("Open With Recovery Last Add Confirmed {}", scanStartPoint);
            } else {
                LOG.debug("Open No Recovery Last Add Confirmed {}", scanStartPoint);
                if (scanStartPoint > DistributedLogConstants.SMALL_LEDGER_THRESHOLD) {
                    scanStartPoint -= DistributedLogConstants.SMALL_LEDGER_THRESHOLD;
                    trySmallLedger = false;
                }
            }

            long endTxId;
            while (true) {
                BKPerStreamLogReader in
                    = new BKPerStreamLogReader(ledgerDescriptor, l, scanStartPoint, ledgerDataAccessorPriv, fence);

                endTxId = DistributedLogConstants.INVALID_TXID;
                try {
                    LogRecord record = in.readOp();
                    while (record != null) {
                        if (endTxId == DistributedLogConstants.INVALID_TXID
                            || record.getTransactionId() > endTxId) {
                            endTxId = record.getTransactionId();
                        }
                        record = in.readOp();
                    }
                } catch (Exception exc) {
                    LOG.info("Reading beyond flush point", exc);
                } finally {
                    in.close();
                }

                if (0 == scanStartPoint) {
                    break;
                } else if (endTxId != DistributedLogConstants.INVALID_TXID) {
                    break;
                } else {
                    if (trySmallLedger && (scanStartPoint > DistributedLogConstants.SMALL_LEDGER_THRESHOLD)) {
                        LOG.info("Retrying recovery from an earlier point in the ledger");
                        scanStartPoint -= DistributedLogConstants.SMALL_LEDGER_THRESHOLD;
                        trySmallLedger = false;
                    } else {
                        LOG.info("Retrying recovery from the beginning of the ledger");
                        scanStartPoint = 0;
                    }
                    // We should try to open a fresh handle for the next attempt to find the
                    // position in the ledger
                    ledgerDescriptor = handleCachePriv.openLedger(l, fence);
                }
            }

            return endTxId;
        } catch (Exception e) {
            throw new IOException("Exception retreiving last tx id for ledger " + l,
                e);
        }
    }

    public String getFullyQualifiedName() {
        return String.format("%s:%s", name, streamIdentifier);
    }

    // Ledgers Related Functions

    protected List<LogSegmentLedgerMetadata> getLedgerList(boolean forceFetch, boolean throwOnEmpty) throws IOException {
        return getLedgerList(forceFetch, LogSegmentLedgerMetadata.COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentLedgerMetadata> getLedgerListDesc(boolean forceFetch, boolean throwOnEmpty) throws IOException {
        return getLedgerList(forceFetch, LogSegmentLedgerMetadata.DESC_COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentLedgerMetadata> getLedgerList(boolean forceFetch) throws IOException {
        return getLedgerList(forceFetch, LogSegmentLedgerMetadata.COMPARATOR, false);
    }

    protected List<LogSegmentLedgerMetadata> getLedgerListDesc(boolean forceFetch) throws IOException {
        return getLedgerList(forceFetch, LogSegmentLedgerMetadata.DESC_COMPARATOR, false);
    }

    protected List<LogSegmentLedgerMetadata> getLedgerList(boolean forceFetch, Comparator comparator, boolean throwOnEmpty)
        throws IOException {
        if (forceFetch) {
            return forceGetLedgerList(comparator, throwOnEmpty);
        } else {
            if(!ledgerListWatchSet.get()) {
                scheduleGetLedgersTask(true, true);
            }
            waitFirstGetLedgersTaskToFinish();
            List<LogSegmentLedgerMetadata> segmentsToReturn;
            synchronized (logSegments) {
                segmentsToReturn = new ArrayList<LogSegmentLedgerMetadata>(logSegments.size());
                segmentsToReturn.addAll(logSegments.values());
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Cached log segments : {}", segmentsToReturn);
                }
            }
            Collections.sort(segmentsToReturn, comparator);
            return segmentsToReturn;
        }
    }

    /**
     * Get a list of all segments in the journal.
     */
    protected List<LogSegmentLedgerMetadata> forceGetLedgerList(final Comparator comparator,
                                                                boolean throwOnEmpty) throws IOException {
        final List<LogSegmentLedgerMetadata> ledgers = new ArrayList<LogSegmentLedgerMetadata>();
        final AtomicInteger result = new AtomicInteger(-1);
        final CountDownLatch latch = new CountDownLatch(1);
        int retryCount = 5;
        int backOff = conf.getReadAheadWaitTime();
        do {
            ledgers.clear();
            asyncGetLedgerListInternal(comparator, null, new BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>>() {
                @Override
                public void operationComplete(int rc, List<LogSegmentLedgerMetadata> logSegmentLedgerMetadatas) {
                    result.set(rc);
                    if (KeeperException.Code.OK.intValue() == rc) {
                        ledgers.addAll(logSegmentLedgerMetadatas);
                    } else {
                        LOG.error("Failed to get ledger list for {} : ", getFullyQualifiedName(), KeeperException.create(rc));
                    }
                    latch.countDown();
                }
            });
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new IOException("Interrupting getting ledger list for " + getFullyQualifiedName(), e);
            }

            KeeperException.Code rc = KeeperException.Code.get(result.get());
            if (rc == KeeperException.Code.OK) {
                break;
            } else if((rc != KeeperException.Code.SESSIONEXPIRED) &&
                (rc != KeeperException.Code.CONNECTIONLOSS) &&
                (rc != KeeperException.Code.SESSIONMOVED)) {
                throw new IOException("Exception reading ledger list for " + getFullyQualifiedName(), KeeperException.create(rc));
            }

            retryCount--;
            try {
                Thread.sleep(backOff);
            } catch (InterruptedException exc) {
                //start a new iteration
            }
        } while (retryCount > 0);

        if (throwOnEmpty && ledgers.isEmpty()) {
            throw new LogEmptyException("Log " + name + ":" + getFullyQualifiedName() + " is empty");
        }
        return ledgers;
    }

    /**
     * Add the segment <i>metadata</i> for <i>name</i> in the cache.
     *
     * @param name
     *          segment znode name.
     * @param metadata
     *          segment metadata.
     */
    protected void addLogSegmentToCache(String name, LogSegmentLedgerMetadata metadata) {
        synchronized (logSegments) {
            if (!logSegments.containsKey(name)) {
                logSegments.put(name, metadata);
                LOG.debug("Added log segment ({} : {}) to cache.", name, metadata);
            }
        }
        // update the last ledger rolling time
        if (!metadata.isInProgress() && (lastLedgerRollingTimeMillis < metadata.getCompletionTime())) {
            lastLedgerRollingTimeMillis = metadata.getCompletionTime();
        }
    }

    protected LogSegmentLedgerMetadata readLogSegmentFromCache(String name) {
        synchronized (logSegments) {
            return logSegments.get(name);
        }
    }

    protected void removeLogSegmentToCache(String name) {
        synchronized (logSegments) {
            LogSegmentLedgerMetadata metadata = logSegments.remove(name);
            LOG.debug("Removed log segment ({} : {}) from cache.", name, metadata);
        }
    }

    protected void asyncGetLedgerList(final Comparator comparator, Watcher watcher,
                                      final BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>> callback) {
        if (null != watcher) {
            chainedWatcher.set(watcher);
            watcher = this;
        }
        asyncGetLedgerListInternal(comparator, watcher, callback);
    }

    private void asyncGetLedgerListInternal(final Comparator comparator, Watcher watcher,
                                            final BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>> callback) {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Async getting ledger list for {}.", getFullyQualifiedName());
            }
            zooKeeperClient.get().getChildren(ledgerPath, watcher, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        callback.operationComplete(rc, null);
                        return;
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Got ledger list from {} : {}", ledgerPath, children);
                    }

                    ledgerListWatchSet.set(true);
                    Set<String> segmentsReceived = new HashSet<String>();
                    segmentsReceived.addAll(children);
                    Set<String> segmentsAdded;
                    Set<String> segmentsRemoved;
                    final List<LogSegmentLedgerMetadata> segmentList;
                    synchronized (logSegments) {
                        Set<String> segmentsCached = logSegments.keySet();
                        segmentsAdded = Sets.difference(segmentsReceived, segmentsCached).immutableCopy();
                        segmentsRemoved = Sets.difference(segmentsCached, segmentsReceived).immutableCopy();
                        for (String s : segmentsRemoved) {
                            logSegments.remove(s);
                            LOG.debug("Removed log segment {} from cache.", s);
                        }
                        segmentList = new ArrayList<LogSegmentLedgerMetadata>(segmentsAdded.size() + logSegments.size());
                        segmentList.addAll(logSegments.values());
                    }

                    if (segmentsAdded.isEmpty()) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("No segments added for {}.", getFullyQualifiedName());
                        }
                        Collections.sort(segmentList, comparator);
                        callback.operationComplete(KeeperException.Code.OK.intValue(), segmentList);
                        return;
                    }

                    final AtomicInteger numChildren = new AtomicInteger(segmentsAdded.size());
                    final AtomicInteger numFailures = new AtomicInteger(0);
                    for (final String segment: segmentsAdded) {
                        LogSegmentLedgerMetadata.read(zooKeeperClient,
                            ledgerPath + "/" + segment, conf.getDLLedgerMetadataLayoutVersion(),
                            new BookkeeperInternalCallbacks.GenericCallback<LogSegmentLedgerMetadata>() {
                                @Override
                                public void operationComplete(int rc, LogSegmentLedgerMetadata result) {
                                    if (BKException.Code.OK != rc) {
                                        // fail fast
                                        if (1 == numFailures.incrementAndGet()) {
                                            // :( properly we need dlog related response code.
                                            callback.operationComplete(rc, null);
                                            return;
                                        }
                                    } else {
                                        addLogSegmentToCache(segment, result);
                                        segmentList.add(result);
                                    }
                                    if (0 == numChildren.decrementAndGet() && numFailures.get() == 0) {
                                        Collections.sort(segmentList, comparator);
                                        callback.operationComplete(BKException.Code.OK, segmentList);
                                    }
                                }
                            });
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            callback.operationComplete(BKException.Code.ZKException, null);
        } catch (InterruptedException e) {
            callback.operationComplete(BKException.Code.InterruptedException, null);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        Watcher cw = chainedWatcher.get();
        if (null != cw) {
            cw.process(event);
            chainedWatcher.set(null);
        }

        if (Watcher.Event.EventType.None.equals(event.getType())) {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                if (!ledgerListWatchSet.get()) {
                    asyncGetLedgerListInternal(LogSegmentLedgerMetadata.COMPARATOR, this, new GetLedgersTask(getFullyQualifiedName(), false));
                }
            } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
                ledgerListWatchSet.set(false);
            }
        } else if (Watcher.Event.EventType.NodeChildrenChanged.equals(event.getType())) {
            asyncGetLedgerListInternal(LogSegmentLedgerMetadata.COMPARATOR, this, new GetLedgersTask(getFullyQualifiedName(), false));
        }
    }

}
