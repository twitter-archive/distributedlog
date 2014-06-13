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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.metadata.BKDLConfig;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
abstract class BKLogPartitionHandler implements Watcher {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionHandler.class);

    static interface LogSegmentFilter {

        static final LogSegmentFilter DEFAULT_FILTER = new LogSegmentFilter() {
            @Override
            public Collection<String> filter(Collection<String> fullList) {
                return fullList;
            }
        };

        Collection<String> filter(Collection<String> fullList);
    }

    private static final int LAYOUT_VERSION = -1;

    protected final String name;
    protected final String streamIdentifier;
    protected final ZooKeeperClient zooKeeperClient;
    protected final DistributedLogConfiguration conf;
    protected final BookKeeperClient bookKeeperClient;
    protected final String partitionRootPath;
    protected final String ledgerPath;
    protected final String digestpw;
    protected volatile long lastLedgerRollingTimeMillis = -1;
    protected final ScheduledExecutorService executorService;
    protected final StatsLogger statsLogger;
    private final AtomicBoolean ledgerListWatchSet = new AtomicBoolean(false);
    private final AtomicBoolean isFullListFetched = new AtomicBoolean(false);
    protected volatile boolean reportGetSegmentStats = false;

    // listener
    protected final CopyOnWriteArraySet<LogSegmentListener> listeners =
            new CopyOnWriteArraySet<LogSegmentListener>();

    // Maintain the list of ledgers
    protected final Map<String, LogSegmentLedgerMetadata> logSegments =
        new HashMap<String, LogSegmentLedgerMetadata>();
    protected final ConcurrentMap<Long, LogSegmentLedgerMetadata> lid2LogSegments =
        new ConcurrentHashMap<Long, LogSegmentLedgerMetadata>();
    protected volatile SyncGetLedgersCallback firstGetLedgersTask = null;
    protected final AsyncNotification notification;
    // log segment filter
    protected final LogSegmentFilter filter;

    // Stats
    private static OpStatsLogger forceGetListStat;
    private static OpStatsLogger getListStat;
    private static OpStatsLogger getFilteredListStat;
    private static OpStatsLogger getFullListStat;
    private static OpStatsLogger getInprogressSegmentStat;
    private static OpStatsLogger getCompletedSegmentStat;
    private static OpStatsLogger negativeGetInprogressSegmentStat;
    private static OpStatsLogger negativeGetCompletedSegmentStat;
    private static OpStatsLogger recoverLastEntryStats;
    private static OpStatsLogger recoverScannedEntriesStats;

    static class SyncGetLedgersCallback implements GenericCallback<List<LogSegmentLedgerMetadata>> {

        final String path;
        final boolean allowEmpty;
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        int rc = BKException.Code.InterruptedException;

        SyncGetLedgersCallback(String path, boolean allowEmpty) {
            this.path = path;
            this.allowEmpty = allowEmpty;
        }

        @Override
        public void operationComplete(int rc, List<LogSegmentLedgerMetadata> logSegmentLedgerMetadatas) {
            this.rc = rc;
            if (KeeperException.Code.OK.intValue() == rc) {
                LOG.debug("Updated ledgers list for {} : {}", path, logSegmentLedgerMetadatas);
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

    static class NOPGetLedgersCallback implements GenericCallback<List<LogSegmentLedgerMetadata>> {

        final String path;

        NOPGetLedgersCallback(String path) {
            this.path = path;
        }

        @Override
        public void operationComplete(int rc, List<LogSegmentLedgerMetadata> logSegmentLedgerMetadatas) {
            if (BKException.Code.OK == rc) {
                LOG.debug("Updated ledgers list : {}", path, logSegmentLedgerMetadatas);
            }
        }
    }

    class WatcherGetLedgersCallback implements GenericCallback<List<LogSegmentLedgerMetadata>>, Runnable {

        final String path;

        WatcherGetLedgersCallback(String path) {
            this.path = path;
        }

        @Override
        public void operationComplete(int rc, List<LogSegmentLedgerMetadata > logSegmentLedgerMetadatas) {
            if (KeeperException.Code.OK.intValue() == rc) {
                LOG.debug("Updated ledgers list : {}", path, logSegmentLedgerMetadatas);
            } else {
                executorService.schedule(this, conf.getZKRetryBackoffStartMillis(), TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void run() {
            asyncGetLedgerListInternal(LogSegmentLedgerMetadata.COMPARATOR, filter, BKLogPartitionHandler.this, this);
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
                          StatsLogger statsLogger,
                          AsyncNotification notification,
                          LogSegmentFilter filter) throws IOException {
        this.name = name;
        this.streamIdentifier = streamIdentifier;
        this.conf = conf;
        this.executorService = executorService;
        this.statsLogger = statsLogger;
        this.notification = notification;
        this.filter = filter;
        partitionRootPath = BKDistributedLogManager.getPartitionPath(uri, name, streamIdentifier);

        ledgerPath = partitionRootPath + "/ledgers";
        digestpw = conf.getBKDigestPW();

        try {
            if (null == zkcBuilder) {
                zkcBuilder = ZooKeeperClientBuilder.newBuilder()
                        .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                        .uri(uri)
                        .statsLogger(statsLogger)
                        .retryThreadCount(conf.getZKClientNumberRetryThreads())
                        .buildNew(false);
            }
            this.zooKeeperClient = zkcBuilder.build();
            LOG.debug("Using ZK Path {}", partitionRootPath);
            if (null == bkcBuilder) {
                // resolve uri
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(this.zooKeeperClient, uri);
                BKDLConfig.propagateConfiguration(bkdlConfig, conf);
                bkcBuilder = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(conf).bkdlConfig(bkdlConfig)
                        .name(String.format("%s:shared", name)).statsLogger(statsLogger);
            }
            this.bookKeeperClient = bkcBuilder.build();
        } catch (KeeperException e) {
            throw new IOException("Error initializing bookkeeper client", e);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted initializing bookkeeper client", ie);
        }

        // Stats
        StatsLogger segmentsLogger = statsLogger.scope("logsegments");
        if (null == forceGetListStat) {
            forceGetListStat = segmentsLogger.getOpStatsLogger("force_get_list");
        }
        if (null == getListStat) {
            getListStat = segmentsLogger.getOpStatsLogger("get_list");
        }
        if (null == getFilteredListStat) {
            getFilteredListStat = segmentsLogger.getOpStatsLogger("get_filtered_list");
        }
        if (null == getFullListStat) {
            getFullListStat = segmentsLogger.getOpStatsLogger("get_full_list");
        }
        if (null == getInprogressSegmentStat) {
            getInprogressSegmentStat = segmentsLogger.getOpStatsLogger("get_inprogress_segment");
        }
        if (null == getCompletedSegmentStat) {
            getCompletedSegmentStat = segmentsLogger.getOpStatsLogger("get_completed_segment");
        }
        if (null == negativeGetInprogressSegmentStat) {
            negativeGetInprogressSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_inprogress_segment");
        }
        if (null == negativeGetCompletedSegmentStat) {
            negativeGetCompletedSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_completed_segment");
        }
        if (null == recoverLastEntryStats) {
            recoverLastEntryStats = segmentsLogger.getOpStatsLogger("recover_last_entry");
        }
        if (null == recoverScannedEntriesStats) {
            recoverScannedEntriesStats = segmentsLogger.getOpStatsLogger("recover_scanned_entries");
        }
    }

    protected void registerListener(LogSegmentListener listener) {
        listeners.add(listener);
    }

    protected void unregisterListener(LogSegmentListener listener) {
        listeners.remove(listener);
    }

    protected void notifyUpdatedLogSegments(List<LogSegmentLedgerMetadata> segments) {
        for (LogSegmentListener listener : listeners) {
            List<LogSegmentLedgerMetadata> listToReturn =
                    new ArrayList<LogSegmentLedgerMetadata>(segments);
            Collections.sort(listToReturn, LogSegmentLedgerMetadata.DESC_COMPARATOR);
            listener.onSegmentsUpdated(listToReturn);
        }
    }

    protected void scheduleGetAllLedgersTaskIfNeeded() {
        if (isFullListFetched.get()) {
            return;
        }
        asyncGetLedgerListInternal(LogSegmentLedgerMetadata.COMPARATOR, LogSegmentFilter.DEFAULT_FILTER,
                null, new NOPGetLedgersCallback(getFullyQualifiedName()));
    }

    protected void scheduleGetLedgersTask(boolean watch, boolean allowEmpty) {
        if (!watch) {
            ledgerListWatchSet.set(true);
        }
        LOG.info("Scheduling get ledgers task for {}, watch = {}.", getFullyQualifiedName(), watch);
        firstGetLedgersTask = new SyncGetLedgersCallback(getFullyQualifiedName(), allowEmpty);
        asyncGetLedgerListInternal(LogSegmentLedgerMetadata.COMPARATOR, filter,
                                   watch ? this : null, firstGetLedgersTask);
        LOG.info("Scheduled get ledgers task for {}, watch = {}.", getFullyQualifiedName(), watch);
    }

    protected void waitFirstGetLedgersTaskToFinish() throws IOException {
        SyncGetLedgersCallback task = firstGetLedgersTask;
        if (null != task) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Wait first getting ledgers task to finish for {}.", getFullyQualifiedName());
            }
            task.waitForFinish();
        }
    }

    public LogRecordWithDLSN getLastLogRecord(boolean recover, boolean includeEndOfStream) throws IOException {
        checkLogStreamExists();
        List<LogSegmentLedgerMetadata> ledgerList = getFullLedgerListDesc(true, true);

        for (LogSegmentLedgerMetadata metadata: ledgerList) {
            LogRecordWithDLSN record = recoverLastRecordInLedger(metadata, false, recover, false, includeEndOfStream);

            if (null != record) {
                assert(!record.isControl());
                LOG.debug("{} getLastLogRecord Returned {}", getFullyQualifiedName(), record);
                return record;
            }
        }

        throw new LogEmptyException("Log " + getFullyQualifiedName() + " has no records");
    }

    public long getLastTxId(boolean recover,
                            boolean includeEndOfStream) throws IOException {
        checkLogStreamExists();
        return getLastLogRecord(recover, includeEndOfStream).getTransactionId();
    }

    public DLSN getLastDLSN(boolean recover,
                            boolean includeEndOfStream) throws IOException {
        checkLogStreamExists();
        return getLastLogRecord(recover, includeEndOfStream).getDlsn();
    }

    public long getLogRecordCount() throws IOException {
        try {
            checkLogStreamExists();
        } catch (LogEmptyException exc) {
            return 0;
        }

        List<LogSegmentLedgerMetadata> ledgerList = getFullLedgerList(true, false);
        long count = 0;
        for (LogSegmentLedgerMetadata l : ledgerList) {
            if (l.isInProgress()) {
                LogRecord record = recoverLastRecordInLedger(l, false, false, false, false);
                if (null != record) {
                    count += record.getCount();
                }
            } else {
                count += l.getRecordCount();
            }
        }
        return count;
    }

    public long getFirstTxId() throws IOException {
        checkLogStreamExists();
        List<LogSegmentLedgerMetadata> ledgerList = getFullLedgerList(true, true);

        // The ledger list should at least have one element
        // First TxId is populated even for in progress ledgers
        return ledgerList.get(0).getFirstTxId();
    }

    private void checkLogStreamExists() throws IOException {
        try {
            if (null == zooKeeperClient.get().exists(ledgerPath, false)) {
                throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while reading {}", ledgerPath, ie);
            throw new DLInterruptedException("Interrupted while checking " + ledgerPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading {} entry in zookeeper", ledgerPath, ke);
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
        }
    }

    public long getTxIdNotLaterThan(long thresholdTxId)
        throws IOException {
        checkLogStreamExists();
        LedgerHandleCache handleCachePriv = new LedgerHandleCache(bookKeeperClient, digestpw);
        LedgerDataAccessor ledgerDataAccessorPriv = new LedgerDataAccessor(handleCachePriv, getFullyQualifiedName(), statsLogger);
        List<LogSegmentLedgerMetadata> ledgerListDesc = getFullLedgerListDesc(true, false);
        for (LogSegmentLedgerMetadata l : ledgerListDesc) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Inspecting Ledger: {}", l);
            }
            if (thresholdTxId < l.getFirstTxId()) {
                continue;
            }

            if (l.isInProgress()) {
                try {
                    long lastTxId = readLastTxIdInLedger(l, false).getLeft();
                    if ((lastTxId != DistributedLogConstants.EMPTY_LEDGER_TX_ID) &&
                        (lastTxId != DistributedLogConstants.INVALID_TXID) &&
                        (lastTxId < thresholdTxId)) {
                        return lastTxId;
                    }
                } catch (DLInterruptedException ie) {
                    throw ie;
                } catch (IOException exc) {
                    LOG.info("Optimistic Transaction Id recovery failed.", exc);
                }
            }

            try {
                LedgerDescriptor ledgerDescriptor = handleCachePriv.openLedger(l, !l.isInProgress());
                BKPerStreamLogReader s
                    = new BKPerStreamLogReader(this, ledgerDescriptor, l, 0,
                    ledgerDataAccessorPriv, false, statsLogger);

                LogRecord prevRecord = null;
                LogRecord currRecord = s.readOp(false);
                while ((null != currRecord) && (currRecord.getTransactionId() <= thresholdTxId)) {
                    prevRecord = currRecord;
                    currRecord = s.readOp(false);
                }

                if (null != prevRecord) {
                    if (prevRecord.getTransactionId() < 0) {
                        LOG.info("getTxIdNotLaterThan returned negative value {} for input {}", prevRecord.getTransactionId(), thresholdTxId);
                    }
                    return prevRecord.getTransactionId();
                }

            } catch (BKException e) {
                throw new IOException("Could not open ledger for " + thresholdTxId, e);
            }

        }

        if (ledgerListDesc.size() == 0) {
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
        }

        throw new AlreadyTruncatedTransactionException("Records prior to " + thresholdTxId +
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
        bookKeeperClient.release();
        zooKeeperClient.close();
    }

    /**
     * Find the id of the last edit log transaction written to a edit log
     * ledger.
     */
    protected Pair<Long, DLSN> readLastTxIdInLedger(LogSegmentLedgerMetadata l, boolean forwardReading) throws IOException {
        LogRecordWithDLSN record = recoverLastRecordInLedger(l, forwardReading, false, false, true);

        if (null == record) {
            return Pair.of(DistributedLogConstants.EMPTY_LEDGER_TX_ID, DLSN.InvalidDLSN);
        }
        else {
            return Pair.of(record.getTransactionId(), record.getDlsn());
        }
    }

    /**
     * Find the id of the last edit log transaction written to a edit log
     * ledger.
     */
    protected LogRecordWithDLSN recoverLastRecordInLedger(LogSegmentLedgerMetadata l,
                                                          boolean forwardReading,
                                                          boolean fence,
                                                          boolean includeControl,
                                                          boolean includeEndOfStream)
        throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            LogRecordWithDLSN record = doRecoverLastRecordInLedger(l, forwardReading, fence, includeControl, includeEndOfStream);
            success = true;
            return record;
        } finally {
            if (success) {
                recoverLastEntryStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                recoverLastEntryStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    protected LogRecordWithDLSN doRecoverLastRecordInLedger(LogSegmentLedgerMetadata l,
                                                            boolean forwardReading,
                                                            boolean fence,
                                                            boolean includeControl,
                                                            boolean includeEndOfStream)
        throws IOException {
        LogRecordWithDLSN lastRecord;
        Throwable exceptionEncountered = null;
        try {
            LedgerHandleCache handleCachePriv = new LedgerHandleCache(bookKeeperClient, digestpw);
            LedgerDataAccessor ledgerDataAccessorPriv = new LedgerDataAccessor(handleCachePriv, getFullyQualifiedName(), statsLogger);
            boolean trySmallLedger = true;
            LedgerDescriptor ledgerDescriptor = handleCachePriv.openLedger(l, fence);
            long scanStartPoint = handleCachePriv.getLastAddConfirmed(ledgerDescriptor);
            // since we piggyback last add confirmed, so we should stop reading at lastAddConfirmed position
            // if forwardReading is disabled.
            final long lastAddConfirmed = scanStartPoint;

            if (scanStartPoint < 0) {
                LOG.debug("Ledger is empty {}", l.getLedgerId());
                // Ledger is empty
                return null;
            }

            if (fence) {
                LOG.debug("{} Open With Recovery Last Add Confirmed {}", getFullyQualifiedName(), scanStartPoint);
            } else {
                LOG.debug("{} Open No Recovery Last Add Confirmed {}", getFullyQualifiedName(), scanStartPoint);
                if (scanStartPoint > DistributedLogConstants.SMALL_LEDGER_THRESHOLD) {
                    scanStartPoint -= DistributedLogConstants.SMALL_LEDGER_THRESHOLD;
                    trySmallLedger = false;
                }
            }

            int numRecordsScanned = 0;
            while (true) {
                BKPerStreamLogReader in
                    = new BKPerStreamLogReader(this, ledgerDescriptor, l, scanStartPoint,
                        ledgerDataAccessorPriv, includeControl, statsLogger);

                lastRecord = null;
                try {
                    LogRecordWithDLSN record = in.readOp(false);
                    while (record != null) {
                        ++numRecordsScanned;
                        if (!forwardReading && record.getDlsn().getEntryId() > lastAddConfirmed) {
                            // we should stop forward reading at current lastAddConfirmed.
                            break;
                        }
                        if ((null == lastRecord
                            || record.getDlsn().compareTo(lastRecord.getDlsn()) > 0) &&
                            (includeEndOfStream || !record.isEndOfStream())) {
                            lastRecord = record;
                        }
                        record = in.readOp(false);
                    }
                } catch (DLInterruptedException die) {
                    throw die;
                } catch (IOException exc) {
                    LOG.info("Reading beyond flush point", exc);
                    exceptionEncountered = exc;
                } finally {
                    in.close();
                }

                if (0 == scanStartPoint) {
                    break;
                } else if (null != lastRecord) {
                    break;
                } else {
                    // Retry from a different point in the ledger
                    exceptionEncountered = null;

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
            recoverScannedEntriesStats.registerSuccessfulEvent(numRecordsScanned);
        } catch (BKException e) {
            throw new IOException("Exception retrieving last tx id for ledger " + l, e);
        }

        // If there was an exception while reading the last record, we cant rely on the value
        // so we must throw the error to the caller
        if (null != exceptionEncountered) {
            throw new IOException("Exception while retrieving last log record", exceptionEncountered);
        }

        if (includeControl && (null == lastRecord)) {
            throw new IOException("Exception while retrieving last log record");
        }

        assert((null == lastRecord) || includeControl || !lastRecord.isControl());
        return lastRecord;
    }

    protected void setLastLedgerRollingTimeMillis(long rollingTimeMillis) {
        if (lastLedgerRollingTimeMillis < rollingTimeMillis) {
            lastLedgerRollingTimeMillis = rollingTimeMillis;
        }
    }

    public String getFullyQualifiedName() {
        return String.format("%s:%s", name, streamIdentifier);
    }

    // Ledgers Related Functions
    // ***Note***
    // Caching of log segment metadata assumes that the data contained in the ZNodes for individual
    // log segments is never updated after creation i.e we never call setData. A log segment
    // is finalized by creating a new ZNode and deleting the in progress node. This code will have
    // to change if we change the behavior

    private List<LogSegmentLedgerMetadata> getCachedLedgerList(Comparator comparator, LogSegmentFilter segmentFilter) {
        List<LogSegmentLedgerMetadata> segmentsToReturn;
        synchronized (logSegments) {
            segmentsToReturn = new ArrayList<LogSegmentLedgerMetadata>(logSegments.size());
            Collection<String> segmentNamesFiltered = segmentFilter.filter(logSegments.keySet());
            for (String name : segmentNamesFiltered) {
                segmentsToReturn.add(logSegments.get(name));
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Cached log segments : {}", segmentsToReturn);
            }
        }
        Collections.sort(segmentsToReturn, comparator);
        return segmentsToReturn;
    }

    protected List<LogSegmentLedgerMetadata> getCachedFullLedgerList(Comparator comparator) {
        return getCachedLedgerList(comparator, LogSegmentFilter.DEFAULT_FILTER);
    }

    protected List<LogSegmentLedgerMetadata> getFullLedgerList(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, true, LogSegmentLedgerMetadata.COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentLedgerMetadata> getFullLedgerListDesc(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, true, LogSegmentLedgerMetadata.DESC_COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentLedgerMetadata> getFilteredLedgerList(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, false, LogSegmentLedgerMetadata.COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentLedgerMetadata> getFilteredLedgerListDesc(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, false, LogSegmentLedgerMetadata.DESC_COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentLedgerMetadata> getLedgerList(boolean forceFetch, boolean fetchFullList,
                                                           Comparator comparator, boolean throwOnEmpty)
            throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            List<LogSegmentLedgerMetadata> segments =
                    doGetLedgerList(forceFetch, fetchFullList, comparator, throwOnEmpty);
            success = true;
            return segments;
        } finally {
            OpStatsLogger statsLogger = fetchFullList ? getFullListStat : getFilteredListStat;
            if (success) {
                statsLogger.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                statsLogger.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    private List<LogSegmentLedgerMetadata> doGetLedgerList(boolean forceFetch, boolean fetchFullList,
                                                           Comparator comparator, boolean throwOnEmpty)
        throws IOException {
        if (fetchFullList) {
            if (forceFetch || !isFullListFetched.get()) {
                return forceGetLedgerList(comparator, LogSegmentFilter.DEFAULT_FILTER, throwOnEmpty);
            } else {
                return getCachedFullLedgerList(comparator);
            }
        } else {
            if (forceFetch) {
                return forceGetLedgerList(comparator, filter, throwOnEmpty);
            } else {
                if(!ledgerListWatchSet.get()) {
                    scheduleGetLedgersTask(true, true);
                }
                waitFirstGetLedgersTaskToFinish();
                return getCachedLedgerList(comparator, filter);
            }
        }
    }

    protected LogSegmentLedgerMetadata removeLogSegmentFromCache(String name) {
        synchronized (logSegments) {
            LogSegmentLedgerMetadata metadata = logSegments.remove(name);
            if (null != metadata) {
                lid2LogSegments.remove(metadata.getLedgerId(), metadata);
                LOG.debug("Removed log segment ({} : {}) from cache.", name, metadata);
            }
            return metadata;
        }
    }

    /**
     * Get a list of all segments in the journal.
     */
    protected List<LogSegmentLedgerMetadata> forceGetLedgerList(final Comparator comparator,
                                                                final LogSegmentFilter segmentFilter,
                                                                boolean throwOnEmpty) throws IOException {
        final List<LogSegmentLedgerMetadata> ledgers = new ArrayList<LogSegmentLedgerMetadata>();
        final AtomicInteger result = new AtomicInteger(-1);
        final CountDownLatch latch = new CountDownLatch(1);
        int retryCount = 5;
        int backOff = conf.getReadAheadWaitTime();
        do {
            ledgers.clear();
            Stopwatch stopwatch = new Stopwatch().start();
            asyncGetLedgerListInternal(comparator, segmentFilter, null, new GenericCallback<List<LogSegmentLedgerMetadata>>() {
                @Override
                public void operationComplete(int rc, List<LogSegmentLedgerMetadata> logSegmentLedgerMetadatas) {
                    result.set(rc);
                    if (KeeperException.Code.OK.intValue() == rc) {
                        ledgers.addAll(logSegmentLedgerMetadatas);
                    } else {
                        LOG.error("Failed to get ledger list for {} : with error {}", getFullyQualifiedName(), rc);
                    }
                    latch.countDown();
                }
            });
            try {
                latch.await();
            } catch (InterruptedException e) {
                forceGetListStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                throw new DLInterruptedException("Interrupted on reading ledger list from zkfor " + getFullyQualifiedName(), e);
            }
            long elapsedMicros = stopwatch.stop().elapsed(TimeUnit.MICROSECONDS);

            KeeperException.Code rc = KeeperException.Code.get(result.get());
            if (rc == KeeperException.Code.OK) {
                forceGetListStat.registerSuccessfulEvent(elapsedMicros);
                break;
            // NONODE exception is possible in two cases
            // 1. A log segment was deleted by truncation between the call to getChildren and read
            // attempt on the znode corresponding to the segment
            // 2. In progress segment has been completed => inprogress ZNode does not exist
            } else if((rc != KeeperException.Code.NONODE) &&
                (rc != KeeperException.Code.SESSIONEXPIRED) &&
                (rc != KeeperException.Code.CONNECTIONLOSS) &&
                (rc != KeeperException.Code.SESSIONMOVED)) {
                forceGetListStat.registerFailedEvent(elapsedMicros);
                throw new IOException("ZK Exception "+ rc +" reading ledger list for " + getFullyQualifiedName());
            }
            forceGetListStat.registerFailedEvent(elapsedMicros);

            retryCount--;
            try {
                Thread.sleep(backOff);
            } catch (InterruptedException exc) {
                //start a new iteration
            }
        } while (retryCount > 0);

        if (throwOnEmpty && ledgers.isEmpty()) {
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
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
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Added log segment ({} : {}) to cache.", name, metadata);
                }
            }
            LogSegmentLedgerMetadata oldMetadata = lid2LogSegments.remove(metadata.getLedgerId());
            if (null == oldMetadata) {
                lid2LogSegments.put(metadata.getLedgerId(), metadata);
            } else {
                if (oldMetadata.isInProgress() && !metadata.isInProgress()) {
                    lid2LogSegments.put(metadata.getLedgerId(), metadata);
                } else {
                    lid2LogSegments.put(oldMetadata.getLedgerId(), oldMetadata);
                }
            }
        }
        // update the last ledger rolling time
        if (!metadata.isInProgress() && (lastLedgerRollingTimeMillis < metadata.getCompletionTime())) {
            lastLedgerRollingTimeMillis = metadata.getCompletionTime();
        }

        if (reportGetSegmentStats) {
            // update stats
            long ts = System.currentTimeMillis();
            if (metadata.isInProgress()) {
                // as we used timestamp as start tx id we could take it as start time
                // NOTE: it is a hack here.
                long elapsedMillis = ts - metadata.getFirstTxId();
                long elapsedMicroSec = TimeUnit.MILLISECONDS.toMicros(elapsedMillis);
                if (elapsedMicroSec > 0) {
                    if (elapsedMillis > DistributedLogConstants.LATENCY_WARN_THRESHOLD_IN_MILLIS) {
                        LOG.warn("{} received inprogress log segment in {} millis: {}",
                                 new Object[] { getFullyQualifiedName(), elapsedMillis, metadata });
                    }
                    getInprogressSegmentStat.registerSuccessfulEvent(elapsedMicroSec);
                } else {
                    negativeGetInprogressSegmentStat.registerSuccessfulEvent(-elapsedMicroSec);
                }
            } else {
                long elapsedMillis = ts - metadata.getCompletionTime();
                long elapsedMicroSec = TimeUnit.MILLISECONDS.toMicros(elapsedMillis);
                if (elapsedMicroSec > 0) {
                    if (elapsedMillis > DistributedLogConstants.LATENCY_WARN_THRESHOLD_IN_MILLIS) {
                        LOG.warn("{} received completed log segment in {} millis : {}",
                                 new Object[] { getFullyQualifiedName(), elapsedMillis, metadata });
                    }
                    getCompletedSegmentStat.registerSuccessfulEvent(elapsedMicroSec);
                } else {
                    negativeGetCompletedSegmentStat.registerSuccessfulEvent(-elapsedMicroSec);
                }
            }
        }
    }

    protected LogSegmentLedgerMetadata readLogSegmentFromCache(String name) {
        synchronized (logSegments) {
            return logSegments.get(name);
        }
    }

    protected LogSegmentLedgerMetadata removeLogSegmentToCache(String name) {
        synchronized (logSegments) {
            LogSegmentLedgerMetadata metadata = logSegments.remove(name);
            if (null != metadata) {
                lid2LogSegments.remove(metadata.getLedgerId(), metadata);
                LOG.debug("Removed log segment ({} : {}) from cache.", name, metadata);
            }
            return metadata;
        }
    }

    protected void asyncGetLedgerList(final Comparator comparator, Watcher watcher,
                                      final GenericCallback<List<LogSegmentLedgerMetadata>> callback) {
        asyncGetLedgerListInternal(comparator, filter, watcher, callback);
    }

    private void asyncGetLedgerListInternal(final Comparator comparator,
                                            final LogSegmentFilter segmentFilter,
                                            final Watcher watcher,
                                            final GenericCallback<List<LogSegmentLedgerMetadata>> finalCallback) {
        final Stopwatch stopwatch = new Stopwatch().start();
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Async getting ledger list for {}.", getFullyQualifiedName());
            }
            final GenericCallback<List<LogSegmentLedgerMetadata>> callback = new GenericCallback<List<LogSegmentLedgerMetadata>>() {
                @Override
                public void operationComplete(int rc, List<LogSegmentLedgerMetadata> result) {
                    long elapsedMicros = stopwatch.stop().elapsed(TimeUnit.MICROSECONDS);
                    if (BKException.Code.OK != rc) {
                        getListStat.registerFailedEvent(elapsedMicros);
                    } else {
                        if (LogSegmentFilter.DEFAULT_FILTER == segmentFilter) {
                            isFullListFetched.set(true);
                        }
                        getListStat.registerSuccessfulEvent(elapsedMicros);
                    }
                    finalCallback.operationComplete(rc, result);
                }
            };
            zooKeeperClient.get().getChildren(ledgerPath, watcher, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(final int rc, final String path, final Object ctx, final List<String> children, final Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        callback.operationComplete(rc, null);
                        return;
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Got ledger list from {} : {}", ledgerPath, children);
                    }

                    ledgerListWatchSet.set(true);
                    Set<String> segmentsReceived = new HashSet<String>();
                    segmentsReceived.addAll(segmentFilter.filter(children));
                    Set<String> segmentsAdded;
                    Set<String> segmentsRemoved;
                    synchronized (logSegments) {
                        Set<String> segmentsCached = logSegments.keySet();
                        segmentsAdded = Sets.difference(segmentsReceived, segmentsCached).immutableCopy();
                        segmentsRemoved = Sets.difference(segmentsCached, segmentsReceived).immutableCopy();
                        for (String s : segmentsRemoved) {
                            LogSegmentLedgerMetadata segmentMetadata = removeLogSegmentToCache(s);
                            LOG.debug("Removed log segment {} from cache : {}.", s, segmentMetadata);
                        }
                    }

                    if (segmentsAdded.isEmpty()) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("No segments added for {}.", getFullyQualifiedName());
                        }

                        List<LogSegmentLedgerMetadata> segmentList = getCachedLedgerList(comparator, segmentFilter);
                        callback.operationComplete(KeeperException.Code.OK.intValue(), segmentList);
                        notifyUpdatedLogSegments(segmentList);
                        if (!segmentsRemoved.isEmpty()) {
                            notifyOnOperationComplete();
                        }
                        return;
                    }

                    final AtomicInteger numChildren = new AtomicInteger(segmentsAdded.size());
                    final AtomicInteger numFailures = new AtomicInteger(0);
                    for (final String segment: segmentsAdded) {
                        LogSegmentLedgerMetadata.read(zooKeeperClient,
                            ledgerPath + "/" + segment, conf.getDLLedgerMetadataLayoutVersion(),
                            new GenericCallback<LogSegmentLedgerMetadata>() {
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
                                    }
                                    if (0 == numChildren.decrementAndGet() && numFailures.get() == 0) {
                                        List<LogSegmentLedgerMetadata> segmentList =
                                            getCachedLedgerList(comparator, segmentFilter);
                                        callback.operationComplete(KeeperException.Code.OK.intValue(), segmentList);
                                        notifyUpdatedLogSegments(segmentList);
                                        notifyOnOperationComplete();
                                    }
                                }
                            });
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            getListStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            finalCallback.operationComplete(BKException.Code.ZKException, null);
        } catch (InterruptedException e) {
            getListStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            finalCallback.operationComplete(BKException.Code.InterruptedException, null);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (Watcher.Event.EventType.None.equals(event.getType())) {
            if (event.getState() == Watcher.Event.KeeperState.Expired) {
                // if the watcher is expired
                executorService.schedule(new WatcherGetLedgersCallback(getFullyQualifiedName()),
                        conf.getZKRetryBackoffStartMillis(), TimeUnit.MILLISECONDS);
            }
        } else if (Watcher.Event.EventType.NodeChildrenChanged.equals(event.getType())) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("LogSegments Changed under {}.", getFullyQualifiedName());
            }
            asyncGetLedgerListInternal(LogSegmentLedgerMetadata.COMPARATOR, filter,
                                       this, new WatcherGetLedgersCallback(getFullyQualifiedName()));
        }
    }

    void notifyOnOperationComplete() {
        if (null != notification) {
            notification.notifyOnOperationComplete();
        }
    }

}
