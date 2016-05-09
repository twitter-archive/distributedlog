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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.exceptions.MetadataException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadata;
import com.twitter.distributedlog.io.AsyncAbortable;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.logsegment.LogSegmentCache;
import com.twitter.distributedlog.logsegment.LogSegmentFilter;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The base class about log handler on managing log segments.
 *
 * <h3>Metrics</h3>
 * The log handler is a base class on managing log segments. so all the metrics
 * here are related to log segments retrieval and exposed under `logsegments`.
 * These metrics are all OpStats, in the format of <code>`scope`/logsegments/`op`</code>.
 * <p>
 * Those operations are:
 * <ul>
 * <li>force_get_list: force to get the list of log segments.
 * <li>get_list: get the list of the log segments. it might just retrieve from
 * local log segment cache.
 * <li>get_filtered_list: get the filtered list of log segments.
 * <li>get_full_list: get the full list of log segments.
 * <li>get_inprogress_segment: time between the inprogress log segment created and
 * the handler read it.
 * <li>get_completed_segment: time between a log segment is turned to completed and
 * the handler read it.
 * <li>negative_get_inprogress_segment: record the negative values for `get_inprogress_segment`.
 * <li>negative_get_completed_segment: record the negative values for `get_completed_segment`.
 * <li>recover_last_entry: recovering last entry from a log segment
 * <li>recover_scanned_entries: the number of entries that are scanned during recovering.
 * </ul>
 * @see BKLogWriteHandler
 * @see BKLogReadHandler
 */
public abstract class BKLogHandler implements Watcher, AsyncCloseable, AsyncAbortable {
    static final Logger LOG = LoggerFactory.getLogger(BKLogHandler.class);

    private static final int LAYOUT_VERSION = -1;

    protected final ZKLogMetadata logMetadata;
    protected final DistributedLogConfiguration conf;
    protected final ZooKeeperClient zooKeeperClient;
    protected final BookKeeperClient bookKeeperClient;
    protected final LogSegmentMetadataStore metadataStore;
    protected final int firstNumEntriesPerReadLastRecordScan;
    protected final int maxNumEntriesPerReadLastRecordScan;
    protected volatile long lastLedgerRollingTimeMillis = -1;
    protected final OrderedScheduler scheduler;
    protected final StatsLogger statsLogger;
    protected final AlertStatsLogger alertStatsLogger;
    private final AtomicBoolean ledgerListWatchSet = new AtomicBoolean(false);
    private final AtomicBoolean isFullListFetched = new AtomicBoolean(false);
    protected volatile boolean reportGetSegmentStats = false;
    private final String lockClientId;
    protected final AtomicReference<IOException> metadataException = new AtomicReference<IOException>(null);

    // listener
    protected final CopyOnWriteArraySet<LogSegmentListener> listeners =
            new CopyOnWriteArraySet<LogSegmentListener>();

    // Maintain the list of ledgers
    protected final LogSegmentCache logSegmentCache;
    protected volatile SyncGetLedgersCallback firstGetLedgersTask = null;

    protected final AsyncNotification notification;
    // log segment filter
    protected final LogSegmentFilter filter;

    // zookeeper children watcher
    private final Watcher getChildrenWatcher;

    // trace
    protected final long metadataLatencyWarnThresholdMillis;

    // Stats
    private final OpStatsLogger forceGetListStat;
    private final OpStatsLogger getListStat;
    private final OpStatsLogger getFilteredListStat;
    private final OpStatsLogger getFullListStat;
    private final OpStatsLogger getInprogressSegmentStat;
    private final OpStatsLogger getCompletedSegmentStat;
    private final OpStatsLogger negativeGetInprogressSegmentStat;
    private final OpStatsLogger negativeGetCompletedSegmentStat;
    private final OpStatsLogger recoverLastEntryStats;
    private final OpStatsLogger recoverScannedEntriesStats;

    static class SyncGetLedgersCallback implements GenericCallback<List<LogSegmentMetadata>> {

        final String path;
        final boolean allowEmpty;
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final Promise<List<LogSegmentMetadata>> promise =
                new Promise<List<LogSegmentMetadata>>();

        int rc = KeeperException.Code.APIERROR.intValue();

        SyncGetLedgersCallback(String path, boolean allowEmpty) {
            this.path = path;
            this.allowEmpty = allowEmpty;
        }

        @Override
        public void operationComplete(int rc, List<LogSegmentMetadata> logSegmentMetadatas) {
            this.rc = rc;
            if (KeeperException.Code.OK.intValue() == rc) {
                LOG.debug("Updated ledgers list for {} : {}", path, logSegmentMetadatas);
                promise.setValue(logSegmentMetadatas);
            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                if (allowEmpty) {
                    promise.setValue(new ArrayList<LogSegmentMetadata>(0));
                } else {
                    promise.setException(new LogNotFoundException("Log " + path + " is not found"));
                }
            } else {
                promise.setException(new MetadataException("Error getting ledgers list for " + path));
            }
            countDownLatch.countDown();
        }

        void waitForFinish() throws IOException {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new DLInterruptedException("Interrupted on getting ledgers list for " + path, e);
            }
            if (KeeperException.Code.OK.intValue() != rc) {
                if (KeeperException.Code.NONODE.intValue() == rc) {
                    if (!allowEmpty) {
                        throw new LogNotFoundException("Log " + path + " is not found");
                    }
                } else {
                    throw new MetadataException("Error getting ledgers list for " + path);
                }
            }
        }
    }

    static class NOPGetLedgersCallback implements GenericCallback<List<LogSegmentMetadata>> {

        final String path;

        NOPGetLedgersCallback(String path) {
            this.path = path;
        }

        @Override
        public void operationComplete(int rc, List<LogSegmentMetadata> logSegmentMetadatas) {
            if (KeeperException.Code.OK.intValue() == rc) {
                LOG.debug("Updated ledgers list : {}", path, logSegmentMetadatas);
            }
        }
    }

    class WatcherGetLedgersCallback implements GenericCallback<List<LogSegmentMetadata>>, Runnable {

        final String path;

        WatcherGetLedgersCallback(String path) {
            this.path = path;
        }

        @Override
        public void operationComplete(int rc, List<LogSegmentMetadata> logSegmentMetadatas) {
            if (KeeperException.Code.OK.intValue() == rc) {
                LOG.debug("Updated ledgers list {} : {}", path, logSegmentMetadatas);
            } else {
                scheduler.schedule(this, conf.getZKRetryBackoffMaxMillis(), TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void run() {
            asyncGetLedgerListWithRetries(LogSegmentMetadata.COMPARATOR, filter, getChildrenWatcher, this);
        }
    }

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogHandler(ZKLogMetadata metadata,
                 DistributedLogConfiguration conf,
                 ZooKeeperClientBuilder zkcBuilder,
                 BookKeeperClientBuilder bkcBuilder,
                 LogSegmentMetadataStore metadataStore,
                 OrderedScheduler scheduler,
                 StatsLogger statsLogger,
                 AlertStatsLogger alertStatsLogger,
                 AsyncNotification notification,
                 LogSegmentFilter filter,
                 String lockClientId) {
        Preconditions.checkNotNull(zkcBuilder);
        Preconditions.checkNotNull(bkcBuilder);
        this.logMetadata = metadata;
        this.conf = conf;
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;
        this.alertStatsLogger = alertStatsLogger;
        this.notification = notification;
        this.filter = filter;
        this.logSegmentCache = new LogSegmentCache(metadata.getLogName());

        firstNumEntriesPerReadLastRecordScan = conf.getFirstNumEntriesPerReadLastRecordScan();
        maxNumEntriesPerReadLastRecordScan = conf.getMaxNumEntriesPerReadLastRecordScan();
        this.zooKeeperClient = zkcBuilder.build();
        LOG.debug("Using ZK Path {}", logMetadata.getLogRootPath());
        this.bookKeeperClient = bkcBuilder.build();
        this.metadataStore = metadataStore;

        if (lockClientId.equals(DistributedLogConstants.UNKNOWN_CLIENT_ID)) {
            this.lockClientId = getHostIpLockClientId();
        } else {
            this.lockClientId = lockClientId;
        }

        this.getChildrenWatcher = this.zooKeeperClient.getWatcherManager()
                .registerChildWatcher(logMetadata.getLogSegmentsPath(), this);

        // Traces
        this.metadataLatencyWarnThresholdMillis = conf.getMetadataLatencyWarnThresholdMillis();

        // Stats
        StatsLogger segmentsLogger = statsLogger.scope("logsegments");
        forceGetListStat = segmentsLogger.getOpStatsLogger("force_get_list");
        getListStat = segmentsLogger.getOpStatsLogger("get_list");
        getFilteredListStat = segmentsLogger.getOpStatsLogger("get_filtered_list");
        getFullListStat = segmentsLogger.getOpStatsLogger("get_full_list");
        getInprogressSegmentStat = segmentsLogger.getOpStatsLogger("get_inprogress_segment");
        getCompletedSegmentStat = segmentsLogger.getOpStatsLogger("get_completed_segment");
        negativeGetInprogressSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_inprogress_segment");
        negativeGetCompletedSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_completed_segment");
        recoverLastEntryStats = segmentsLogger.getOpStatsLogger("recover_last_entry");
        recoverScannedEntriesStats = segmentsLogger.getOpStatsLogger("recover_scanned_entries");
    }

    BKLogHandler checkMetadataException() throws IOException {
        if (null != metadataException.get()) {
            throw metadataException.get();
        }
        return this;
    }

    public void reportGetSegmentStats(boolean enabled) {
        this.reportGetSegmentStats = enabled;
    }

    public String getLockClientId() {
        return lockClientId;
    }

    private String getHostIpLockClientId() {
        try {
            return InetAddress.getLocalHost().toString();
        } catch(Exception ex) {
            return DistributedLogConstants.UNKNOWN_CLIENT_ID;
        }
    }

    protected void registerListener(LogSegmentListener listener) {
        listeners.add(listener);
    }

    protected void unregisterListener(LogSegmentListener listener) {
        listeners.remove(listener);
    }

    protected void notifyUpdatedLogSegments(List<LogSegmentMetadata> segments) {
        for (LogSegmentListener listener : listeners) {
            List<LogSegmentMetadata> listToReturn =
                    new ArrayList<LogSegmentMetadata>(segments);
            Collections.sort(listToReturn, LogSegmentMetadata.DESC_COMPARATOR);
            listener.onSegmentsUpdated(listToReturn);
        }
    }

    protected void scheduleGetAllLedgersTaskIfNeeded() {
        if (isFullListFetched.get()) {
            return;
        }
        asyncGetLedgerListWithRetries(LogSegmentMetadata.COMPARATOR, LogSegmentFilter.DEFAULT_FILTER,
                null, new NOPGetLedgersCallback(getFullyQualifiedName()));
    }

    protected void scheduleGetLedgersTask(boolean watch, boolean allowEmpty) {
        if (!watch) {
            ledgerListWatchSet.set(true);
        }
        LOG.info("Scheduling get ledgers task for {}, watch = {}.", getFullyQualifiedName(), watch);
        firstGetLedgersTask = new SyncGetLedgersCallback(getFullyQualifiedName(), allowEmpty);
        asyncGetLedgerListWithRetries(LogSegmentMetadata.COMPARATOR, filter,
                watch ? getChildrenWatcher : null, firstGetLedgersTask);
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

    public Future<LogRecordWithDLSN> asyncGetFirstLogRecord() {
        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
        checkLogStreamExistsAsync().addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                asyncGetFullLedgerList(true, true).addEventListener(new FutureEventListener<List<LogSegmentMetadata>>() {

                    @Override
                    public void onSuccess(List<LogSegmentMetadata> ledgerList) {
                        if (ledgerList.isEmpty()) {
                            promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
                            return;
                        }
                        Future<LogRecordWithDLSN> firstRecord = null;
                        for (LogSegmentMetadata ledger : ledgerList) {
                            if (!ledger.isTruncated() && (ledger.getRecordCount() > 0 || ledger.isInProgress())) {
                                firstRecord = asyncReadFirstUserRecord(ledger, DLSN.InitialDLSN);
                                break;
                            }
                        }
                        if (null != firstRecord) {
                            promise.become(firstRecord);
                        } else {
                            promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                promise.setException(cause);
            }
        });
        return promise;
    }

    public Future<LogRecordWithDLSN> getLastLogRecordAsync(final boolean recover, final boolean includeEndOfStream) {
        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
        checkLogStreamExistsAsync().addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                asyncGetFullLedgerListDesc(true, true).addEventListener(new FutureEventListener<List<LogSegmentMetadata>>() {

                    @Override
                    public void onSuccess(List<LogSegmentMetadata> ledgerList) {
                        if (ledgerList.isEmpty()) {
                            promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
                            return;
                        }
                        asyncGetLastLogRecord(ledgerList.iterator(), promise, recover, false, includeEndOfStream);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                promise.setException(cause);
            }
        });
        return promise;
    }

    private void asyncGetLastLogRecord(final Iterator<LogSegmentMetadata> ledgerIter,
                                       final Promise<LogRecordWithDLSN> promise,
                                       final boolean fence,
                                       final boolean includeControlRecord,
                                       final boolean includeEndOfStream) {
        if (ledgerIter.hasNext()) {
            LogSegmentMetadata metadata = ledgerIter.next();
            asyncReadLastRecord(metadata, fence, includeControlRecord, includeEndOfStream).addEventListener(
                    new FutureEventListener<LogRecordWithDLSN>() {
                        @Override
                        public void onSuccess(LogRecordWithDLSN record) {
                            if (null == record) {
                                asyncGetLastLogRecord(ledgerIter, promise, fence, includeControlRecord, includeEndOfStream);
                            } else {
                                promise.setValue(record);
                            }
                        }

                        @Override
                        public void onFailure(Throwable cause) {
                            promise.setException(cause);
                        }
                    }
            );
        } else {
            promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
        }
    }

    public LogRecordWithDLSN getLastLogRecord(boolean recover, boolean includeEndOfStream) throws IOException {
        checkLogStreamExists();
        List<LogSegmentMetadata> ledgerList = getFullLedgerListDesc(true, true);

        for (LogSegmentMetadata metadata: ledgerList) {
            LogRecordWithDLSN record = recoverLastRecordInLedger(metadata, recover, false, includeEndOfStream);

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
        } catch (LogNotFoundException exc) {
            return 0;
        }

        List<LogSegmentMetadata> ledgerList = getFullLedgerList(true, false);
        long count = 0;
        for (LogSegmentMetadata l : ledgerList) {
            if (l.isInProgress()) {
                LogRecord record = recoverLastRecordInLedger(l, false, false, false);
                if (null != record) {
                    count += record.getLastPositionWithinLogSegment();
                }
            } else {
                count += l.getRecordCount();
            }
        }
        return count;
    }

    private Future<LogRecordWithDLSN> asyncReadFirstUserRecord(LogSegmentMetadata ledger, DLSN beginDLSN) {
        final LedgerHandleCache handleCache =
                LedgerHandleCache.newBuilder().bkc(bookKeeperClient).conf(conf).build();
        return ReadUtils.asyncReadFirstUserRecord(
                getFullyQualifiedName(),
                ledger,
                firstNumEntriesPerReadLastRecordScan,
                maxNumEntriesPerReadLastRecordScan,
                new AtomicInteger(0),
                scheduler,
                handleCache,
                beginDLSN
        ).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
    }

    /**
     * This is a helper method to compactly return the record count between two records, the first denoted by
     * beginDLSN and the second denoted by endPosition. Its up to the caller to ensure that endPosition refers to
     * position in the same ledger as beginDLSN.
     */
    private Future<Long> asyncGetLogRecordCount(LogSegmentMetadata ledger, final DLSN beginDLSN, final long endPosition) {
        return asyncReadFirstUserRecord(ledger, beginDLSN).map(new Function<LogRecordWithDLSN, Long>() {
            public Long apply(final LogRecordWithDLSN beginRecord) {
                long recordCount = 0;
                if (null != beginRecord) {
                    recordCount = endPosition + 1 - beginRecord.getLastPositionWithinLogSegment();
                }
                return recordCount;
            }
        });
    }

    /**
     * Ledger metadata tells us how many records are in each completed segment, but for the first and last segments
     * we may have to crack open the entry and count. For the first entry, we need to do so because beginDLSN may be
     * an interior entry. For the last entry, if it is inprogress, we need to recover it and find the last user
     * entry.
     */
    private Future<Long> asyncGetLogRecordCount(final LogSegmentMetadata ledger, final DLSN beginDLSN) {
        if (ledger.isInProgress() && ledger.isDLSNinThisSegment(beginDLSN)) {
            return asyncReadLastUserRecord(ledger).flatMap(new Function<LogRecordWithDLSN, Future<Long>>() {
                public Future<Long> apply(final LogRecordWithDLSN endRecord) {
                    if (null != endRecord) {
                        return asyncGetLogRecordCount(ledger, beginDLSN, endRecord.getLastPositionWithinLogSegment() /* end position */);
                    } else {
                        return Future.value((long) 0);
                    }
                }
            });
        } else if (ledger.isInProgress()) {
            return asyncReadLastUserRecord(ledger).map(new Function<LogRecordWithDLSN, Long>() {
                public Long apply(final LogRecordWithDLSN endRecord) {
                    if (null != endRecord) {
                        return (long) endRecord.getLastPositionWithinLogSegment();
                    } else {
                        return (long) 0;
                    }
                }
            });
        } else if (ledger.isDLSNinThisSegment(beginDLSN)) {
            return asyncGetLogRecordCount(ledger, beginDLSN, ledger.getRecordCount() /* end position */);
        } else {
            return Future.value((long) ledger.getRecordCount());
        }
    }

    /**
     * Get a count of records between beginDLSN and the end of the stream.
     *
     * @param beginDLSN dlsn marking the start of the range
     * @return the count of records present in the range
     */
    public Future<Long> asyncGetLogRecordCount(final DLSN beginDLSN) {

        return checkLogStreamExistsAsync().flatMap(new Function<Void, Future<Long>>() {
            public Future<Long> apply(Void done) {

                return asyncGetFullLedgerList(true, false).flatMap(new Function<List<LogSegmentMetadata>, Future<Long>>() {
                    public Future<Long> apply(List<LogSegmentMetadata> ledgerList) {

                        List<Future<Long>> futureCounts = new ArrayList<Future<Long>>(ledgerList.size());
                        for (LogSegmentMetadata ledger : ledgerList) {
                            if (ledger.getLogSegmentSequenceNumber() >= beginDLSN.getLogSegmentSequenceNo()) {
                                futureCounts.add(asyncGetLogRecordCount(ledger, beginDLSN));
                            }
                        }
                        return Future.collect(futureCounts).map(new Function<List<Long>, Long>() {
                            public Long apply(List<Long> counts) {
                                return sum(counts);
                            }
                        });
                    }
                });
            }
        });
    }

    private Long sum(List<Long> values) {
        long sum = 0;
        for (Long value : values) {
            sum += value;
        }
        return sum;
    }

    public long getFirstTxId() throws IOException {
        checkLogStreamExists();
        List<LogSegmentMetadata> ledgerList = getFullLedgerList(true, true);

        // The ledger list should at least have one element
        // First TxId is populated even for in progress ledgers
        return ledgerList.get(0).getFirstTxId();
    }

    Future<Void> checkLogStreamExistsAsync() {
        final Promise<Void> promise = new Promise<Void>();
        try {
            final ZooKeeper zk = zooKeeperClient.get();
            zk.sync(logMetadata.getLogSegmentsPath(), new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int syncRc, String path, Object syncCtx) {
                    if (KeeperException.Code.NONODE.intValue() == syncRc) {
                        promise.setException(new LogNotFoundException(
                                String.format("Log %s does not exist or has been deleted", getFullyQualifiedName())));
                        return;
                    } else if (KeeperException.Code.OK.intValue() != syncRc){
                        promise.setException(new ZKException("Error on checking log existence for " + getFullyQualifiedName(),
                                KeeperException.create(KeeperException.Code.get(syncRc))));
                        return;
                    }
                    zk.exists(logMetadata.getLogSegmentsPath(), false, new AsyncCallback.StatCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, Stat stat) {
                            if (KeeperException.Code.OK.intValue() == rc) {
                                promise.setValue(null);
                            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                promise.setException(new LogNotFoundException(String.format("Log %s does not exist or has been deleted", getFullyQualifiedName())));
                            } else {
                                promise.setException(new ZKException("Error on checking log existence for " + getFullyQualifiedName(),
                                        KeeperException.create(KeeperException.Code.get(rc))));
                            }
                        }
                    }, null);
                }
            }, null);

        } catch (InterruptedException ie) {
            LOG.error("Interrupted while reading {}", logMetadata.getLogSegmentsPath(), ie);
            promise.setException(new DLInterruptedException("Interrupted while checking "
                    + logMetadata.getLogSegmentsPath(), ie));
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        }
        return promise;
    }

    private void checkLogStreamExists() throws IOException {
        try {
            if (null == Utils.sync(zooKeeperClient, logMetadata.getLogSegmentsPath())
                    .exists(logMetadata.getLogSegmentsPath(), false)) {
                throw new LogNotFoundException("Log " + getFullyQualifiedName() + " doesn't exist");
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while reading {}", logMetadata.getLogSegmentsPath(), ie);
            throw new DLInterruptedException("Interrupted while checking "
                    + logMetadata.getLogSegmentsPath(), ie);
        } catch (KeeperException ke) {
            LOG.error("Error checking existence for {} : ", logMetadata.getLogSegmentsPath(), ke);
            throw new ZKException("Error checking existence for " + getFullyQualifiedName() + " : ", ke);
        }
    }

    @Override
    public Future<Void> asyncClose() {
        // No-op
        this.zooKeeperClient.getWatcherManager().unregisterChildWatcher(logMetadata.getLogSegmentsPath(), this);
        return Future.Void();
    }

    @Override
    public Future<Void> asyncAbort() {
        return asyncClose();
    }

    /**
     * Find the id of the last edit log transaction written to a edit log
     * ledger.
     */
    protected Pair<Long, DLSN> readLastTxIdInLedger(LogSegmentMetadata l) throws IOException {
        LogRecordWithDLSN record = recoverLastRecordInLedger(l, false, false, true);

        if (null == record) {
            return Pair.of(DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID, DLSN.InvalidDLSN);
        }
        else {
            return Pair.of(record.getTransactionId(), record.getDlsn());
        }
    }

    /**
     * Find the id of the last edit log transaction written to a edit log
     * ledger.
     */
    protected LogRecordWithDLSN recoverLastRecordInLedger(LogSegmentMetadata l,
                                                          boolean fence,
                                                          boolean includeControl,
                                                          boolean includeEndOfStream)
        throws IOException {
        return FutureUtils.result(asyncReadLastRecord(l, fence, includeControl, includeEndOfStream));
    }

    public Future<LogRecordWithDLSN> asyncReadLastUserRecord(final LogSegmentMetadata l) {
        return asyncReadLastRecord(l, false, false, false);
    }

    public Future<LogRecordWithDLSN> asyncReadLastRecord(final LogSegmentMetadata l,
                                                         final boolean fence,
                                                         final boolean includeControl,
                                                         final boolean includeEndOfStream) {
        final AtomicInteger numRecordsScanned = new AtomicInteger(0);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final LedgerHandleCache handleCache =
                LedgerHandleCache.newBuilder().bkc(bookKeeperClient).conf(conf).build();
        return ReadUtils.asyncReadLastRecord(
                getFullyQualifiedName(),
                l,
                fence,
                includeControl,
                includeEndOfStream,
                firstNumEntriesPerReadLastRecordScan,
                maxNumEntriesPerReadLastRecordScan,
                numRecordsScanned,
                scheduler,
                handleCache
        ).addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN value) {
                recoverLastEntryStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                recoverScannedEntriesStats.registerSuccessfulEvent(numRecordsScanned.get());
            }

            @Override
            public void onFailure(Throwable cause) {
                recoverLastEntryStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
    }

    protected void setLastLedgerRollingTimeMillis(long rollingTimeMillis) {
        if (lastLedgerRollingTimeMillis < rollingTimeMillis) {
            lastLedgerRollingTimeMillis = rollingTimeMillis;
        }
    }

    public String getFullyQualifiedName() {
        return logMetadata.getFullyQualifiedName();
    }

    // Ledgers Related Functions
    // ***Note***
    // Get ledger list should go through #getCachedLogSegments as we need to assign start sequence id for inprogress log
    // segment so the reader could generate the right sequence id.

    protected List<LogSegmentMetadata> getCachedLogSegments(Comparator<LogSegmentMetadata> comparator)
        throws UnexpectedException {
        try {
            return logSegmentCache.getLogSegments(comparator);
        } catch (UnexpectedException ue) {
            // the log segments cache went wrong
            LOG.error("Unexpected exception on getting log segments from the cache for stream {}",
                    getFullyQualifiedName(), ue);
            metadataException.compareAndSet(null, ue);
            throw ue;
        }
    }

    protected List<LogSegmentMetadata> getFullLedgerList(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, true, LogSegmentMetadata.COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentMetadata> getFullLedgerListDesc(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, true, LogSegmentMetadata.DESC_COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentMetadata> getFilteredLedgerList(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, false, LogSegmentMetadata.COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentMetadata> getFilteredLedgerListDesc(boolean forceFetch, boolean throwOnEmpty)
            throws IOException {
        return getLedgerList(forceFetch, false, LogSegmentMetadata.DESC_COMPARATOR, throwOnEmpty);
    }

    protected List<LogSegmentMetadata> getLedgerList(boolean forceFetch,
                                                     boolean fetchFullList,
                                                     Comparator<LogSegmentMetadata> comparator,
                                                     boolean throwOnEmpty)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            List<LogSegmentMetadata> segments =
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

    private List<LogSegmentMetadata> doGetLedgerList(boolean forceFetch, boolean fetchFullList,
                                                     Comparator<LogSegmentMetadata> comparator,
                                                     boolean throwOnEmpty)
        throws IOException {
        if (fetchFullList) {
            if (forceFetch || !isFullListFetched.get()) {
                return forceGetLedgerList(comparator, LogSegmentFilter.DEFAULT_FILTER, throwOnEmpty);
            } else {
                return getCachedLogSegments(comparator);
            }
        } else {
            if (forceFetch) {
                return forceGetLedgerList(comparator, filter, throwOnEmpty);
            } else {
                if(!ledgerListWatchSet.get()) {
                    scheduleGetLedgersTask(true, true);
                }
                waitFirstGetLedgersTaskToFinish();
                return getCachedLogSegments(comparator);
            }
        }
    }

    /**
     * Get a list of all segments in the journal.
     */
    protected List<LogSegmentMetadata> forceGetLedgerList(final Comparator<LogSegmentMetadata> comparator,
                                                                final LogSegmentFilter segmentFilter,
                                                                boolean throwOnEmpty) throws IOException {
        final List<LogSegmentMetadata> ledgers = new ArrayList<LogSegmentMetadata>();
        final AtomicInteger result = new AtomicInteger(-1);
        final CountDownLatch latch = new CountDownLatch(1);
        Stopwatch stopwatch = Stopwatch.createStarted();
        asyncGetLedgerListInternal(comparator, segmentFilter, null, new GenericCallback<List<LogSegmentMetadata>>() {
            @Override
            public void operationComplete(int rc, List<LogSegmentMetadata> logSegmentMetadatas) {
                result.set(rc);
                if (KeeperException.Code.OK.intValue() == rc) {
                    ledgers.addAll(logSegmentMetadatas);
                } else {
                    LOG.error("Failed to get ledger list for {} : with error {}", getFullyQualifiedName(), rc);
                }
                latch.countDown();
            }
        }, new AtomicInteger(conf.getZKNumRetries()), new AtomicLong(conf.getZKRetryBackoffStartMillis()));
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
        } else {
            forceGetListStat.registerFailedEvent(elapsedMicros);
            if (KeeperException.Code.NONODE == rc) {
                throw new LogNotFoundException("Log " + getFullyQualifiedName() + " is not found");
            } else {
                throw new IOException("ZK Exception " + rc + " reading ledger list for " + getFullyQualifiedName());
            }
        }

        if (throwOnEmpty && ledgers.isEmpty()) {
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
        }
        return ledgers;
    }

    protected Future<List<LogSegmentMetadata>> asyncGetFullLedgerList(boolean forceFetch, boolean throwOnEmpty) {
        return asyncGetLedgerList(forceFetch, true, LogSegmentMetadata.COMPARATOR, throwOnEmpty);
    }

    protected Future<List<LogSegmentMetadata>> asyncGetFullLedgerListDesc(boolean forceFetch, boolean throwOnEmpty) {
        return asyncGetLedgerList(forceFetch, true, LogSegmentMetadata.DESC_COMPARATOR, throwOnEmpty);
    }

    protected Future<List<LogSegmentMetadata>> asyncGetFilteredLedgerList(boolean forceFetch, boolean throwOnEmpty) {
        return asyncGetLedgerList(forceFetch, false, LogSegmentMetadata.COMPARATOR, throwOnEmpty);
    }

    protected Future<List<LogSegmentMetadata>> asyncGetFilteredLedgerListDesc(boolean forceFetch, boolean throwOnEmpty) {
        return asyncGetLedgerList(forceFetch, false, LogSegmentMetadata.DESC_COMPARATOR, throwOnEmpty);
    }

    protected Future<List<LogSegmentMetadata>> asyncGetLedgerList(final boolean forceFetch,
                                                                        final boolean fetchFullList,
                                                                        final Comparator<LogSegmentMetadata> comparator,
                                                                        final boolean throwOnEmpty) {
        final Promise<List<LogSegmentMetadata>> promise = new Promise<List<LogSegmentMetadata>>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final OpStatsLogger statsLogger = fetchFullList ? getFullListStat : getFilteredListStat;
        asyncDoGetLedgerList(forceFetch, fetchFullList, comparator, throwOnEmpty)
                .addEventListener(new FutureEventListener<List<LogSegmentMetadata>>() {
                    @Override
                    public void onSuccess(List<LogSegmentMetadata> value) {
                        statsLogger.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                        promise.setValue(value);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        statsLogger.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                        promise.setException(cause);
                    }
                });
        return promise;
    }

    private Future<List<LogSegmentMetadata>> asyncDoGetLedgerList(final boolean forceFetch,
                                                                  final boolean fetchFullList,
                                                                  final Comparator<LogSegmentMetadata> comparator,
                                                                  final boolean throwOnEmpty) {
        if (fetchFullList) {
            if (forceFetch || !isFullListFetched.get()) {
                return asyncForceGetLedgerList(comparator, LogSegmentFilter.DEFAULT_FILTER, throwOnEmpty);
            } else {
                try {
                    return Future.value(getCachedLogSegments(comparator));
                } catch (UnexpectedException ue) {
                    return Future.exception(ue);
                }
            }
        } else {
            if (forceFetch) {
                return asyncForceGetLedgerList(comparator, filter, throwOnEmpty);
            } else {
                final Promise<List<LogSegmentMetadata>> promise =
                        new Promise<List<LogSegmentMetadata>>();
                SyncGetLedgersCallback task = firstGetLedgersTask;
                task.promise.addEventListener(new FutureEventListener<List<LogSegmentMetadata>>() {
                    @Override
                    public void onSuccess(List<LogSegmentMetadata> value) {
                        try {
                            promise.setValue(getCachedLogSegments(comparator));
                        } catch (UnexpectedException e) {
                            promise.setException(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                });
                return promise;
            }
        }
    }

    protected Future<List<LogSegmentMetadata>> asyncForceGetLedgerList(final Comparator<LogSegmentMetadata> comparator,
                                                                       final LogSegmentFilter segmentFilter,
                                                                       final boolean throwOnEmpty) {
        final Promise<List<LogSegmentMetadata>> promise = new Promise<List<LogSegmentMetadata>>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        asyncGetLedgerListWithRetries(comparator, segmentFilter, null)
            .addEventListener(new FutureEventListener<List<LogSegmentMetadata>>() {

                @Override
                public void onSuccess(List<LogSegmentMetadata> ledgers) {
                    forceGetListStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                    if (ledgers.isEmpty() && throwOnEmpty) {
                        promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " is empty"));
                    } else {
                        promise.setValue(ledgers);
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    forceGetListStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                    promise.setException(cause);
                }
            });
        return promise;
    }

    /**
     * Add the segment <i>metadata</i> for <i>name</i> in the cache.
     *
     * @param name
     *          segment znode name.
     * @param metadata
     *          segment metadata.
     */
    protected void addLogSegmentToCache(String name, LogSegmentMetadata metadata) {
        logSegmentCache.add(name, metadata);
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
                    if (elapsedMillis > metadataLatencyWarnThresholdMillis) {
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
                    if (elapsedMillis > metadataLatencyWarnThresholdMillis) {
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

    protected LogSegmentMetadata readLogSegmentFromCache(String name) {
        return logSegmentCache.get(name);
    }

    protected LogSegmentMetadata removeLogSegmentFromCache(String name) {
        return logSegmentCache.remove(name);
    }

    public void asyncGetLedgerList(final Comparator<LogSegmentMetadata> comparator,
                                   Watcher watcher,
                                   final GenericCallback<List<LogSegmentMetadata>> callback) {
        asyncGetLedgerListWithRetries(comparator, filter, watcher, callback);
    }

    protected Future<List<LogSegmentMetadata>> asyncGetLedgerListWithRetries(Comparator<LogSegmentMetadata> comparator,
                                                                             LogSegmentFilter segmentFilter,
                                                                             Watcher watcher) {
        final Promise<List<LogSegmentMetadata>> promise = new Promise<List<LogSegmentMetadata>>();
        asyncGetLedgerListWithRetries(comparator, segmentFilter, watcher, new GenericCallback<List<LogSegmentMetadata>>() {
            @Override
            public void operationComplete(int rc, List<LogSegmentMetadata> segments) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    promise.setValue(segments);
                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                    promise.setException(new LogNotFoundException("Log " + getFullyQualifiedName() + " not found"));
                } else {
                    String errMsg = "ZK Exception " + rc + " reading ledger list for " + getFullyQualifiedName();
                    promise.setException(new ZKException(errMsg, KeeperException.Code.get(rc)));
                }
            }
        });
        return promise;
    }

    private void asyncGetLedgerListWithRetries(final Comparator<LogSegmentMetadata> comparator,
                                               final LogSegmentFilter segmentFilter,
                                               final Watcher watcher,
                                               final GenericCallback<List<LogSegmentMetadata>> finalCallback) {
        asyncGetLedgerListInternal(comparator, segmentFilter, watcher, finalCallback,
                new AtomicInteger(conf.getZKNumRetries()), new AtomicLong(conf.getZKRetryBackoffStartMillis()));
    }

    private void asyncGetLedgerListInternal(final Comparator<LogSegmentMetadata> comparator,
                                            final LogSegmentFilter segmentFilter,
                                            final Watcher watcher,
                                            final GenericCallback<List<LogSegmentMetadata>> finalCallback,
                                            final AtomicInteger numAttemptsLeft,
                                            final AtomicLong backoffMillis) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Async getting ledger list for {}.", getFullyQualifiedName());
            }
            final GenericCallback<List<LogSegmentMetadata>> callback = new GenericCallback<List<LogSegmentMetadata>>() {
                @Override
                public void operationComplete(int rc, List<LogSegmentMetadata> result) {
                    long elapsedMicros = stopwatch.stop().elapsed(TimeUnit.MICROSECONDS);
                    if (KeeperException.Code.OK.intValue() != rc) {
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
            zooKeeperClient.get().getChildren(logMetadata.getLogSegmentsPath(), watcher, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(final int rc, final String path, final Object ctx, final List<String> children, final Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {

                        if ((KeeperException.Code.CONNECTIONLOSS.intValue() == rc ||
                             KeeperException.Code.SESSIONEXPIRED.intValue() == rc ||
                             KeeperException.Code.SESSIONMOVED.intValue() == rc) &&
                            numAttemptsLeft.decrementAndGet() > 0) {
                            long backoffMs = backoffMillis.get();
                            backoffMillis.set(Math.min(conf.getZKRetryBackoffMaxMillis(), 2 * backoffMs));
                            scheduler.schedule(new Runnable() {
                                @Override
                                public void run() {
                                    asyncGetLedgerListInternal(comparator, segmentFilter, watcher,
                                            finalCallback, numAttemptsLeft, backoffMillis);
                                }
                            }, backoffMs, TimeUnit.MILLISECONDS);
                            return;
                        }
                        callback.operationComplete(rc, null);
                        return;
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Got ledger list from {} : {}", logMetadata.getLogSegmentsPath(), children);
                    }

                    ledgerListWatchSet.set(true);
                    Set<String> segmentsReceived = new HashSet<String>();
                    segmentsReceived.addAll(segmentFilter.filter(children));
                    Set<String> segmentsAdded;
                    final Set<String> removedSegments = Collections.synchronizedSet(new HashSet<String>());
                    final Map<String, LogSegmentMetadata> addedSegments =
                            Collections.synchronizedMap(new HashMap<String, LogSegmentMetadata>());
                    Pair<Set<String>, Set<String>> segmentChanges = logSegmentCache.diff(segmentsReceived);
                    segmentsAdded = segmentChanges.getLeft();
                    removedSegments.addAll(segmentChanges.getRight());

                    if (segmentsAdded.isEmpty()) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("No segments added for {}.", getFullyQualifiedName());
                        }

                        // update the cache before fetch
                        logSegmentCache.update(removedSegments, addedSegments);

                        List<LogSegmentMetadata> segmentList;
                        try {
                            segmentList = getCachedLogSegments(comparator);
                        } catch (UnexpectedException e) {
                            callback.operationComplete(KeeperException.Code.DATAINCONSISTENCY.intValue(), null);
                            return;
                        }
                        callback.operationComplete(KeeperException.Code.OK.intValue(), segmentList);
                        notifyUpdatedLogSegments(segmentList);
                        if (!removedSegments.isEmpty()) {
                            notifyOnOperationComplete();
                        }
                        return;
                    }

                    final AtomicInteger numChildren = new AtomicInteger(segmentsAdded.size());
                    final AtomicInteger numFailures = new AtomicInteger(0);
                    for (final String segment: segmentsAdded) {
                        metadataStore.getLogSegment(logMetadata.getLogSegmentPath(segment))
                                .addEventListener(new FutureEventListener<LogSegmentMetadata>() {

                                    @Override
                                    public void onSuccess(LogSegmentMetadata result) {
                                        addedSegments.put(segment, result);
                                        complete();
                                    }

                                    @Override
                                    public void onFailure(Throwable cause) {
                                        // NONODE exception is possible in two cases
                                        // 1. A log segment was deleted by truncation between the call to getChildren and read
                                        // attempt on the znode corresponding to the segment
                                        // 2. In progress segment has been completed => inprogress ZNode does not exist
                                        if (cause instanceof KeeperException &&
                                                KeeperException.Code.NONODE == ((KeeperException) cause).code()) {
                                            removedSegments.add(segment);
                                            complete();
                                        } else {
                                            // fail fast
                                            if (1 == numFailures.incrementAndGet()) {
                                                int rcToReturn = KeeperException.Code.SYSTEMERROR.intValue();
                                                if (cause instanceof KeeperException) {
                                                    rcToReturn = ((KeeperException) cause).code().intValue();
                                                } else if (cause instanceof ZKException) {
                                                    rcToReturn = ((ZKException) cause).getKeeperExceptionCode().intValue();
                                                }
                                                // :( properly we need dlog related response code.
                                                callback.operationComplete(rcToReturn, null);
                                                return;
                                            }
                                        }
                                    }

                                    private void complete() {
                                        if (0 == numChildren.decrementAndGet() && numFailures.get() == 0) {
                                            // update the cache only when fetch completed
                                            logSegmentCache.update(removedSegments, addedSegments);
                                            List<LogSegmentMetadata> segmentList;
                                            try {
                                                segmentList = getCachedLogSegments(comparator);
                                            } catch (UnexpectedException e) {
                                                callback.operationComplete(KeeperException.Code.DATAINCONSISTENCY.intValue(), null);
                                                return;
                                            }
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
            finalCallback.operationComplete(KeeperException.Code.CONNECTIONLOSS.intValue(), null);
        } catch (InterruptedException e) {
            getListStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            finalCallback.operationComplete(KeeperException.Code.CONNECTIONLOSS.intValue(), null);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (Watcher.Event.EventType.None.equals(event.getType())) {
            if (event.getState() == Watcher.Event.KeeperState.Expired) {
                // if the watcher is expired
                scheduler.schedule(new WatcherGetLedgersCallback(getFullyQualifiedName()),
                        conf.getZKRetryBackoffStartMillis(), TimeUnit.MILLISECONDS);
            }
        } else if (Watcher.Event.EventType.NodeChildrenChanged.equals(event.getType())) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("LogSegments Changed under {}.", getFullyQualifiedName());
            }
            asyncGetLedgerListWithRetries(LogSegmentMetadata.COMPARATOR, filter,
                    getChildrenWatcher, new WatcherGetLedgersCallback(getFullyQualifiedName()));
        }
    }

    void notifyOnOperationComplete() {
        if (null != notification) {
            notification.notifyOnOperationComplete();
        }
    }

    // ZooKeeper Watchers

    Watcher registerExpirationHandler(final ZooKeeperClient.ZooKeeperSessionExpireNotifier onExpired) {
        if (conf.getZKNumRetries() > 0) {
            return new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // nop
                }
            };
        }
        return zooKeeperClient.registerExpirationHandler(onExpired);
    }

    boolean unregister(Watcher watcher) {
        return zooKeeperClient.unregister(watcher);
    }
}
