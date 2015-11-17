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
package com.twitter.distributedlog.v2;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.twitter.distributedlog.AlreadyTruncatedTransactionException;
import com.twitter.distributedlog.LogEmptyException;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.v2.metadata.BKDLConfig;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.twitter.distributedlog.DLSNUtil.*;

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
abstract class BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionHandler.class);

    private static final int LAYOUT_VERSION = -1;
    private static final int MAX_RETRIES_FOR_NONODE_EXCEPTION = 5;

    static final String VERSION_PATH = "/version";
    static final String LEDGERS_PATH = "/ledgers";
    static final String MAXTXID_PATH = "/maxtxid";
    static final String LOCK_PATH = "/lock";

    protected final String name;
    protected final String streamIdentifier;
    protected final DistributedLogConfiguration conf;
    protected final ZooKeeperClient zooKeeperClient;
    protected final boolean ownZKC;
    protected final BookKeeperClient bookKeeperClient;
    protected final boolean ownBKC;
    protected final String partitionRootPath;
    protected final String ledgerPath;
    protected final String digestpw;
    protected long lastLedgerRollingTimeMillis = -1;
    protected final ScheduledExecutorService executorService;
    protected final StatsLogger statsLogger;

    // Maintain the list of ledgers
    protected final Map<String, LogSegmentLedgerMetadata> logSegments =
            new HashMap<String, LogSegmentLedgerMetadata>();
    protected final ConcurrentMap<Long, LogSegmentLedgerMetadata> lid2LogSegments =
            new ConcurrentHashMap<Long, LogSegmentLedgerMetadata>();

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
        partitionRootPath = BKDistributedLogManager.getPartitionPath(uri, name, streamIdentifier);

        ledgerPath = partitionRootPath + LEDGERS_PATH;
        digestpw = conf.getBKDigestPW();

        if (null == zkcBuilder) {
            zkcBuilder = ZooKeeperClientBuilder.newBuilder()
                    .name(String.format("dlzk:%s:handler_dedicated", name))
                    .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                    .uri(uri)
                    .statsLogger(statsLogger.scope("dlzk_handler_dedicated"))
                    .retryThreadCount(conf.getZKClientNumberRetryThreads())
                    .requestRateLimit(conf.getZKRequestRateLimit())
                    .zkAclId(conf.getZkAclId());
            ownZKC = true;
        } else {
            ownZKC = false;
        }
        this.zooKeeperClient = zkcBuilder.build();
        LOG.debug("Using ZK Path {}", partitionRootPath);
        if (null == bkcBuilder) {
            // resolve uri
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(this.zooKeeperClient, uri);
            bkcBuilder = BookKeeperClientBuilder.newBuilder()
                    .dlConfig(conf)
                    .name(String.format("bk:%s:handler_dedicated", name))
                    .zkServers(bkdlConfig.getBkZkServersForWriter())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .statsLogger(statsLogger);
            ownBKC = true;
        } else {
            ownBKC = false;
        }
        this.bookKeeperClient = bkcBuilder.build();
    }

    public LogRecord getLastLogRecord(boolean recover, boolean includeEndOfStream) throws IOException {
        checkLogStreamExists();
        List<LogSegmentLedgerMetadata> ledgerList = getLedgerListDesc(true);

        for (LogSegmentLedgerMetadata metadata: ledgerList) {
            LogRecord record = recoverLastRecordInLedger(metadata, recover, false, includeEndOfStream);

            if (null != record) {
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

    public long getLogRecordCount() throws IOException {
        try {
            checkLogStreamExists();
        } catch (LogEmptyException exc) {
            return 0;
        }

        List<LogSegmentLedgerMetadata> ledgerList = getLedgerList(false);
        long count = 0;
        for (LogSegmentLedgerMetadata l : ledgerList) {
            if (l.isInProgress()) {
                LogRecord record = recoverLastRecordInLedger(l, false, false, false);
                if (null != record) {
                    count += getPositionWithinLogSegment(record);
                }
            } else {
                count += l.getRecordCount();
            }
        }
        return count;
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
        List<LogSegmentLedgerMetadata> ledgerListDesc = getLedgerListDesc(false);
        for (LogSegmentLedgerMetadata l : ledgerListDesc) {
            LOG.debug("Inspecting Ledger: {}", l);
            if (thresholdTxId < l.getFirstTxId()) {
                continue;
            }

            if (l.isInProgress()) {
                try {
                    long lastTxId = readLastTxIdInLedger(l);
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
        if (ownBKC) {
            bookKeeperClient.close();
        }
        if (ownZKC) {
            zooKeeperClient.close();
        }
    }

    /**
     * Find the id of the last edit log transaction written to a edit log
     * ledger.
     */
    protected long readLastTxIdInLedger(LogSegmentLedgerMetadata l) throws IOException {
        LogRecord record = recoverLastRecordInLedger(l, false, false, true);

        if (null == record) {
            return DistributedLogConstants.EMPTY_LEDGER_TX_ID;
        }
        else {
            return record.getTransactionId();
        }
    }

    /**
     * Find the id of the last edit log transaction written to a edit log
     * ledger.
     */
    protected LogRecordWithDLSN recoverLastRecordInLedger(LogSegmentLedgerMetadata l,
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

            if (scanStartPoint < 0) {
                LOG.debug("Ledger is empty {}", l.getLedgerId());
                // Ledger is empty
                return null;
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

            while (true) {
                BKPerStreamLogReader in
                    = new BKPerStreamLogReader(this, ledgerDescriptor, l, scanStartPoint,
                        ledgerDataAccessorPriv, includeControl, statsLogger);

                lastRecord = null;
                try {
                    LogRecordWithDLSN record = in.readOp(false);
                    while (record != null) {
                        if ((null == lastRecord
                            || record.getTransactionId() > lastRecord.getTransactionId()) &&
                            (includeEndOfStream || !isEndOfStream(record))) {
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

        return lastRecord;
    }

    // Ledgers Related Functions
    // ***Note***
    // Caching of log segment metadata assumes that the data contained in the ZNodes for individual
    // log segments is never updated after creation i.e we never call setData. A log segment
    // is finalized by creating a new ZNode and deleting the in progress node. This code will have
    // to change if we change the behavior

    private List<LogSegmentLedgerMetadata> getCachedLedgerList(Comparator comparator) {
        List<LogSegmentLedgerMetadata> segmentsToReturn;
        synchronized (logSegments) {
            segmentsToReturn = new ArrayList<LogSegmentLedgerMetadata>(logSegments.size());
            for (String name : logSegments.keySet()) {
                segmentsToReturn.add(logSegments.get(name));
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Cached log segments : {}", segmentsToReturn);
            }
        }
        Collections.sort(segmentsToReturn, comparator);
        return segmentsToReturn;
    }

    public List<LogSegmentLedgerMetadata> getLedgerList(boolean throwOnEmpty) throws IOException {
        return getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, throwOnEmpty, Optional.<Watcher>absent());
    }

    protected List<LogSegmentLedgerMetadata> getLedgerListDesc(boolean throwOnEmpty) throws IOException {
        return getLedgerList(LogSegmentLedgerMetadata.DESC_COMPARATOR, throwOnEmpty, Optional.<Watcher>absent());
    }

    public List<LogSegmentLedgerMetadata> getLedgerList(Watcher watcher) throws IOException {
        return getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, false, Optional.fromNullable(watcher));
    }

    public List<LogSegmentLedgerMetadata> getLedgerList() throws IOException {
        return getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, false, Optional.<Watcher>absent());
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
    }

    protected LogSegmentLedgerMetadata readLogSegmentFromCache(String name) {
        synchronized (logSegments) {
            return logSegments.get(name);
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
    protected List<LogSegmentLedgerMetadata> getLedgerList(Comparator comparator, boolean throwOnEmpty, Optional<Watcher> watcherOptional) throws IOException {
        int numAttempts = 1;
        while (true) {
            try {
                final List<String> ledgerNames;
                if (watcherOptional.isPresent()) {
                    ledgerNames = zooKeeperClient.get().getChildren(ledgerPath, watcherOptional.get());
                } else {
                    ledgerNames = zooKeeperClient.get().getChildren(ledgerPath, false);
                }
                Set<String> segmentsReceived = new HashSet<String>();
                segmentsReceived.addAll(ledgerNames);
                Set<String> segmentsAdded;
                Set<String> segmentsRemoved;
                synchronized (logSegments) {
                    Set<String> segmentsCached = logSegments.keySet();
                    segmentsAdded = Sets.difference(segmentsReceived, segmentsCached).immutableCopy();
                    segmentsRemoved = Sets.difference(segmentsCached, segmentsReceived).immutableCopy();
                    for (String s : segmentsRemoved) {
                        LogSegmentLedgerMetadata segmentMetadata = removeLogSegmentFromCache(s);
                        LOG.debug("Removed log segment {} from cache : {}.", s, segmentMetadata);
                    }
                }

                if (segmentsAdded.isEmpty()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("No segments added for {}.", getFullyQualifiedName());
                    }
                    return getCachedLedgerList(comparator);
                }

                for (String segment : segmentsAdded) {
                    LogSegmentLedgerMetadata l = LogSegmentLedgerMetadata.read(zooKeeperClient, ledgerPath + "/" + segment);
                    addLogSegmentToCache(segment, l);
                    if (!l.isInProgress() && (lastLedgerRollingTimeMillis < l.getCompletionTime())) {
                        lastLedgerRollingTimeMillis = l.getCompletionTime();
                    }
                }
                break;
            } catch (ZooKeeperClient.ZooKeeperConnectionException zce) {
                throw new IOException("Exception on establishing zookeeper connection : ", zce);
            } catch (KeeperException.NoNodeException nne) {
                if (!nne.getPath().contains("inprogress") || numAttempts >= MAX_RETRIES_FOR_NONODE_EXCEPTION) {
                    throw new IOException("Exception reading ledger list from zk", nne);
                }
                numAttempts++;
            } catch (KeeperException e) {
                throw new IOException("Exception reading ledger list from zk", e);
            } catch (InterruptedException e) {
                throw new DLInterruptedException("Interrupted on reading ledger list from zk", e);
            }
        }
        List<LogSegmentLedgerMetadata> ledgers = getCachedLedgerList(comparator);

        if (throwOnEmpty && ledgers.isEmpty()) {
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
        }

        return ledgers;
    }

    protected void getLedgerList(final Comparator comparator, Watcher watcher,
            final BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>> callback) {
        try {
            zooKeeperClient.get().getChildren(ledgerPath, watcher, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        callback.operationComplete(-1, null);
                        return;
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Got ledger list from {} : {}", ledgerPath, children);
                    }

                    Set<String> segmentsReceived = new HashSet<String>();
                    segmentsReceived.addAll(children);
                    Set<String> segmentsAdded;
                    Set<String> segmentsRemoved;
                    synchronized (logSegments) {
                        Set<String> segmentsCached = logSegments.keySet();
                        segmentsAdded = Sets.difference(segmentsReceived, segmentsCached).immutableCopy();
                        segmentsRemoved = Sets.difference(segmentsCached, segmentsReceived).immutableCopy();
                        for (String s : segmentsRemoved) {
                            LogSegmentLedgerMetadata segmentMetadata = removeLogSegmentFromCache(s);
                            LOG.debug("Removed log segment {} from cache : {}.", s, segmentMetadata);
                        }
                    }

                    if (segmentsAdded.isEmpty()) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("No segments added for {}.", getFullyQualifiedName());
                        }

                        List<LogSegmentLedgerMetadata> segmentList = getCachedLedgerList(comparator);
                        callback.operationComplete(BKException.Code.OK, segmentList);
                        return;
                    }

                    final AtomicInteger numChildren = new AtomicInteger(segmentsAdded.size());
                    final AtomicInteger numFailures = new AtomicInteger(0);
                    for (final String segment: segmentsAdded) {
                        LogSegmentLedgerMetadata.read(zooKeeperClient, ledgerPath + "/" + segment,
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
                                }
                                if (0 == numChildren.decrementAndGet() && numFailures.get() == 0) {
                                    List<LogSegmentLedgerMetadata> segmentList = getCachedLedgerList(comparator);
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

    public String getFullyQualifiedName() {
        return String.format("%s:%s", name, streamIdentifier);
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
