package com.twitter.distributedlog.v2;

import com.google.common.base.Optional;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.lock.DistributedReentrantLock;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.Utils;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.DLSNUtil.*;

class BKLogPartitionWriteHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    private static final int LAYOUT_VERSION = -1;

    private final DistributedReentrantLock lock;
    private final DistributedReentrantLock deleteLock;
    protected final OrderedSafeExecutor lockStateExecutor;
    private final String maxTxIdPath;
    private final MaxTxId maxTxId;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private boolean lockAcquired;
    private LedgerHandle currentLedger = null;
    private long currentLedgerStartTxId = DistributedLogConstants.INVALID_TXID;
    private volatile boolean recovered = false;
    protected final PermitLimiter writeLimiter;

    private static int bytesToInt(byte[] b) {
        assert b.length >= 4;
        return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
    }

    private static byte[] intToBytes(int i) {
        return new byte[]{
            (byte) (i >> 24),
            (byte) (i >> 16),
            (byte) (i >> 8),
            (byte) (i)};
    }

    // Stats
    private final OpStatsLogger closeOpStats;
    private final OpStatsLogger openOpStats;
    private final OpStatsLogger recoverOpStats;
    private final OpStatsLogger deleteOpStats;

    static void createStreamIfNotExists(final String partitionRootPath,
                                        final ZooKeeperClient zooKeeperClient) throws IOException {
        String versionPath = partitionRootPath + VERSION_PATH;
        String ledgerPath = partitionRootPath + LEDGERS_PATH;
        try {
            while (zooKeeperClient.get().exists(partitionRootPath, false) == null) {
                try {
                    Utils.zkCreateFullPathOptimistic(zooKeeperClient, partitionRootPath, new byte[]{'0'},
                            zooKeeperClient.getDefaultACL(), CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException exc) {
                    LOG.debug("Race on node creation, clean retry");
                }
            }

            Stat versionStat = zooKeeperClient.get().exists(versionPath, false);
            if (versionStat != null) {
                byte[] d = zooKeeperClient.get().getData(versionPath, false, versionStat);
                // There's only one version at the moment
                assert bytesToInt(d) == LAYOUT_VERSION;
            } else {
                zooKeeperClient.get().create(versionPath, intToBytes(LAYOUT_VERSION),
                        zooKeeperClient.getDefaultACL(), CreateMode.PERSISTENT);
            }

            if (zooKeeperClient.get().exists(ledgerPath, false) == null) {
                zooKeeperClient.get().create(ledgerPath, new byte[]{'0'},
                        zooKeeperClient.getDefaultACL(), CreateMode.PERSISTENT);
            }
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            throw new IOException("Encountered zookeeper connection issue when initializing stream for " +
                    partitionRootPath, e);
        } catch (KeeperException ke) {
            throw new IOException("Encountered zookeeper issue when initializing stream for " + partitionRootPath, ke);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on constructing log partition handler for " +
                    partitionRootPath, ie);
        }
    }

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogPartitionWriteHandler(String name,
                               String streamIdentifier,
                               DistributedLogConfiguration conf,
                               URI uri,
                               ZooKeeperClientBuilder zkcBuilder,
                               BookKeeperClientBuilder bkcBuilder,
                               ScheduledExecutorService executorService,
                               OrderedSafeExecutor lockStateExecutor,
                               StatsLogger statsLogger,
                               String clientId,
                               PermitLimiter writeLimiter) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder, executorService, statsLogger);
        this.lockStateExecutor = lockStateExecutor;
        this.writeLimiter = writeLimiter;

        ensembleSize = conf.getEnsembleSize();

        if (ensembleSize < conf.getWriteQuorumSize()) {
            writeQuorumSize = ensembleSize;
            LOG.warn("Setting write quorum size {} greater than ensemble size {}",
                conf.getWriteQuorumSize(), ensembleSize);
        } else {
            writeQuorumSize = conf.getWriteQuorumSize();
        }

        if (writeQuorumSize < conf.getAckQuorumSize()) {
            ackQuorumSize = writeQuorumSize;
            LOG.warn("Setting write ack quorum size {} greater than write quorum size {}",
                conf.getAckQuorumSize(), writeQuorumSize);
        } else {
            ackQuorumSize = conf.getAckQuorumSize();
        }

        createStreamIfNotExists(partitionRootPath, zooKeeperClient);

        maxTxIdPath = partitionRootPath + MAXTXID_PATH;
        String lockPath = partitionRootPath + LOCK_PATH;

        if (clientId.equals(DistributedLogConstants.UNKNOWN_CLIENT_ID)){
            try {
                clientId = InetAddress.getLocalHost().toString();
            } catch(Exception exc) {
                // Best effort
                clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
            }
        }

        try {
            if (zooKeeperClient.get().exists(lockPath, false) == null) {
                FutureUtils.result(Utils.zkAsyncCreateFullPathOptimistic(
                        zooKeeperClient,
                        lockPath,
                        Optional.of(partitionRootPath),
                        new byte[0],
                        zooKeeperClient.getDefaultACL(),
                        CreateMode.PERSISTENT));
            }
        } catch (ZKException ke) {
            if (KeeperException.Code.NODEEXISTS != ke.getKeeperExceptionCode()) {
                throw ke;
            }
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on creating zookeeper lock " + lockPath, e);
        } catch (KeeperException e) {
            if (KeeperException.Code.NODEEXISTS != e.code()) {
                throw new ZKException("Exception when creating zookeeper lock " + lockPath, e);
            }
        }
        lock = new DistributedReentrantLock(lockStateExecutor, zooKeeperClient, lockPath,
                conf.getLockTimeoutMilliSeconds(), clientId, statsLogger, conf.getZKNumRetries(),
                conf.getLockReacquireTimeoutMilliSeconds(), conf.getLockOpTimeoutMilliSeconds());
        deleteLock = new DistributedReentrantLock(lockStateExecutor, zooKeeperClient, lockPath,
                conf.getLockTimeoutMilliSeconds(), clientId, statsLogger, conf.getZKNumRetries(),
                conf.getLockReacquireTimeoutMilliSeconds(), conf.getLockOpTimeoutMilliSeconds());
        maxTxId = new MaxTxId(zooKeeperClient, maxTxIdPath);
        lastLedgerRollingTimeMillis = Utils.nowInMillis();
        lockAcquired = false;

        // Stats
        StatsLogger segmentsStatsLogger = statsLogger.scope("segments");
        openOpStats = segmentsStatsLogger.getOpStatsLogger("open");
        closeOpStats = segmentsStatsLogger.getOpStatsLogger("close");
        recoverOpStats = segmentsStatsLogger.getOpStatsLogger("recover");
        deleteOpStats = segmentsStatsLogger.getOpStatsLogger("delete");
    }

    /**
     * Start a new log segment in a BookKeeper ledger.
     * First ensure that we have the write lock for this journal.
     * Then create a ledger and stream based on that ledger.
     * The ledger id is written to the inprogress znode, so that in the
     * case of a crash, a recovery process can find the ledger we were writing
     * to when we crashed.
     *
     * @param txId First transaction id to be written to the stream
     */
    public BKPerStreamLogWriter startLogSegment(long txId) throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            BKPerStreamLogWriter writer = doStartLogSegment(txId);
            success = true;
            return writer;
        } finally {
            if (success) {
                openOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                openOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    private BKPerStreamLogWriter doStartLogSegment(long txId) throws IOException {
        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lockAcquired = true;
        long highestTxIdWritten = maxTxId.get();
        if (highestTxIdWritten == DistributedLogConstants.MAX_TXID) {
            LOG.error("We've already marked the stream as ended and attempting to start a new log segment");
            throw new EndOfStreamException("Writing to a stream after it has been marked as completed");
        }

        if (txId < highestTxIdWritten) {
            LOG.error("We've already seen TxId {} the max TXId is {}", txId, highestTxIdWritten);
            LOG.error("Last Committed Ledger {}", getLedgerListDesc(false));
            throw new TransactionIdOutOfOrderException(txId, highestTxIdWritten);
        }

        boolean writeInprogressZnode = false;
        try {
            if (currentLedger != null) {
                try {
                    // bookkeeper errored on last stream, clean up ledger
                    currentLedger.close();
                } catch (BKException.BKLedgerClosedException lce) {
                    LOG.debug("Ledger already closed {}", lce);
                }
            }
            currentLedger = bookKeeperClient.get().createLedger(ensembleSize, writeQuorumSize, ackQuorumSize,
                BookKeeper.DigestType.CRC32,
                digestpw.getBytes(UTF_8));
            String znodePath = inprogressZNode(txId);
            LogSegmentLedgerMetadata l = new LogSegmentLedgerMetadata(znodePath,
                DistributedLogConstants.LAYOUT_VERSION, currentLedger.getId(), txId);

            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentAfterLedgerCreate);

            /*
             * Write the ledger metadata out to the inprogress ledger znode
             * This can fail if for some reason our write lock has
             * expired (@see DistributedExclusiveLock) and another process has managed to
             * create the inprogress znode.
             * In this case, throw an exception. We don't want to continue
             * as this would lead to a split brain situation.
             */
            l.write(zooKeeperClient, znodePath);
            writeInprogressZnode = true;
            LOG.debug("Storing MaxTxId in startLogSegment  {} {}", znodePath, txId);

            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentAfterInProgressCreate);

            maxTxId.store(txId);
            currentLedgerStartTxId = txId;
            return new BKPerStreamLogWriter(getFullyQualifiedName(), conf, currentLedger, lock, txId, executorService,
                                            statsLogger, writeLimiter);
        } catch (Exception e) {
            LOG.error("Exception during StartLogSegment", e);
            if (currentLedger != null) {
                try {
                    long id = currentLedger.getId();
                    currentLedger.close();
                    // If we already wrote inprogress znode, we should not delete the ledger.
                    // There are cases where if the thread was interrupted on the client
                    // while the ZNode was being written, it still may have been written while we
                    // hit an exception. For now we will leak the ledger and leave a warning
                    // With ledger pre-allocation we would have a separate node to track allocation
                    // and would be doing a ZK transaction to move the ledger to the stream , this
                    // leak will automatically be addressed in that change
                    // {@link https://jira.twitter.biz/browse/PUBSUB-1230}
                    if (!writeInprogressZnode) {
                        LOG.warn("Potentially leaking Ledger with Id {}", id);
                    }
                } catch (Exception e2) {
                    //log & ignore, an IOException will be thrown soon
                    LOG.error("Error closing ledger", e2);
                }
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Error creating ledger", e);
            }
        }
    }

    public boolean shouldStartNewSegment() {

        boolean shouldSwitch = false;

        if (conf.getLogSegmentRollingIntervalMinutes() > 0) {
            shouldSwitch = (Utils.elapsedMSec(lastLedgerRollingTimeMillis) >
                ((long)conf.getLogSegmentRollingIntervalMinutes() * 60 * 1000));
        }

        if (shouldSwitch) {
            LOG.debug("Last Finalize Time: {} elapsed time (MSec): {}", lastLedgerRollingTimeMillis, Utils.elapsedMSec(lastLedgerRollingTimeMillis));
        }
        return shouldSwitch;
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     * <p/>
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
     public void completeAndCloseLogSegment(BKPerStreamLogWriter writer)
        throws IOException {
         writer.closeToFinalize();
         completeAndCloseLogSegment(inprogressZNodeName(currentLedgerStartTxId), currentLedgerStartTxId, writer.getLastTxId(), writer.getRecordCount(), true);
     }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     * <p/>
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    @VisibleForTesting
    public void completeAndCloseLogSegment(String inprogressZnodeName, long firstTxId, long lastTxId, int recordCount)
        throws IOException {
        completeAndCloseLogSegment(inprogressZnodeName, firstTxId, lastTxId, recordCount, true);
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     * <p/>
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    public void completeAndCloseLogSegment(String inprogressZnodeName, long firstTxId, long lastTxId,
                                           int recordCount, boolean shouldReleaseLock)
            throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            doCompleteAndCloseLogSegment(inprogressZnodeName, firstTxId, lastTxId, recordCount, shouldReleaseLock);
            success = true;
        } finally {
            if (success) {
                closeOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                closeOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    private void doCompleteAndCloseLogSegment(String inprogressZnodeName, long firstTxId, long lastTxId,
                                              int recordCount, boolean shouldReleaseLock)
            throws IOException {
        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        String inprogressPath = inprogressZNode(inprogressZnodeName);
        try {
            Stat inprogressStat = zooKeeperClient.get().exists(inprogressPath, false);
            if (inprogressStat == null) {
                throw new IOException("Inprogress znode " + inprogressPath
                    + " doesn't exist");
            }

            lock.checkOwnershipAndReacquire(true);
            LogSegmentLedgerMetadata l
                = LogSegmentLedgerMetadata.read(zooKeeperClient, inprogressPath);

            if (currentLedger != null) { // normal, non-recovery case
                if (l.getLedgerId() == currentLedger.getId()) {
                    try {
                        currentLedger.close();
                    } catch (BKException.BKLedgerClosedException lce) {
                        LOG.debug("Ledger already closed", lce);
                    } catch (BKException bke) {
                        LOG.error("Error closing current ledger", bke);
                    }
                    currentLedger = null;
                } else {
                    throw new IOException(
                        "Active ledger has different ID to inprogress. "
                            + l.getLedgerId() + " found, "
                            + currentLedger.getId() + " expected");
                }
            }

            if (l.getFirstTxId() != firstTxId) {
                throw new IOException("Transaction id not as expected, "
                    + l.getFirstTxId() + " found, " + firstTxId + " expected");
            }

            lastLedgerRollingTimeMillis = l.finalizeLedger(lastTxId,
                    conf.getEnableRecordCounts() ? recordCount : 0);
            String pathForCompletedLedger = completedLedgerZNode(firstTxId, lastTxId);
            try {
                l.write(zooKeeperClient, pathForCompletedLedger);
            } catch (KeeperException.NodeExistsException nee) {
                if (!l.checkEquivalence(zooKeeperClient, pathForCompletedLedger)) {
                    if ((l.getFirstTxId() == DistributedLogConstants.MAX_TXID) &&
                        (l.getLastTxId() == DistributedLogConstants.MAX_TXID)) {
                        throw new EndOfStreamException("Writing to " + getFullyQualifiedName()  + " after it has been marked as completed");
                    } else {
                        throw new IOException("Node " + pathForCompletedLedger + " already exists"
                            + " but data doesn't match");
                    }
                }
            }

            LOG.debug("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressPath, lastTxId);
            maxTxId.store(lastTxId);

            if (FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_FinalizeLedgerBeforeDelete)) {
                return;
            }

            zooKeeperClient.get().delete(inprogressPath, inprogressStat.getVersion());
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted when finalising stream " + partitionRootPath, e);
        } catch (KeeperException.NoNodeException e) {
            throw new IOException("Error when finalising stream " + partitionRootPath, e);
        } catch (KeeperException e) {
            throw new IOException("Error when finalising stream " + partitionRootPath, e);
        } finally {
            if (shouldReleaseLock && lockAcquired) {
                lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
                lockAcquired = false;
            }
        }
    }

    public void recoverIncompleteLogSegments() throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            doRecoverIncompleteLogSegments();
            success = true;
        } finally {
            if (success) {
                recoverOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                recoverOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    private void doRecoverIncompleteLogSegments() throws IOException {
        if (recovered) {
            return;
        }
        LOG.info("Initiating Recovery For {}", getFullyQualifiedName());
        lock.acquire(DistributedReentrantLock.LockReason.RECOVER);
        synchronized (this) {
            try {
                if (recovered) {
                    return;
                }
                for (LogSegmentLedgerMetadata l : getLedgerList()) {
                    if (!l.isInProgress()) {
                        continue;
                    }
                    long endTxId = DistributedLogConstants.EMPTY_LEDGER_TX_ID;
                    int recordCount = 0;

                    LogRecord record = recoverLastRecordInLedger(l, true, true, true);

                    if (null != record) {
                        endTxId = record.getTransactionId();
                        recordCount = getPositionWithinLogSegment(record);
                    }

                    if (endTxId == DistributedLogConstants.INVALID_TXID) {
                        LOG.error("Unrecoverable corruption has occurred in segment "
                            + l.toString() + " at path " + l.getZkPath()
                            + ". Unable to continue recovery.");
                        throw new IOException("Unrecoverable corruption,"
                            + " please check logs.");
                    } else if (endTxId == DistributedLogConstants.EMPTY_LEDGER_TX_ID) {
                        // TODO: Empty ledger - Ideally we should just remove it?
                        endTxId = l.getFirstTxId();
                    }

                    // Make the lock release symmetric by having this function acquire and
                    // release the lock and have complete and close only release the lock
                    // that's acquired in start log segment
                    completeAndCloseLogSegment(l.getZNodeName(), l.getFirstTxId(), endTxId, recordCount, false);
                    LOG.info("Recovered {} FirstTxId:{} LastTxId:{}", new Object[]{getFullyQualifiedName(), l.getFirstTxId(), endTxId});

                }
                if (lastLedgerRollingTimeMillis < 0) {
                    lastLedgerRollingTimeMillis = Utils.nowInMillis();
                }
                recovered = true;
            } finally {
                lock.release(DistributedReentrantLock.LockReason.RECOVER);
            }
        }
    }

    public void deleteLog() throws IOException {
        try {
            deleteLock.acquire(DistributedReentrantLock.LockReason.DELETELOG);
        } catch (LockingException lockExc) {
            throw new IOException("deleteLog could not acquire exclusive lock on the partition" + getFullyQualifiedName());
        }

        try {
            purgeAllLogs();
        } finally {
            deleteLock.release(DistributedReentrantLock.LockReason.DELETELOG);
        }

        try {
            lock.close();
            deleteLock.close();
            zooKeeperClient.get().exists(ledgerPath, false);
            zooKeeperClient.get().exists(maxTxIdPath, false);
            if (partitionRootPath.toLowerCase().contains("distributedlog")) {
                ZKUtil.deleteRecursive(zooKeeperClient.get(), partitionRootPath);
            } else {
                LOG.warn("Skip deletion of unrecognized ZK Path {}", partitionRootPath);
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + ledgerPath, ie);
            throw new DLInterruptedException("Interrupted while deleting " + ledgerPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error deleting" + ledgerPath + "entry in zookeeper", ke);
        }
    }

    public void purgeAllLogs()
        throws IOException {
        purgeLogsOlderThanInternal(-1);
    }

    public void purgeLogsOlderThan(long minTxIdToKeep)
        throws IOException {
        assert (minTxIdToKeep > 0);
        purgeLogsOlderThanInternal(minTxIdToKeep);
    }

    void purgeLogsOlderThanTimestamp(final long minTimestampToKeep, final long sanityCheckThreshold,
                                     final BookkeeperInternalCallbacks.GenericCallback<Void> callback) {
        assert (minTimestampToKeep < Utils.nowInMillis());
        getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, null, new BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>>() {
            @Override
            public void operationComplete(int rc, List<LogSegmentLedgerMetadata> result) {
                if (BKException.Code.OK != rc) {
                    LOG.error("Failed to get ledger list to purge for {} : ", getFullyQualifiedName(),
                        BKException.create(rc));
                    callback.operationComplete(rc, null);
                    return;
                }
                final List<LogSegmentLedgerMetadata> purgeList =
                    new ArrayList<LogSegmentLedgerMetadata>(result.size());
                boolean logTimestamp = true;
                for (LogSegmentLedgerMetadata l : result) {
                    if ((!l.isInProgress() && l.getCompletionTime() < minTimestampToKeep)) {
                        if (logTimestamp) {
                            LOG.info("Deleting ledgers older than {}", minTimestampToKeep);
                            logTimestamp = false;
                        }
                        purgeList.add(l);
                    }
                }
                // purge logs
                purgeLogs(purgeList, callback);
            }
        });
    }

    public void purgeLogsOlderThanInternal(long minTxIdToKeep)
        throws IOException {
        for (LogSegmentLedgerMetadata l : getLedgerList()) {
            if ((minTxIdToKeep < 0) ||
                (!l.isInProgress() && l.getLastTxId() < minTxIdToKeep)) {
                deleteLedgerAndMetadata(l);
            }
        }
    }

    private void purgeLogs(final List<LogSegmentLedgerMetadata> logs,
                           final BookkeeperInternalCallbacks.GenericCallback<Void> callback) {
        if (logs.size() == 0) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Nothing to purge.");
            }
            callback.operationComplete(BKException.Code.OK, null);
            return;
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Purging logs : {}.", logs);
        }
        final AtomicInteger numLogs = new AtomicInteger(logs.size());
        final BookkeeperInternalCallbacks.GenericCallback<Void> deleteCallback =
                new BookkeeperInternalCallbacks.GenericCallback<Void>() {
                    @Override
                    public void operationComplete(int rc, Void result) {
                        // we don't really care about the delete result right now
                        if (numLogs.decrementAndGet() == 0) {
                            callback.operationComplete(BKException.Code.OK, null);
                        }
                    }
                };
        for (LogSegmentLedgerMetadata l : logs) {
            deleteLedgerAndMetadata(l, deleteCallback);
        }
    }

    private void deleteLedgerAndMetadata(final LogSegmentLedgerMetadata ledgerMetadata,
                                         final BookkeeperInternalCallbacks.GenericCallback<Void> callback) {
        LOG.info("Deleting ledger for {}", ledgerMetadata);
        try {
            bookKeeperClient.get().asyncDeleteLedger(ledgerMetadata.getLedgerId(), new AsyncCallback.DeleteCallback() {
                @Override
                public void deleteComplete(int rc, Object ctx) {
                    if (BKException.Code.NoSuchLedgerExistsException == rc) {
                        LOG.warn("No ledger {} found to delete for {}.", ledgerMetadata.getLedgerId(), ledgerMetadata);
                        callback.operationComplete(rc, null);
                    } else if (BKException.Code.OK != rc) {
                        LOG.error("Couldn't delete ledger {} from bookkeeper : ",
                                ledgerMetadata.getLedgerId(), BKException.create(rc));
                        callback.operationComplete(rc, null);
                        return;
                    }
                    // after the ledger is deleted, we delete the metadata znode
                    try {
                        zooKeeperClient.get().delete(ledgerMetadata.getZkPath(), ledgerMetadata.getZkVersion(),
                            new org.apache.zookeeper.AsyncCallback.VoidCallback() {
                                @Override
                                public void processResult(int rc, String path, Object ctx) {
                                    if (KeeperException.Code.OK.intValue() != rc) {
                                        LOG.error("Couldn't purge {} : ", ledgerMetadata,
                                                KeeperException.create(KeeperException.Code.get(rc)));
                                        callback.operationComplete(BKException.Code.ZKException, null);
                                        return;
                                    }
                                    callback.operationComplete(BKException.Code.OK, null);
                                }
                            }, null);
                    } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                        LOG.error("Encountered zookeeper connection issue when purging {} : ", ledgerMetadata, e);
                        callback.operationComplete(BKException.Code.ZKException, null);
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted when purging {}.", ledgerMetadata);
                        callback.operationComplete(BKException.Code.InterruptedException, null);
                    }
                }
            }, null);
        } catch (IOException e) {
            callback.operationComplete(BKException.Code.BookieHandleNotAvailableException, null);
        }
    }

    private void deleteLedgerAndMetadata(LogSegmentLedgerMetadata ledgerMetadata) throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            doDeleteLedgerAndMetadata(ledgerMetadata);
            success = true;
        } finally {
            if (success) {
                deleteOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                deleteOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    private void doDeleteLedgerAndMetadata(LogSegmentLedgerMetadata ledgerMetadata) {
        final AtomicInteger rcHolder = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        deleteLedgerAndMetadata(ledgerMetadata, new BookkeeperInternalCallbacks.GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                rcHolder.set(rc);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while purging {}", ledgerMetadata);
        }
        if (BKException.Code.OK != rcHolder.get()) {
            LOG.error("Failed to purge {} : ", ledgerMetadata,
                    BKException.create(rcHolder.get()));
        }
    }

    public void close() throws IOException {
        if (lockAcquired) {
            lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
            lockAcquired = false;
        }
        lock.close();
        deleteLock.close();
        // close the zookeeper client & bookkeeper client after closing the lock
        super.close();
    }

    /**
     * Get the znode path for a finalize ledger
     */
    String completedLedgerZNode(long startTxId,
                                long endTxId) {
        return String.format("%s/logrecs_%018d_%018d",
            ledgerPath, startTxId, endTxId);
    }

    /**
     * Get the znode path for the inprogressZNode
     */
    String inprogressZNode(long startTxid) {
        return ledgerPath + "/" + inprogressZNodeName(startTxid);
    }

    String inprogressZNodeName(long startTxid) {
        return "inprogress_" + Long.toString(startTxid, 16);
    }

    String inprogressZNode(String inprogressZNodeName) {
        return ledgerPath + "/" + inprogressZNodeName;
    }
}
