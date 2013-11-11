package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;

class BKLogPartitionWriteHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    private static final int LAYOUT_VERSION = -1;

    private final DistributedReentrantLock lock;
    private final DistributedReentrantLock deleteLock;
    private final String maxTxIdPath;
    private final MaxTxId maxTxId;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private boolean lockAcquired;
    private LedgerHandle currentLedger = null;
    private long currentLedgerStartTxId = DistributedLogConstants.INVALID_TXID;
    private volatile boolean recovered = false;

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
                               StatsLogger statsLogger,
                               String clientId) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder, executorService, statsLogger);
        ensembleSize = conf.getEnsembleSize();
        writeQuorumSize = conf.getWriteQuorumSize();
        ackQuorumSize = conf.getAckQuorumSize();

        maxTxIdPath = partitionRootPath + "/maxtxid";
        String lockPath = partitionRootPath + "/lock";
        String versionPath = partitionRootPath + "/version";

        try {
            while (zooKeeperClient.get().exists(partitionRootPath, false) == null) {
                try {
                    Utils.zkCreateFullPathOptimistic(zooKeeperClient, partitionRootPath, new byte[]{'0'},
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    // HACK: Temporary for the test configuration used by test
                    try {
                        Thread.sleep(150);
                    } catch (Exception exc) {
                        // Ignore everything
                    }
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
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            if (zooKeeperClient.get().exists(ledgerPath, false) == null) {
                zooKeeperClient.get().create(ledgerPath, new byte[]{'0'},
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new IOException("Error initializing zk", e);
        }

        if (clientId.equals(DistributedLogConstants.UNKNOWN_CLIENT_ID)){
            try {
                clientId = InetAddress.getLocalHost().toString();
            } catch(Exception exc) {
                // Best effort
                clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
            }
        }

        lock = new DistributedReentrantLock(zooKeeperClient, lockPath, conf.getLockTimeout() * 1000, clientId);
        deleteLock = new DistributedReentrantLock(zooKeeperClient, lockPath, conf.getLockTimeout() * 1000, clientId);
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
        long start = MathUtils.nowInNano();
        boolean success = false;
        try {
            BKPerStreamLogWriter writer = doStartLogSegment(txId);
            success = true;
            return writer;
        } finally {
            long elapsed = MathUtils.elapsedMSec(start);
            if (success) {
                openOpStats.registerSuccessfulEvent(elapsed);
            } else {
                openOpStats.registerFailedEvent(elapsed);
            }
        }
    }

    private BKPerStreamLogWriter doStartLogSegment(long txId) throws IOException {
        checkLogExists();

        lock.acquire("StartLogSegment");
        lockAcquired = true;
        long highestTxIdWritten = maxTxId.get();
        if (txId < highestTxIdWritten) {
            if (highestTxIdWritten == DistributedLogConstants.MAX_TXID) {
                LOG.error("We've already marked the stream as ended and attempting to start a new log segment");
                throw new EndOfStreamException("Writing to a stream after it has been marked as completed");
            }
            else {
                LOG.error("We've already seen TxId {} the max TXId is {}", txId, maxTxId);
                LOG.error("Last Committed Ledger {}", getLedgerListDesc());
                throw new IOException("We've already seen " + txId
                    + ". A new stream cannot be created with it");
            }
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

            // For any active stream we will always make sure that there is at least one
            // active ledger (except when the stream first starts out). Therefore when we
            // see no ledger metadata for a stream, we assume that this is the first ledger
            // in the stream
            long ledgerSeqNo = DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO;

            if (conf.getDLLedgerMetadataLayoutVersion() >=
                DistributedLogConstants.FIRST_LEDGER_METADATA_VERSION_FOR_LEDGER_SEQNO) {
                List<LogSegmentLedgerMetadata> ledgerListDesc = getLedgerListDesc();
                ledgerSeqNo = DistributedLogConstants.FIRST_LEDGER_SEQNO;
                if (!ledgerListDesc.isEmpty()) {
                    ledgerSeqNo = ledgerListDesc.get(0).getLedgerSequenceNumber();
                }
            }

            LogSegmentLedgerMetadata l = new LogSegmentLedgerMetadata(znodePath, conf.getDLLedgerMetadataLayoutVersion(), currentLedger.getId(), txId, ledgerSeqNo);

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
            return new BKPerStreamLogWriter(conf, currentLedger, lock, txId, ledgerSeqNo, executorService, statsLogger);
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
            throw new IOException("Error creating ledger", e);
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
    public void completeAndCloseLogSegment(long lastTxId)
        throws IOException {
        completeAndCloseLogSegment(currentLedgerStartTxId, lastTxId, true);
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
    public void completeAndCloseLogSegment(long firstTxId, long lastTxId)
        throws IOException {
        completeAndCloseLogSegment(firstTxId, lastTxId, true);
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
    public void completeAndCloseLogSegment(long firstTxId, long lastTxId, boolean shouldReleaseLock)
            throws IOException {
        long start = MathUtils.nowInNano();
        boolean success = false;
        try {
            doCompleteAndCloseLogSegment(firstTxId, lastTxId, shouldReleaseLock);
            success = true;
        } finally {
            long elapsed = MathUtils.elapsedMSec(start);
            if (success) {
                closeOpStats.registerSuccessfulEvent(elapsed);
            } else {
                closeOpStats.registerFailedEvent(elapsed);
            }
        }
    }

    private void doCompleteAndCloseLogSegment(long firstTxId, long lastTxId, boolean shouldReleaseLock)
            throws IOException {
        checkLogExists();
        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        String inprogressPath = inprogressZNode(firstTxId);
        boolean acquiredLocally = false;
        try {
            Stat inprogressStat = zooKeeperClient.get().exists(inprogressPath, false);
            if (inprogressStat == null) {
                throw new IOException("Inprogress znode " + inprogressPath
                    + " doesn't exist");
            }

            acquiredLocally = lock.checkWriteLock();
            LogSegmentLedgerMetadata l
                = LogSegmentLedgerMetadata.read(zooKeeperClient, inprogressPath,
                    conf.getDLLedgerMetadataLayoutVersion());

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

            lastLedgerRollingTimeMillis = l.finalizeLedger(lastTxId);
            String pathForCompletedLedger = completedLedgerZNode(firstTxId, lastTxId);
            try {
                l.write(zooKeeperClient, pathForCompletedLedger);
            } catch (KeeperException.NodeExistsException nee) {
                if (!l.checkEquivalence(zooKeeperClient, pathForCompletedLedger)) {
                    throw new IOException("Node " + pathForCompletedLedger + " already exists"
                        + " but data doesn't match");
                }
            }
            LOG.debug("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressPath, lastTxId);
            maxTxId.store(lastTxId);

            if (FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_FinalizeLedgerBeforeDelete)) {
                return;
            }

            zooKeeperClient.get().delete(inprogressPath, inprogressStat.getVersion());
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when finalising stream " + partitionRootPath, e);
        } catch (KeeperException.NoNodeException e) {
            throw new IOException("Error when finalising stream " + partitionRootPath, e);
        } catch (KeeperException e) {
            throw new IOException("Error when finalising stream " + partitionRootPath, e);
        } finally {
            if (acquiredLocally || (shouldReleaseLock && lockAcquired)) {
                lock.release("CompleteAndClose");
                lockAcquired = false;
            }
        }
    }

    public void recoverIncompleteLogSegments() throws IOException {
        long start = MathUtils.nowInNano();
        boolean success = false;
        try {
            doRecoverIncompleteLogSegments();
            success = true;
        } finally {
            long elapsed = MathUtils.elapsedMSec(start);
            if (success) {
                recoverOpStats.registerSuccessfulEvent(elapsed);
            } else {
                recoverOpStats.registerFailedEvent(elapsed);
            }
        }
    }

    private void doRecoverIncompleteLogSegments() throws IOException {
        if (recovered) {
            return;
        }
        LOG.info("Initiating Recovery For {}", getFullyQualifiedName());
        lock.acquire("RecoverIncompleteSegments");
        synchronized (this) {
            try {
                if (recovered) {
                    return;
                }
                for (LogSegmentLedgerMetadata l : getLedgerList()) {
                    if (!l.isInProgress()) {
                        continue;
                    }
                    long endTxId = recoverLastTxId(l, true);
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
                    completeAndCloseLogSegment(l.getFirstTxId(), endTxId, false);
                    LOG.info("Recovered {} LastTxId:{}", getFullyQualifiedName(), endTxId);

                }
                if (lastLedgerRollingTimeMillis < 0) {
                    lastLedgerRollingTimeMillis = Utils.nowInMillis();
                }
                recovered = true;
            } catch (IOException io) {
                throw new IOException("Couldn't get list of inprogress segments", io);
            } finally {
                lock.release("RecoverIncompleteSegments");
            }
        }
    }

    public void deleteLog() throws IOException {
        try {
            checkLogExists();
        } catch (IOException exc) {
            return;
        }

        try {
            deleteLock.acquire("DeleteLog");
        } catch (LockingException lockExc) {
            throw new IOException("deleteLog could not acquire exclusive lock on the partition" + getFullyQualifiedName());
        }

        try {
            purgeAllLogs();
        } finally {
            deleteLock.release("DeleteLog");
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
                        new ArrayList<LogSegmentLedgerMetadata>(result.size() - 1);
                boolean logTimestamp = true;
                for (int iterator = 0; iterator < (result.size() - 1); iterator++) {
                    LogSegmentLedgerMetadata l = result.get(iterator);
                    if ((!l.isInProgress() && l.getCompletionTime() < minTimestampToKeep)) {
                        if (logTimestamp) {
                            LOG.info("Deleting ledgers older than {}", minTimestampToKeep);
                            logTimestamp = false;
                        }

                        // Something went wrong - leave the ledger around for debugging
                        //
                        if (conf.getSanityCheckDeletes() && (l.getCompletionTime() < sanityCheckThreshold)) {
                            LOG.warn("Found a ledger {} older than {}", l, sanityCheckThreshold);
                        } else {
                            purgeList.add(l);
                        }
                    }
                }
                // purge logs
                purgeLogs(purgeList, callback);
            }
        });
    }

    public void purgeLogsOlderThanInternal(long minTxIdToKeep)
        throws IOException {
        List<LogSegmentLedgerMetadata> ledgerList = getLedgerList();

        // If we are deleting the log we can remove the last entry else we must retain
        // at least one ledger for the stream
        int numEntriesToProcess = ledgerList.size() - 1;

        if (minTxIdToKeep < 0) {
            numEntriesToProcess++;
        }

        for (int iterator = 0; iterator < numEntriesToProcess; iterator++) {
            LogSegmentLedgerMetadata l = ledgerList.get(iterator);
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
        long start = MathUtils.nowInNano();
        boolean success = false;
        try {
            doDeleteLedgerAndMetadata(ledgerMetadata);
            success = true;
        } finally {
            long elapsed = MathUtils.elapsedMSec(start);
            if (success) {
                deleteOpStats.registerSuccessfulEvent(elapsed);
            } else {
                deleteOpStats.registerFailedEvent(elapsed);
            }
        }
    }

    private void doDeleteLedgerAndMetadata(LogSegmentLedgerMetadata ledgerMetadata) throws IOException {
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
        try {
            super.close();
            if (lockAcquired) {
                lock.release("WriteHandlerClose");
                lockAcquired = false;
            }
        } catch (Exception e) {
            throw new IOException("Couldn't close zookeeper client", e);
        }
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
        return ledgerPath + "/inprogress_" + Long.toString(startTxid, 16);
    }
}
