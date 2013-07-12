package com.twitter.distributedlog;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKUtil;

import java.io.IOException;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKLogPartitionWriteHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    private static final int LAYOUT_VERSION = -1;

    private final DistributedReentrantLock lock;
    private final DistributedReentrantLock deleteLock;
    private final String maxTxIdPath;
    private final MaxTxId maxTxId;
    private final int ensembleSize;
    private final int quorumSize;
    private boolean lockAcquired;
    private LedgerHandle currentLedger = null;
    private long currentLedgerStartTxId = DistributedLogConstants.INVALID_TXID;
    private boolean recovered = false;

    private int bytesToInt(byte[] b) {
        assert b.length >= 4;
        return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
    }

    private byte[] intToBytes(int i) {
        return new byte[] {
            (byte)(i >> 24),
            (byte)(i >> 16),
            (byte)(i >> 8),
            (byte)(i) };
    }

    /**
     * Construct a Bookkeeper journal manager.
     */
    public BKLogPartitionWriteHandler(String name,
                                 PartitionId partition,
                                 DistributedLogConfiguration conf,
                                 URI uri,
                                 ZooKeeperClient zkcShared,
                                 BookKeeperClient bkcShared) throws IOException {
        super(name, partition, conf, uri, zkcShared, bkcShared);
        ensembleSize = conf.getEnsembleSize();
        quorumSize = conf.getQuorumSize();

        maxTxIdPath = partitionRootPath + "/maxtxid";
        String lockPath = partitionRootPath + "/lock";
        String versionPath = partitionRootPath + "/version";

        try {
            while (zkc.get().exists(partitionRootPath, false) == null) {
                try {
                    Utils.zkCreateFullPathOptimistic(zkc, partitionRootPath, new byte[] {'0'},
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

            Stat versionStat = zkc.get().exists(versionPath, false);
            if (versionStat != null) {
                byte[] d = zkc.get().getData(versionPath, false, versionStat);
                // There's only one version at the moment
                assert bytesToInt(d) == LAYOUT_VERSION;
            } else {
                zkc.get().create(versionPath, intToBytes(LAYOUT_VERSION),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            if (zkc.get().exists(ledgerPath, false) == null) {
                zkc.get().create(ledgerPath, new byte[] {'0'},
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new IOException("Error initializing zk", e);
        }

        lock = new DistributedReentrantLock(zkc, lockPath, conf.getLockTimeout() * 1000);
        deleteLock = new DistributedReentrantLock(zkc, lockPath, conf.getLockTimeout() * 1000);
        maxTxId = new MaxTxId(zkc, maxTxIdPath);
        lastLedgerRollingTimeMillis = Utils.nowInMillis();
    }

    /**
     * Start a new log segment in a BookKeeper ledger.
     * First ensure that we have the write lock for this journal.
     * Then create a ledger and stream based on that ledger.
     * The ledger id is written to the inprogress znode, so that in the
     * case of a crash, a recovery process can find the ledger we were writing
     * to when we crashed.
     * @param txId First transaction id to be written to the stream
     */
    public BKPerStreamLogWriter startLogSegment(long txId) throws IOException {
        checkLogExists();

        lock.acquire("StartLogSegment");
        lockAcquired = true;
        if (txId < maxTxId.get()) {
            LOG.error("We've already seen TxId {} the max TXId is {}", txId, maxTxId);
            LOG.error("Last Committed Ledger {}", getLedgerListDesc());
            throw new IOException("We've already seen " + txId
                + ". A new stream cannot be created with it");
        }
        try {
            if (currentLedger != null) {
                try {
                    // bookkeeper errored on last stream, clean up ledger
                    currentLedger.close();
                } catch (BKException.BKLedgerClosedException lce) {
                    LOG.debug("Ledger already closed {}", lce);
                }
            }
            currentLedger = bkc.get().createLedger(ensembleSize, quorumSize,
                BookKeeper.DigestType.CRC32,
                digestpw.getBytes());
            String znodePath = inprogressZNode(txId);
            LogSegmentLedgerMetadata l = new LogSegmentLedgerMetadata(znodePath,
                DistributedLogConstants.LAYOUT_VERSION,  currentLedger.getId(), txId);

            /*
             * Write the ledger metadata out to the inprogress ledger znode
             * This can fail if for some reason our write lock has
             * expired (@see DistributedExclusiveLock) and another process has managed to
             * create the inprogress znode.
             * In this case, throw an exception. We don't want to continue
             * as this would lead to a split brain situation.
             */
            l.write(zkc, znodePath);
            LOG.debug("Storing MaxTxId in startLogSegment " + znodePath + " " + txId);
            maxTxId.store(txId);
            currentLedgerStartTxId = txId;
            return new BKPerStreamLogWriter(conf, currentLedger, lock, txId);
        } catch (Exception e) {
            if (currentLedger != null) {
                try {
                    long id = currentLedger.getId();
                    currentLedger.close();
                    bkc.get().deleteLedger(id);
                } catch (Exception e2) {
                    //log & ignore, an IOException will be thrown soon
                    LOG.error("Error closing ledger", e2);
                }
            }
            throw new IOException("Error creating ledger", e);
        }
    }

    public boolean shouldStartNewSegment() {
        boolean shouldSwitch = (Utils.elapsedMSec(lastLedgerRollingTimeMillis) >
            (conf.getLogSegmentRollingIntervalMinutes() * 60 * 1000));

        if (shouldSwitch) {
            LOG.debug("Last Finalize Time: {} elapsed time (MSec): {}", lastLedgerRollingTimeMillis, Utils.elapsedMSec(lastLedgerRollingTimeMillis));
        }
        return shouldSwitch;
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     *
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    public void completeAndCloseLogSegment(long lastTxId)
        throws IOException {
        completeAndCloseLogSegment(currentLedgerStartTxId, lastTxId);
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     *
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    public void completeAndCloseLogSegment(long firstTxId, long lastTxId)
        throws IOException {
        checkLogExists();
        LOG.debug("Completing and Closing Log Segment" + firstTxId + " " + lastTxId);
        String inprogressPath = inprogressZNode(firstTxId);
        try {
            Stat inprogressStat = zkc.get().exists(inprogressPath, false);
            if (inprogressStat == null) {
                throw new IOException("Inprogress znode " + inprogressPath
                    + " doesn't exist");
            }

            lock.checkWriteLock();
            LogSegmentLedgerMetadata l
                =  LogSegmentLedgerMetadata.read(zkc, inprogressPath);

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
                l.write(zkc, pathForCompletedLedger);
            } catch (KeeperException.NodeExistsException nee) {
                if (!l.verify(zkc, pathForCompletedLedger)) {
                    throw new IOException("Node " + pathForCompletedLedger + " already exists"
                        + " but data doesn't match");
                }
            }
            LOG.debug("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressPath, lastTxId);
            maxTxId.store(lastTxId);
            zkc.get().delete(inprogressPath, inprogressStat.getVersion());
        } catch (KeeperException e) {
            throw new IOException("Error finalising ledger", e);
        } catch (InterruptedException ie) {
            throw new IOException("Error finalising ledger", ie);
        } finally {
            lock.release("CompleteAndClose");
        }
    }

    public void recoverIncompleteLogSegments() throws IOException {
        if (recovered) {
            return;
        }
        LOG.info("Initiating Recovery For {}", getFullyQualifiedName());
        lock.acquire("RecoverIncompleteSegments");
        synchronized (this) {
            try {
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

                    completeAndCloseLogSegment(l.getFirstTxId(), endTxId);
                    LOG.info("Recovered {} LastTxId:{}", getFullyQualifiedName(), endTxId);

                }
                if (lastLedgerRollingTimeMillis < 0) {
                    lastLedgerRollingTimeMillis = Utils.nowInMillis();
                }
                recovered = true;
            }  catch (IOException io) {
                throw new IOException("Couldn't get list of inprogress segments", io);
            } finally {
                if (lock.haveLock()) {
                    lock.release("RecoverIncompleteSegments");
                }
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
        } catch (DistributedReentrantLock.LockingException lockExc) {
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
            zkc.get().exists(ledgerPath, false);
            zkc.get().exists(maxTxIdPath, false);
            if (partitionRootPath.toLowerCase().contains("distributedlog")) {
                ZKUtil.deleteRecursive(zkc.get(), partitionRootPath);
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

    public void purgeLogsOlderThanTimestamp(long minTimestampToKeep, long sanityCheckThreshold)
        throws IOException {
        assert(minTimestampToKeep < Utils.nowInMillis());
        boolean logTimestamp = true;
        for (LogSegmentLedgerMetadata l : getLedgerList()) {
            if ((!l.isInProgress() && l.getCompletionTime() < minTimestampToKeep)) {
                try {
                    if (logTimestamp) {
                        LOG.info("Deleting ledgers older than {}", minTimestampToKeep);
                        logTimestamp = false;
                    }

                    // Something went wrong - leave the ledger around for debugging
                    //
                    if (conf.getSanityCheckDeletes() && (l.getCompletionTime() < sanityCheckThreshold)) {
                        LOG.warn("Found a ledger {} older than {}", l, sanityCheckThreshold);
                    }
                    else {
                        LOG.info("Deleting ledger for {}", l);
                        Stat stat = zkc.get().exists(l.getZkPath(), false);
                        bkc.get().deleteLedger(l.getLedgerId());
                        zkc.get().delete(l.getZkPath(), stat.getVersion());
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while purging " + l, ie);
                } catch (BKException bke) {
                    LOG.error("Couldn't delete ledger from bookkeeper", bke);
                } catch (KeeperException ke) {
                    LOG.error("Error deleting ledger entry in zookeeper", ke);
                }
            }
        }
    }

    public void purgeLogsOlderThanInternal(long minTxIdToKeep)
        throws IOException {
        for (LogSegmentLedgerMetadata l : getLedgerList()) {
            if ((minTxIdToKeep < 0) ||
                (!l.isInProgress() && l.getLastTxId() < minTxIdToKeep)) {
                try {
                    LOG.info("Deleting ledger for {}", l);
                    Stat stat = zkc.get().exists(l.getZkPath(), false);
                    bkc.get().deleteLedger(l.getLedgerId());
                    zkc.get().delete(l.getZkPath(), stat.getVersion());
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while purging " + l, ie);
                } catch (BKException bke) {
                    LOG.error("Couldn't delete ledger from bookkeeper", bke);
                } catch (KeeperException ke) {
                    LOG.error("Error deleting ledger entry in zookeeper", ke);
                }
            }
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
