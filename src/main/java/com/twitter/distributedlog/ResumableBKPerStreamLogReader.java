package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResumableBKPerStreamLogReader extends BKPerStreamLogReader implements Watcher {
    static final Logger LOG = LoggerFactory.getLogger(ResumableBKPerStreamLogReader.class);

    private static Counter resumeMisses = null;
    private static OpStatsLogger resumeHitStat = null;
    private static OpStatsLogger resumeSetWatcherStat = null;

    private final LogSegmentLedgerMetadata metadata;
    private String zkPath;
    private final BKLogPartitionReadHandler ledgerManager;
    private final ZooKeeperClient zkc;
    private LedgerDataAccessor ledgerDataAccessor;
    private boolean shouldResume = true;
    private AtomicBoolean watchSet = new AtomicBoolean(false);
    private AtomicBoolean nodeDeleteNotification = new AtomicBoolean(false);
    private long startBkEntry;
    private boolean openedWithNoRecovery = true;

    /**
     * Construct BookKeeper log record input stream.
     */
    ResumableBKPerStreamLogReader(BKLogPartitionReadHandler ledgerManager,
                                  ZooKeeperClient zkc,
                                  LedgerDataAccessor ledgerDataAccessor,
                                  LogSegmentLedgerMetadata metadata,
                                  boolean noBlocking,
                                  long startBkEntry,
                                  StatsLogger statsLogger) throws IOException {
        super(ledgerManager, metadata, statsLogger);
        this.metadata = metadata;
        this.ledgerManager = ledgerManager;
        this.zkc = zkc;
        this.zkPath = metadata.getZkPath();
        this.ledgerDataAccessor = ledgerDataAccessor;
        ledgerDescriptor = null;
        this.startBkEntry = startBkEntry;

        // Stats
        StatsLogger readerStatsLogger = statsLogger.scope("reader");
        if (null == resumeMisses) {
            resumeMisses = readerStatsLogger.getCounter("resume_miss");
        }

        if (null == resumeHitStat) {
            resumeHitStat = readerStatsLogger.getOpStatsLogger("resume_hit");
        }

        if (null == resumeSetWatcherStat) {
            resumeSetWatcherStat = readerStatsLogger.getOpStatsLogger("resume_setwatcher");
        }

        resume(!noBlocking);
    }

    /**
     * Construct BookKeeper log record input stream.
     */
    ResumableBKPerStreamLogReader(BKLogPartitionReadHandler ledgerManager,
                                  ZooKeeperClient zkc,
                                  LedgerDataAccessor ledgerDataAccessor,
                                  LogSegmentLedgerMetadata metadata,
                                  boolean noBlocking,
                                  StatsLogger statsLogger) throws IOException {
        this(ledgerManager, zkc, ledgerDataAccessor, metadata, noBlocking, 0, statsLogger);
    }

    synchronized public void resume(boolean shouldReadLAC) throws IOException {
        if (!shouldResume) {
            resumeMisses.inc();
            return;
        }

        Stopwatch stopwatch = new Stopwatch().start();
        try {
            doResume(shouldReadLAC);
            resumeHitStat.registerSuccessfulEvent(stopwatch.stop().elapsedTime(TimeUnit.MICROSECONDS));
        } catch (IOException ioe) {
            resumeHitStat.registerFailedEvent(stopwatch.stop().elapsedTime(TimeUnit.MICROSECONDS));
            throw ioe;
        }
    }

    synchronized public void doResume(boolean shouldReadLAC) throws IOException {
        if (isInProgress() && watchSet.compareAndSet(false, true)) {
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                if (null == zkc.get().exists(zkPath, this)) {
                    nodeDeleteNotification.set(true);
                }
                resumeSetWatcherStat.registerSuccessfulEvent(stopwatch.stop().elapsedTime(TimeUnit.MICROSECONDS));
            } catch (ZooKeeperClient.ZooKeeperConnectionException exc) {
                watchSet.set(false);
                LOG.debug("Error on setup latch due to zookeeper connection issue : ", exc);
                resumeSetWatcherStat.registerFailedEvent(stopwatch.stop().elapsedTime(TimeUnit.MICROSECONDS));
            } catch (KeeperException ke) {
                watchSet.set(false);
                LOG.debug("Error on setup latch due to zookeeper exception : ", ke);
                resumeSetWatcherStat.registerFailedEvent(stopwatch.stop().elapsedTime(TimeUnit.MICROSECONDS));
            } catch (InterruptedException ie) {
                watchSet.set(false);
                LOG.warn("Unable to setup latch", ie);
                resumeSetWatcherStat.registerFailedEvent(stopwatch.stop().elapsedTime(TimeUnit.MICROSECONDS));
                throw new DLInterruptedException("Interrupted on setup latch : ", ie);
            }
        }

        try {
            LedgerDescriptor h = ledgerDescriptor;
            if (null == ledgerDescriptor){
                h = ledgerManager.getHandleCache().openLedger(metadata, !isInProgress());
                positionInputStream(h, ledgerDataAccessor, startBkEntry);
            }  else {
                startBkEntry = lin.nextEntryToRead();
                if(nodeDeleteNotification.compareAndSet(true, false)) {
                    if (!ledgerDescriptor.isFenced()) {
                        ledgerManager.getHandleCache().closeLedger(ledgerDescriptor);
                        h = ledgerManager.getHandleCache().openLedger(metadata, true);
                    }
                    LOG.debug("{} Reading Last Add Confirmed {} after ledger close", startBkEntry,
                        ledgerManager.getHandleCache().getLastAddConfirmed(h));
                    inProgress = false;
                    positionInputStream(h, ledgerDataAccessor, startBkEntry);
                } else if (isInProgress()) {
                    if (shouldReadLAC && (startBkEntry > ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor))) {
                        ledgerManager.getHandleCache().tryReadLastConfirmed(ledgerDescriptor);
                    }
                    LOG.debug("{} : Advancing Last Add Confirmed {}", ledgerManager.getFullyQualifiedName(), ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor));
                }
            }

            resetExhausted();
            shouldResume = false;
        } catch (IOException e) {
            LOG.error("Could not open ledger {}", metadata.getLedgerId(), e);
            throw e;
        } catch (BKException e) {
            LOG.error("Could not open ledger {}", metadata.getLedgerId(), e);
            throw new IOException("Could not open ledger " + metadata.getLedgerId(), e);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on opening ledger " + metadata.getLedgerId(), ie);
        }
    }

    synchronized public void requireResume() {
        shouldResume = true;
    }

    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            nodeDeleteNotification.set(true);
            LOG.debug("{} Node Deleted", ledgerManager.getFullyQualifiedName());
            ledgerManager.notifyOnOperationComplete();
            return;
        } else if (event.getType() == Watcher.Event.EventType.None) {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                LOG.debug("Reconnected ...");
            } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
                LOG.info("ZK Session Expired");
            }
        } else {
            LOG.warn("Unexpected Watch {} Received for node {}", event, zkPath);
        }
        // Except when the node has been deleted, require the next resume call to
        // reset the watch as it may have been cleared by this invocation
        watchSet.set(false);
    }

    synchronized public LedgerReadPosition getNextLedgerEntryToRead() {
        assert (null != lin);
        return new LedgerReadPosition(metadata.getLedgerId(), metadata.getLedgerSequenceNumber(), lin.nextEntryToRead());
    }

    synchronized boolean reachedEndOfLogSegment() {
        return ((null != lin) && !inProgress && lin.reachedEndOfLedger());
    }

    synchronized public DLSN getNextDLSN() {
        if (null != lin) {
            return lin.getCurrentPosition();
        } else {
            return DLSN.InvalidDLSN;
        }
    }
}
