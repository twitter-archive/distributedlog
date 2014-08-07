package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.exceptions.DLIllegalStateException;
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

    private final Counter resumeMisses;
    private final OpStatsLogger resumeHitStat;
    private final OpStatsLogger resumeSetWatcherStat;

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
        resumeMisses = readerStatsLogger.getCounter("resume_miss");
        resumeHitStat = readerStatsLogger.getOpStatsLogger("resume_hit");
        resumeSetWatcherStat = readerStatsLogger.getOpStatsLogger("resume_setwatcher");

        resume(true);
    }

    /**
     * Construct BookKeeper log record input stream.
     */
    ResumableBKPerStreamLogReader(BKLogPartitionReadHandler ledgerManager,
                                  ZooKeeperClient zkc,
                                  LedgerDataAccessor ledgerDataAccessor,
                                  LogSegmentLedgerMetadata metadata,
                                  StatsLogger statsLogger) throws IOException {
        this(ledgerManager, zkc, ledgerDataAccessor, metadata, 0, statsLogger);
    }

    public LogSegmentLedgerMetadata getLogSegmentLedgerMetadata() {
        return metadata;
    }

    synchronized public void resume(boolean shouldReadLAC) throws IOException {
        if (!shouldResume) {
            resumeMisses.inc();
            return;
        }

        Stopwatch stopwatch = new Stopwatch().start();
        try {
            doResume(shouldReadLAC);
            resumeHitStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
        } catch (IOException ioe) {
            resumeHitStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
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
                resumeSetWatcherStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } catch (ZooKeeperClient.ZooKeeperConnectionException exc) {
                watchSet.set(false);
                LOG.warn("Error on setup latch due to zookeeper connection issue : ", exc);
                resumeSetWatcherStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } catch (KeeperException ke) {
                watchSet.set(false);
                LOG.warn("Error on setup latch due to zookeeper exception : ", ke);
                resumeSetWatcherStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } catch (InterruptedException ie) {
                watchSet.set(false);
                LOG.warn("Unable to setup latch", ie);
                resumeSetWatcherStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                throw new DLInterruptedException("Interrupted on setup latch : ", ie);
            } catch (RuntimeException exc) {
                watchSet.set(false);
                resumeSetWatcherStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                // Outside of a bug we don't expect to be in this state
                throw new DLIllegalStateException("Unexpected runtime exception encountered", exc);
            }
        }

        try {
            LedgerDescriptor h = ledgerDescriptor;
            if (null == ledgerDescriptor){
                h = ledgerManager.getHandleCache().openLedger(metadata, !isInProgress());
                // if we retrieved the handle from the cache, then the last add confirmed may not be
                // up to date so check and update it. This can cause the reader to return null when
                // there actually is data. This makes no difference in practice as the next call to read
                // will resume again and there it will go through the else part of this condition
                // So this is mostly for tests
                if (shouldReadLAC && isInProgress() && (startBkEntry > ledgerManager.getHandleCache().getLastAddConfirmed(h))) {
                    ledgerManager.getHandleCache().tryReadLastConfirmed(h);
                }
                positionInputStream(h, ledgerDataAccessor, startBkEntry);
            }  else {
                startBkEntry = lin.nextEntryToRead();
                if(nodeDeleteNotification.compareAndSet(true, false)) {
                    if (!ledgerDescriptor.isFenced()) {
                        ledgerManager.getHandleCache().closeLedger(ledgerDescriptor);
                        h = ledgerManager.getHandleCache().openLedger(metadata, true);
                    }
                    if (enableTrace) {
                        LOG.info("{}: {} Reading Last Add Confirmed {} after ledger close",
                            new Object[] {ledgerManager.getFullyQualifiedName(), startBkEntry,
                                ledgerManager.getHandleCache().getLastAddConfirmed(h)});
                    } else {
                        LOG.debug("{}: {} Reading Last Add Confirmed {} after ledger close",
                            new Object[] {ledgerManager.getFullyQualifiedName(), startBkEntry,
                                ledgerManager.getHandleCache().getLastAddConfirmed(h)});
                    }
                    inProgress = false;
                    positionInputStream(h, ledgerDataAccessor, startBkEntry);
                } else if (isInProgress()) {
                    if (shouldReadLAC && (startBkEntry > ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor))) {
                        ledgerManager.getHandleCache().tryReadLastConfirmed(ledgerDescriptor);
                    }
                    if (enableTrace) {
                        LOG.info("{}: Advancing Last Add Confirmed {}",
                            ledgerManager.getFullyQualifiedName(), ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor));
                    } else {
                        LOG.debug("{}: Advancing Last Add Confirmed {}",
                            ledgerManager.getFullyQualifiedName(), ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor));
                    }
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

    synchronized public boolean canResume() throws IOException {
        return (null == ledgerDescriptor) ||
            (nodeDeleteNotification.get() ||
            !isInProgress() ||
            (lin.nextEntryToRead() < ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor)) );
    }

    synchronized public void requireResume() {
        shouldResume = true;
    }

    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            nodeDeleteNotification.set(true);
            if (enableTrace) {
                LOG.info("{}: Node {} Deleted", ledgerManager.getFullyQualifiedName(), event.getPath());
            } else {
                LOG.debug("{}: Node {} Deleted", ledgerManager.getFullyQualifiedName(), event.getPath());
            }
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
        return new LedgerReadPosition(metadata.getLedgerSequenceNumber(), lin.nextEntryToRead());
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

    @Override
    public String toString() {
        return String.format("Resumable Reader on stream %s,  LedgerInputStream %s", ledgerManager.getFullyQualifiedName(), lin);
    }
}
