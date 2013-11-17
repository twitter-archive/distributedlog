package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResumableBKPerStreamLogReader extends BKPerStreamLogReader implements Watcher {
    static final Logger LOG = LoggerFactory.getLogger(ResumableBKPerStreamLogReader.class);

    private final LogSegmentLedgerMetadata metadata;
    private String zkPath;
    private final BKLogPartitionReadHandler ledgerManager;
    private final ZooKeeperClient zkc;
    private LedgerDataAccessor ledgerDataAccessor;
    private boolean shouldResume = true;
    private boolean watchSet = false;
    private AtomicBoolean nodeDeleteNotification = new AtomicBoolean(false);
    private long startBkEntry;
    protected final boolean noBlocking;
    private boolean openedWithNoRecovery = true;

    /**
     * Construct BookKeeper log record input stream.
     */
    ResumableBKPerStreamLogReader(BKLogPartitionReadHandler ledgerManager,
                                  ZooKeeperClient zkc,
                                  LedgerDataAccessor ledgerDataAccessor,
                                  LogSegmentLedgerMetadata metadata,
                                  boolean noBlocking,
                                  long startBkEntry) throws IOException {
        super(metadata, noBlocking);
        this.metadata = metadata;
        this.ledgerManager = ledgerManager;
        this.zkc = zkc;
        this.zkPath = metadata.getZkPath();
        this.ledgerDataAccessor = ledgerDataAccessor;
        ledgerDescriptor = null;
        this.startBkEntry = startBkEntry;
        this.noBlocking = noBlocking;
        resume();
    }

    /**
     * Construct BookKeeper log record input stream.
     */
    ResumableBKPerStreamLogReader(BKLogPartitionReadHandler ledgerManager,
                                  ZooKeeperClient zkc,
                                  LedgerDataAccessor ledgerDataAccessor,
                                  LogSegmentLedgerMetadata metadata,
                                  boolean noBlocking) throws IOException {
        this(ledgerManager, zkc, ledgerDataAccessor, metadata, noBlocking, 0);
    }

    synchronized public void resume() throws IOException {
        if (!shouldResume) {
            return;
        }

        if (isInProgress() && !watchSet) {
            try {
                if (null == zkc.get().exists(zkPath, this)) {
                    nodeDeleteNotification.set(true);
                }
                watchSet = true;
            } catch (Exception exc) {
                watchSet = false;
                LOG.debug("Unable to setup latch", exc);
            }
        }

        try {
            LedgerDescriptor h;
            if (null == ledgerDescriptor){
                h = ledgerManager.getHandleCache().openLedger(metadata, !isInProgress());
            }  else {
                startBkEntry = lin.nextEntryToRead();
                if(nodeDeleteNotification.compareAndSet(true, false)) {
                    ledgerManager.getHandleCache().readLastConfirmed(ledgerDescriptor);
                    LOG.debug("{} Reading Last Add Confirmed {} after ledger close", startBkEntry, ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor));
                    inProgress = false;
                } else if (isInProgress()) {
                    if (startBkEntry > ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor)) {
                        ledgerManager.getHandleCache().readLastConfirmed(ledgerDescriptor);
                    }
                    LOG.debug("Advancing Last Add Confirmed {}", ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor));
                }
                h = ledgerDescriptor;
            }

            positionInputStream(h, ledgerDataAccessor, startBkEntry);
            startBkEntry = 0;
            shouldResume = false;
        } catch (Exception e) {
            LOG.error("Could not open ledger for partition " + metadata.getLedgerId(), e);
            throw new IOException("Could not open ledger for " + metadata.getLedgerId(), e);
        }
    }

    synchronized public void requireResume() {
        shouldResume = true;
    }

    public void process(WatchedEvent event) {
        if ((event.getType() == Watcher.Event.EventType.None)
            && (event.getState() == Watcher.Event.KeeperState.SyncConnected)) {
            LOG.debug("Reconnected ...");
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            nodeDeleteNotification.set(true);
            LOG.debug("Node Deleted");
        }
    }

    synchronized public LedgerReadPosition getNextLedgerEntryToRead() {
        assert (null != lin);
        return new LedgerReadPosition(metadata.getLedgerId(), lin.nextEntryToRead());
    }

    synchronized boolean reachedEndOfLogSegment() {
        if (null == lin) {
            return false;
        }

        if (inProgress) {
            return false;
        }

        return lin.reachedEndOfLedger();
    }

    synchronized public DLSN getNextDLSN() {
        if (null != lin) {
            return lin.getCurrentPosition();
        } else {
            return DLSN.InvalidDLSN;
        }
    }

    synchronized boolean reachedEndOfLogSegment() {
        if (null == lin) {
            return false;
        }

        if (inProgress) {
            return false;
        }

        return lin.reachedEndOfLedger();
    }
}
