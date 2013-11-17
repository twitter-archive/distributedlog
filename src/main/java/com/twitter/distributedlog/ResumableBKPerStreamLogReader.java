package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResumableBKPerStreamLogReader extends BKPerStreamLogReader implements Watcher {
    static final Logger LOG = LoggerFactory.getLogger(ResumableBKPerStreamLogReader.class);

    private final long ledgerId;
    private String zkPath;
    private final BKLogPartitionReadHandler ledgerManager;
    private final ZooKeeperClient zkc;
    private LedgerDataAccessor ledgerDataAccessor;
    private boolean shouldResume = true;
    private boolean watchSet = false;
    private AtomicBoolean nodeDeleteNotification = new AtomicBoolean(false);

    /**
     * Construct BookKeeper log record input stream.
     */
    ResumableBKPerStreamLogReader(BKLogPartitionReadHandler ledgerManager,
                                  ZooKeeperClient zkc,
                                  LedgerDataAccessor ledgerDataAccessor,
                                  LogSegmentLedgerMetadata metadata) throws IOException {
        super(metadata);
        this.ledgerId = metadata.getLedgerId();
        this.ledgerManager = ledgerManager;
        this.zkc = zkc;
        this.zkPath = metadata.getZkPath();
        this.ledgerDataAccessor = ledgerDataAccessor;
        ledgerDescriptor = null;
        resume();
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
            long startBkEntry = 0;
            LedgerDescriptor h;
            if (null == ledgerDescriptor){
                h = ledgerManager.getHandleCache().openLedger(ledgerId, !isInProgress());
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
            shouldResume = false;
        } catch (Exception e) {
            LOG.error("Could not open ledger for partition " + ledgerId, e);
            throw new IOException("Could not open ledger for " + ledgerId, e);
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
        return new LedgerReadPosition(ledgerId, lin.nextEntryToRead());
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
