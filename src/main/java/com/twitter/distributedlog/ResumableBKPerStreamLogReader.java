package com.twitter.distributedlog;

import java.io.IOException;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
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
    private boolean reInitializeMetadata = true;

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

        if (isInProgress() && reInitializeMetadata) {
            reInitializeMetadata = false;
            try {
                zkc.get().exists(zkPath, this);
            } catch (Exception exc) {
                LOG.debug("Unable to setup latch", exc);
            }
            for (LogSegmentLedgerMetadata l : ledgerManager.getLedgerList()) {
                if (l.getLedgerId() == ledgerId) {
                    if (!l.isInProgress()) {
                       inProgress = false;
                        try {
                            zkc.get().exists(zkPath, false);
                        } catch (Exception exc) {
                            LOG.debug("Unable to remove latch", exc);
                        }
                        lastTxId = l.getLastTxId();
                    }
                    break;
                }
            }
        }
        LedgerDescriptor h;
        long startBkEntry = 0;
        if (null != ledgerDescriptor) {
            startBkEntry = lin.nextEntryToRead();
        }

        try {
            if (isInProgress()) { // we don't want to fence the current journal
                if (null == ledgerDescriptor) {
                    h = ledgerManager.getHandleCache().openLedger(ledgerId, false);
                } else {
                    if (startBkEntry > ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor)) {
                        ledgerManager.getHandleCache().readLastConfirmed(ledgerDescriptor);
                    }
                    LOG.debug("Advancing Last Add Confirmed {}", ledgerManager.getHandleCache().getLastAddConfirmed(ledgerDescriptor));
                    h = ledgerDescriptor;
                }
            } else {
                if (null != ledgerDescriptor) {
                    ledgerManager.getHandleCache().closeLedger(ledgerDescriptor);
                    ledgerDescriptor = null;
                }
                h = ledgerManager.getHandleCache().openLedger(ledgerId, true);
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

    synchronized public void process(WatchedEvent event) {
        if ((event.getType() == Watcher.Event.EventType.None)
            && (event.getState() == Watcher.Event.KeeperState.SyncConnected)) {
            LOG.debug("Reconnected ...");
        } else {
            reInitializeMetadata = true;
            LOG.debug("Node Changed");
        }
    }

    synchronized public LedgerReadPosition getNextLedgerEntryToRead() {
        assert (null != lin);
        return new LedgerReadPosition(ledgerId, lin.nextEntryToRead());
    }

    synchronized public void setReadAheadCache() {
        ledgerDataAccessor = ledgerManager.getLedgerDataAccessor();
        if (null != lin) {
            lin.setLedgerDataAccessor(ledgerDataAccessor);
        }
    }
}
