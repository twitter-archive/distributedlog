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
    private AtomicBoolean reInitializeMetadata = new AtomicBoolean(true);
    private long startBkEntry;
    protected final boolean noBlocking;

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

        if (isInProgress() && reInitializeMetadata.compareAndSet(true, false)) {
            try {
                zkc.get().exists(zkPath, this);
            } catch (Exception exc) {
                reInitializeMetadata.set(true);
                LOG.debug("Unable to setup latch", exc);
            }
            for (LogSegmentLedgerMetadata l : ledgerManager.getLedgerList()) {
                if (l.getLedgerId() == metadata.getLedgerId()) {
                    if (!l.isInProgress()) {
                        inProgress = false;
                        try {
                            zkc.get().exists(zkPath, false);
                        } catch (Exception exc) {
                            LOG.debug("Unable to remove latch", exc);
                        }
                    }
                    break;
                }
            }
        }
        LedgerDescriptor h;
        if (null != ledgerDescriptor) {
            startBkEntry = lin.nextEntryToRead();
        }

        try {
            if (isInProgress()) { // we don't want to fence the current journal
                if (null == ledgerDescriptor) {
                    h = ledgerManager.getHandleCache().openLedger(metadata, false);
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
                h = ledgerManager.getHandleCache().openLedger(metadata, true);
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
        } else {
            reInitializeMetadata.set(true);
            LOG.debug("Node Changed");
        }
    }

    synchronized public LedgerReadPosition getNextLedgerEntryToRead() {
        assert (null != lin);
        return new LedgerReadPosition(metadata.getLedgerId(), lin.nextEntryToRead());
    }

    synchronized public void setReadAheadCache() {
        ledgerDataAccessor = ledgerManager.getLedgerDataAccessor();
        if (null != lin) {
            lin.setLedgerDataAccessor(ledgerDataAccessor);
        }
    }
}
