package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import org.apache.bookkeeper.client.BKException;
import org.apache.zookeeper.KeeperException;
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
    private AtomicBoolean watchSet = new AtomicBoolean(false);
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

        if (isInProgress() && watchSet.compareAndSet(false, true)) {
            try {
                if (null == zkc.get().exists(zkPath, this)) {
                    nodeDeleteNotification.set(true);
                }
            } catch (ZooKeeperClient.ZooKeeperConnectionException exc) {
                watchSet.set(false);
                LOG.debug("Error on setup latch due to zookeeper connection issue : ", exc);
            } catch (KeeperException ke) {
                watchSet.set(false);
                LOG.debug("Error on setup latch due to zookeeper exception : ", ke);
            } catch (InterruptedException ie) {
                watchSet.set(false);
                throw new DLInterruptedException("Interrupted on setup latch : ", ie);
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
        } catch (IOException e) {
            LOG.error("Could not open ledger {}", ledgerId, e);
            throw e;
        } catch (BKException e) {
            LOG.error("Could not open ledger {}", ledgerId, e);
            throw new IOException("Could not open ledger " + ledgerId, e);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on opening ledger " + ledgerId, ie);
        }
    }

    synchronized public void requireResume() {
        shouldResume = true;
    }

    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.None) {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                LOG.debug("Reconnected ...");
            } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
                watchSet.set(false);
            }
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
