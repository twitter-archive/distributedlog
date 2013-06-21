package com.twitter.distributedlog;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;

import java.util.Enumeration;
import java.util.List;
import java.io.IOException;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BKLogPartitionReadHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    private static final int LAYOUT_VERSION = -1;
    private LedgerDataAccessor ledgerDataAccessor = null;
    private final LedgerHandleCache handleCache;

    private ReadAheadThread readAheadThread = null;

    /**
     * Construct a Bookkeeper journal manager.
     */
    public BKLogPartitionReadHandler(String name,
                                 PartitionId partition,
                                 DistributedLogConfiguration conf,
                                 URI uri,
                                 ZooKeeperClient zkcShared,
                                 BookKeeper bkcShared) throws IOException {
        super(name, partition, conf, uri, zkcShared, bkcShared);
        handleCache = new LedgerHandleCache(this.bkc, this.digestpw);
        ledgerDataAccessor = new LedgerDataAccessor(handleCache);
    }

    public ResumableBKPerStreamLogReader getInputStream(long fromTxId, boolean inProgressOk)
        throws IOException {
        checkLogExists();
        return getInputStream(fromTxId, inProgressOk, true);
    }

    public ResumableBKPerStreamLogReader getInputStream(long fromTxId, boolean inProgressOk, boolean fException)
        throws IOException {
        boolean logExists = false;
        try {
            if (null != zkc.get().exists(ledgerPath, false)) {
                logExists = true;
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + ledgerPath, ie);
            throw new LogEmptyException("Log " + name + ":" + partition + " is empty");
        } catch (KeeperException ke) {
            LOG.error("Error deleting" + ledgerPath + "entry in zookeeper", ke);
            throw new LogEmptyException("Log " + name + ":" + partition + " is empty");
        }

        if (logExists) {
            for (LogSegmentLedgerMetadata l : getLedgerList()) {
                LOG.debug("Inspecting Ledger: {}", l);
                long lastTxId = l.getLastTxId();
                if (l.isInProgress()) {
                    if (!inProgressOk) {
                        continue;
                    }

                    try {
                        lastTxId = recoverLastTxId(l, false);
                    } catch (IOException exc) {
                        lastTxId = l.getFirstTxId();
                        LOG.info("Reading beyond flush point");
                    }

                    if (lastTxId == DistributedLogConstants.INVALID_TXID) {
                        lastTxId = l.getFirstTxId();
                    }
                }

                if (fromTxId <= lastTxId) {
                    try {
                        ResumableBKPerStreamLogReader s
                            = new ResumableBKPerStreamLogReader(this, zkc, ledgerDataAccessor, l);
                        if (s.skipTo(fromTxId)) {
                            return s;
                        } else {
                            return null;
                        }
                    } catch (Exception e) {
                        LOG.error("Could not open ledger for partition " + partition + " for startTxId " + fromTxId, e);
                        throw new IOException("Could not open ledger for " + fromTxId, e);
                    }
                } else {
                    ledgerDataAccessor.removeLedger(l.getLedgerId());
                }
            }
        }
        if (fException) {
            throw new IOException("No ledger for fromTxnId " + fromTxId + " found.");
        }
        else {
            return null;
        }
    }


    public void close() throws IOException {
        try {
            if (null != readAheadThread) {
                readAheadThread.shutdown();
            }

            if (null != ledgerDataAccessor) {
                ledgerDataAccessor.clear();
            }

            super.close();

        } catch (Exception e) {
            throw new IOException("Couldn't close zookeeper client", e);
        }
    }

    private void setWatcherOnLedgerRoot(Watcher watcher) throws IOException,KeeperException,InterruptedException {
        zkc.get().getChildren(ledgerPath, watcher);
    }

    public boolean startReadAhead(LedgerReadPosition startPosition) {
        if (null == readAheadThread) {
            readAheadThread = new ReadAheadThread(partition,
                this,
                startPosition,
                ledgerDataAccessor,
                conf.getReadAheadBatchSize(),
                conf.getReadAheadMaxEntries(),
                conf.getReadAheadWaitTime());
            readAheadThread.start();
            return true;
        } else {
            return false;
        }
    }

    public LedgerDataAccessor getLedgerDataAccessor() {
        return ledgerDataAccessor;
    }

    public LedgerHandleCache getHandleCache() {
        return handleCache;
    }

    private class ReadAheadThread extends Thread implements Watcher {
        volatile boolean running = true;

        private final PartitionId partition;
        private final BKLogPartitionReadHandler bkLedgerManager;
        private boolean reInitializeMetadata = true;
        private LedgerReadPosition nextReadPosition;
        private LogSegmentLedgerMetadata currentMetadata = null;
        private int currentMetadataIndex;
        private LedgerDescriptor currentLH;
        private final LedgerDataAccessor ledgerDataAccessor;
        private List<LogSegmentLedgerMetadata> ledgerList;
        private final long readAheadBatchSize;
        private final long readAheadMaxEntries;
        private final long readAheadWaitTime;
        private Long notificationObject = new Long(0);

        public ReadAheadThread(PartitionId partition,
                               BKLogPartitionReadHandler ledgerManager,
                               LedgerReadPosition startPosition,
                               LedgerDataAccessor ledgerDataAccessor,
                               int readAheadBatchSize,
                               int readAheadMaxEntries,
                               int readAheadWaitTime) {
            super("ReadAheadThread");
            this.bkLedgerManager = ledgerManager;
            this.partition = partition;
            this.nextReadPosition = startPosition;
            this.ledgerDataAccessor = ledgerDataAccessor;
            this.readAheadBatchSize = readAheadBatchSize;
            this.readAheadMaxEntries = readAheadMaxEntries;
            this.readAheadWaitTime = readAheadWaitTime;
            ledgerDataAccessor.setNotificationObject(notificationObject);
        }

        @Override
        public void run() {
            LOG.info("ReadAheadThread Thread started for partition {}", partition);

            while(running) {
                try {
                    boolean inProgressChanged = false;
                    if (reInitializeMetadata || (null == currentMetadata)) {
                        reInitializeMetadata = false;
                        try {
                            bkLedgerManager.setWatcherOnLedgerRoot(this);
                        } catch (Exception exc) {
                            reInitializeMetadata = true;
                            LOG.debug("Unable to setup watcher", exc);
                        }
                        ledgerList = bkLedgerManager.getLedgerList();
                        for (int i = 0; i < ledgerList.size(); i++) {
                            LogSegmentLedgerMetadata l = ledgerList.get(i);
                            if (l.getLedgerId() == nextReadPosition.getLedgerId()) {
                                if (currentMetadata != null) {
                                    inProgressChanged = currentMetadata.isInProgress() && !l.isInProgress();
                                }
                                currentMetadata = l;
                                currentMetadataIndex = i;
                                break;
                            }
                        }
                    }

                    if (currentMetadata.isInProgress()) { // we don't want to fence the current journal
                        if (null == currentLH) {
                            currentLH = bkLedgerManager.getHandleCache().openLedger(currentMetadata.getLedgerId(), false);

                        } else {
                            long lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                            if (lastAddConfirmed < nextReadPosition.getEntryId()) {
                                bkLedgerManager.getHandleCache().readLastConfirmed(currentLH);
                                LOG.debug("Advancing Last Add Confirmed {}", bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH));
                            }
                        }
                    } else {
                        if (null != currentLH) {
                            if (inProgressChanged) {
                                bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                currentLH = null;
                            } else if (nextReadPosition.getEntryId() > bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH)) {
                                bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                currentLH = null;
                                currentMetadata = null;
                                if (currentMetadataIndex+1 < ledgerList.size()) {
                                    currentMetadata = ledgerList.get(++currentMetadataIndex);
                                    nextReadPosition.positionOnNewLedger(currentMetadata.getLedgerId());
                                }
                            }
                        } else {
                            currentLH = bkLedgerManager.getHandleCache().openLedger(currentMetadata.getLedgerId(), true);
                        }
                    }

                    boolean cacheFull = false;

                    if (null != currentLH) {
                        long lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                        while (lastAddConfirmed >= nextReadPosition.getEntryId()) {
                            Enumeration<LedgerEntry> entries
                                = bkLedgerManager.getHandleCache().readEntries(currentLH, nextReadPosition.getEntryId(),
                                Math.min(lastAddConfirmed, (nextReadPosition.getEntryId() + readAheadBatchSize - 1)));
                            if (entries.hasMoreElements()) {
                                nextReadPosition.advance();
                                LedgerEntry e = entries.nextElement();
                                ledgerDataAccessor.set(new LedgerReadPosition(e.getLedgerId(), e.getEntryId()), e);
                            }
                            if (ledgerDataAccessor.getNumCacheEntries() > readAheadMaxEntries) {
                                cacheFull = true;
                                break;
                            }
                        }
                    }

                    if (cacheFull || ((null != currentMetadata) && currentMetadata.isInProgress())) {
                        synchronized(notificationObject) {
                            notificationObject.wait(readAheadWaitTime);
                        }
                    }
                } catch (InterruptedException exc) {
                    running = false;
                  // exit silently
                } catch (IOException ioExc) {
                    LOG.debug("ReadAhead Thread Encountered an exception ", ioExc);
                    reInitializeMetadata = true;
                } catch (Exception e) {
                    LOG.error("ReadAhead Thread Encountered an unexpected exception ", e);
                    reInitializeMetadata = true;
                }
            }
            LOG.info("ReadAheadThread Thread stopped for partition {}", partition);
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }

        synchronized public void process(WatchedEvent event) {
            if ((event.getType() == Watcher.Event.EventType.None)
                && (event.getState() == Watcher.Event.KeeperState.SyncConnected)) {
                LOG.debug("Reconnected ...");
            } else {
                reInitializeMetadata = true;
                synchronized(notificationObject) {
                    notificationObject.notifyAll();
                }
                LOG.debug("Read ahead node changed");
            }
        }

    }
}
