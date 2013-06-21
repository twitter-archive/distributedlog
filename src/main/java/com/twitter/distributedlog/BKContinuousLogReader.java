package com.twitter.distributedlog;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKContinuousLogReader implements LogReader, ZooKeeperClient.ZooKeeperSessionExpireNotifier {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReader.class);

    private final BKDistributedLogManager bkDistributedLogManager;
    private final PartitionId partition;
    private final long startTxId;
    private long lastTxId;
    private final BKLogPartitionReadHandler bkLedgerManager;
    private ResumableBKPerStreamLogReader currentReader = null;
    private final boolean readAheadEnabled;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;


    public BKContinuousLogReader (BKDistributedLogManager bkdlm, PartitionId partition, long startTxId, boolean readAheadEnabled) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(partition);
        this.partition = partition;
        this.startTxId = startTxId;
        lastTxId = startTxId -1;
        this.readAheadEnabled = readAheadEnabled;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
    }

    /**
     * Close the stream.
     * @throws IOException if an error occurred while closing
     */
    @Override
    public void close() throws IOException {
        if (null != currentReader) {
            currentReader.close();
        }

        if (null != bkLedgerManager) {
            bkLedgerManager.close();
        }
        bkDistributedLogManager.unregister(sessionExpireWatcher);
    }

    /**
     * Read the next log record from the stream
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    @Override
    public LogRecord readNext(boolean shouldBlock) throws IOException {
        if (shouldBlock) {
            throw new IOException("Not Yet Implemented");
        }

        checkClosedOrInError("LogReader#readNext");

        LogRecord record = null;
        boolean advancedOnce = false;
        while (!advancedOnce) {
            if (null == currentReader) {
                LOG.debug("Opening reader on partition {} starting at TxId: {}", partition, (lastTxId+1));
                currentReader = bkLedgerManager.getInputStream(lastTxId + 1, true, false);
                if (null != currentReader) {
                    if(readAheadEnabled && bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead())) {
                        bkLedgerManager.getLedgerDataAccessor().setReadAheadEnabled(true);
                    }
                    LOG.debug("Opened reader on partition {} starting at TxId: {}", partition, (lastTxId+1));
                }
                advancedOnce = true;
            } else {
                currentReader.resume();
            }

            if (null != currentReader) {
                record = currentReader.readOp();

                if (null == record) {
                    if (currentReader.isInProgress()) {
                        currentReader.requireResume();
                        break;
                    } else {
                        currentReader.close();
                        currentReader = null;
                    }
                }
                else {
                    break;
                }
            } else {
                LOG.debug("No reader starting at TxId: {}", (lastTxId+1));
            }

        }

        if (null != record) {
            lastTxId = record.getTransactionId();
        }

        return record;
    }

    /**
     * Read the next numLogRec log records from the stream
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public List<LogRecord> readBulk(boolean shouldBlock, int numLogRecords) throws IOException{
        LinkedList<LogRecord> retList = new LinkedList<LogRecord>();

        int numRead = 0;
        LogRecord record = readNext(shouldBlock);
        while ((null != record)) {
            retList.add(record);
            numRead++;
            if (numRead >= numLogRecords) {
                break;
            }
            record = readNext(shouldBlock);
        }

        return retList;
    }

    @Override
    public void notifySessionExpired() {
        zkSessionExpired = true;
    }

    private void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (zkSessionExpired) {
            LOG.error("Executing " + operation + " after losing connection to zookeeper");
            throw new AlreadyClosedException("Executing " + operation + " after losing connection to zookeeper");
        }

        bkDistributedLogManager.checkClosed(operation);
    }
}