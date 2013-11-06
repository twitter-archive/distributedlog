package com.twitter.distributedlog;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;

public class BKContinuousLogReader implements LogReader, ZooKeeperClient.ZooKeeperSessionExpireNotifier {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReader.class);

    private final BKDistributedLogManager bkDistributedLogManager;
    private final long startTxId;
    private long lastTxId;
    private final BKLogPartitionReadHandler bkLedgerManager;
    private ResumableBKPerStreamLogReader currentReader = null;
    private final boolean readAheadEnabled;
    private final int readAheadWaitTime;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;
    private boolean endOfStreamEncountered = false;


    public BKContinuousLogReader (BKDistributedLogManager bkdlm,
                                  String streamIdentifier,
                                  long startTxId,
                                  boolean readAheadEnabled,
                                  int readAheadWaitTime) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier);
        this.startTxId = startTxId;
        lastTxId = startTxId - 1;
        this.readAheadEnabled = readAheadEnabled;
        this.readAheadWaitTime = readAheadWaitTime;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
    }

    /**
     * Close the stream.
     *
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
     *
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    @Override
    public LogRecord readNext(boolean shouldBlock) throws IOException {
        if (shouldBlock) {
            throw new NotYetImplementedException("readNext with shouldBlock=true");
        }

        checkClosedOrInError("LogReader#readNext");

        LogRecord record = null;
        boolean advancedOnce = false;
        while (!advancedOnce) {
            advancedOnce = createOrPositionReader(advancedOnce);

            if (null != currentReader) {

                record = currentReader.readOp();

                if (null == record) {
                    if (handleEndOfCurrentStream()) {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                LOG.debug("No reader starting at TxId: {}", (lastTxId + 1));
            }

        }

        if (null != record) {
            if (record.isEndOfStream()) {
                endOfStreamEncountered = true;
                throw new EndOfStreamException("End of Stream Reached for" + bkLedgerManager.getFullyQualifiedName());
            }

            lastTxId = record.getTransactionId();
        }

        return record;
    }

    private boolean createOrPositionReader(boolean advancedOnce) throws IOException {
        if (null == currentReader) {
            LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));
            currentReader = bkLedgerManager.getInputStream(lastTxId + 1, true, false, (lastTxId >= startTxId));
            if (null != currentReader) {
                if(readAheadEnabled && bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead())) {
                    bkLedgerManager.getLedgerDataAccessor().setReadAheadEnabled(true, readAheadWaitTime);
                }
                LOG.debug("Opened reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));
            }
            advancedOnce = true;
        } else {
            currentReader.resume();
        }

        return advancedOnce;
    }

    private boolean handleEndOfCurrentStream() throws IOException {
        boolean shouldBreak = false;
        if (currentReader.isInProgress()) {
            currentReader.requireResume();
            shouldBreak = true;
        } else {
            currentReader.close();
            currentReader = null;
        }
        return shouldBreak;
    }

    /**
     * Read the next numLogRec log records from the stream
     *
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    @Override
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

    @Override
    public long getLastTxId() {
        return lastTxId;
    }

    private void checkClosedOrInError(String operation) throws EndOfStreamException, AlreadyClosedException, LogReadException {
        if (endOfStreamEncountered) {
            throw new EndOfStreamException("End of Stream Reached for" + bkLedgerManager.getFullyQualifiedName());
        }

        if (zkSessionExpired) {
            LOG.error("Executing " + operation + " after losing connection to zookeeper");
            throw new AlreadyClosedException("Executing " + operation + " after losing connection to zookeeper");
        }

        if (null != bkLedgerManager) {
            bkLedgerManager.checkClosedOrInError();
        }

        bkDistributedLogManager.checkClosedOrInError(operation);
    }
}