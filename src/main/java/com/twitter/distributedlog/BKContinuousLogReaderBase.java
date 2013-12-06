package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BKContinuousLogReaderBase implements ZooKeeperClient.ZooKeeperSessionExpireNotifier, Closeable {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReaderBase.class);

    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogPartitionReadHandler bkLedgerManager;
    protected ResumableBKPerStreamLogReader currentReader = null;
    protected final boolean readAheadEnabled;
    protected final int readAheadWaitTime;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;
    private boolean endOfStreamEncountered = false;
    protected final boolean noBlocking;
    protected DLSN nextDLSN = DLSN.InvalidDLSN;


    public BKContinuousLogReaderBase(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     boolean readAheadEnabled,
                                     int readAheadWaitTime,
                                     boolean noBlocking,
                                     AsyncNotification notification) throws IOException {
        // noBlocking => readAheadEnabled
        assert(!noBlocking || readAheadEnabled);
        this.bkDistributedLogManager = bkdlm;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier, notification);
        this.readAheadEnabled = readAheadEnabled;
        this.readAheadWaitTime = readAheadWaitTime;
        this.noBlocking = noBlocking;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
    }

    /**
     * Close the stream.
     *
     * @throws IOException if an error occurred while closing
     */
    @Override
    public void close() throws IOException {
        try {
            if (null != currentReader) {
                currentReader.close();
            }
            if (null != bkLedgerManager) {
                bkLedgerManager.close();
            }
        } finally {
            bkDistributedLogManager.unregister(sessionExpireWatcher);
        }
    }

    /**
     * Read the next log record from the stream
     *
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public LogRecordWithDLSN readNext(boolean shouldBlock) throws IOException {
        if (shouldBlock) {
            throw new NotYetImplementedException("readNext with shouldBlock=true");
        }

        checkClosedOrInError("LogReader#readNext");

        LogRecordWithDLSN record = null;
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
                LOG.debug("No reader at specified start point: {}", nextDLSN);
            }

        }

        if (null != record) {
            if (record.isEndOfStream()) {
                endOfStreamEncountered = true;
                throw new EndOfStreamException("End of Stream Reached for" + bkLedgerManager.getFullyQualifiedName());
            }
        }

        return record;
    }


    protected boolean createOrPositionReader(boolean advancedOnce) throws IOException {
        if (null == currentReader) {
            currentReader = getCurrentReader();
            if ((null != currentReader) && !noBlocking) {
                if(readAheadEnabled && bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead())) {
                    bkLedgerManager.getLedgerDataAccessor().setReadAheadEnabled(true, readAheadWaitTime);
                }
                LOG.debug("Opened reader on partition {}", bkLedgerManager.getFullyQualifiedName());
            }
            advancedOnce = (currentReader == null);
        } else {
            currentReader.resume();
        }

        return advancedOnce;
    }

    abstract protected ResumableBKPerStreamLogReader getCurrentReader() throws IOException;
    abstract protected LogRecordWithDLSN readNextWithSkip() throws IOException;


    private boolean handleEndOfCurrentStream() throws IOException {
        boolean shouldBreak = false;
        if (currentReader.reachedEndOfLogSegment()) {
            nextDLSN = currentReader.getNextDLSN();
            currentReader.close();
            currentReader = null;
        } else {
            currentReader.requireResume();
            shouldBreak = true;
        }
        return shouldBreak;
    }

    /**
     * Read the next numLogRec log records from the stream
     *
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

    public void checkClosedOrInError(String operation) throws EndOfStreamException, AlreadyClosedException, LogReadException {
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

    String getFullyQualifiedName() {
        return bkLedgerManager.getFullyQualifiedName();
    }
}
