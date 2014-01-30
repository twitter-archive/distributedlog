package com.twitter.distributedlog;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;

public class BKContinuousLogReader implements LogReader, ZooKeeperClient.ZooKeeperSessionExpireNotifier, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReader.class);

    private final BKDistributedLogManager bkDistributedLogManager;
    private final long startTxId;
    private long lastTxId;
    private final BKLogPartitionReadHandler bkLedgerManager;
    private ResumableBKPerStreamLogReader currentReader = null;
    private final boolean readAheadEnabled;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;
    private boolean endOfStreamEncountered = false;
    private ReaderNotification notification = null;


    public BKContinuousLogReader (BKDistributedLogManager bkdlm,
                                  String streamIdentifier,
                                  long startTxId,
                                  boolean readAheadEnabled) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier);
        this.startTxId = startTxId;
        lastTxId = startTxId - 1;
        this.readAheadEnabled = readAheadEnabled;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
    }

    /**
     * Close the stream.
     *
     * @throws IOException if an error occurred while closing
     */
    @Override
    public synchronized void close() throws IOException {
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
     * @param nonBlocking should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    @Override
    public synchronized LogRecord readNext(boolean nonBlocking) throws IOException {
        if (nonBlocking && !readAheadEnabled) {
            throw new IllegalArgumentException("Non blocking semantics require read-ahead");
        }

        checkClosedOrInError("LogReader#readNext");

        LogRecord record = null;
        boolean advancedOnce = false;
        while (!advancedOnce) {
            advancedOnce = createOrPositionReader(nonBlocking);

            if (null != currentReader) {

                record = currentReader.readOp(nonBlocking);

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

    private boolean createOrPositionReader(boolean nonBlocking) throws IOException {
        boolean advancedOnce = false;
        if (null == currentReader) {
            LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));
            currentReader = bkLedgerManager.getInputStream(lastTxId + 1, (lastTxId >= startTxId));
            if (null != currentReader) {
                if (readAheadEnabled) {
                    bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead());
                }
                LOG.debug("Opened reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));
            }
            advancedOnce = (currentReader == null);
        } else {
            currentReader.resume(!nonBlocking);
        }

        return advancedOnce;
    }

    private boolean handleEndOfCurrentStream() throws IOException {
        boolean shouldBreak = false;
        if (currentReader.reachedEndOfLogSegment()) {
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
    @Override
    public synchronized List<LogRecord> readBulk(boolean nonBlocking, int numLogRecords) throws IOException{
        LinkedList<LogRecord> retList = new LinkedList<LogRecord>();

        int numRead = 0;
        LogRecord record = readNext(nonBlocking);
        while ((null != record)) {
            retList.add(record);
            numRead++;
            if (numRead >= numLogRecords) {
                break;
            }
            record = readNext(nonBlocking);
        }

        return retList;
    }

    @Override
    public void notifySessionExpired() {
        zkSessionExpired = true;
    }

    @Override
    public synchronized long getLastTxId() {
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

    /**
     * Triggered when the background activity encounters an exception
     */
    @Override
    public synchronized void notifyOnError() {
        if (null != notification) {
            notification.notifyNextRecordAvailable();
            notification = null;
            bkLedgerManager.setNotification(null);
        }
    }

    /**
     * Triggered when the background activity completes an operation
     */
    @Override
    public synchronized void notifyOnOperationComplete() {
        if (null != notification) {
            notification.notifyNextRecordAvailable();
            notification = null;
            bkLedgerManager.setNotification(null);
        }
    }

    /**
     * Register for notifications of changes to background reader when using
     * non blocking semantics
     *
     * @param readNotification Implementation of the ReaderNotification interface whose methods
     * are called when new data is available or when the reader errors out
     */
    @Override
    public synchronized void registerNotification(ReaderNotification readNotification) {
        if ((null != notification) && (null != readNotification)) {
            throw new IllegalStateException("Notification already registered for " + bkLedgerManager.getFullyQualifiedName());
        }

        this.notification = readNotification;
        try {
            // Set the notification first and then check for errors so we don't
            // miss errors set by the read ahead task
            bkLedgerManager.setNotification(this);

            checkClosedOrInError("registerNotification");

            if ((null != currentReader) && currentReader.canResume()) {
                notifyOnOperationComplete();
            }
            LOG.debug("Registered for notification {}", bkLedgerManager.getFullyQualifiedName());
        } catch (IOException exc) {
            LOG.error("registerNotification encountered exception", exc);
            notifyOnError();
        }
    }
}
