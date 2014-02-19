package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.EndOfStreamException;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BKContinuousLogReaderBase implements ZooKeeperClient.ZooKeeperSessionExpireNotifier, Closeable, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReaderBase.class);

    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogPartitionReadHandler bkLedgerManager;
    protected ResumableBKPerStreamLogReader currentReader = null;
    protected final boolean readAheadEnabled;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;
    private volatile boolean endOfStreamEncountered = false;
    private LogReader.ReaderNotification notification = null;
    protected DLSN nextDLSN = DLSN.InvalidDLSN;
    protected boolean simulateErrors = false;


    public BKContinuousLogReaderBase(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     boolean readAheadEnabled,
                                     AsyncNotification notification) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier, notification);
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
     * @param nonBlockingReadOperation should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public synchronized LogRecordWithDLSN readNext(boolean nonBlockingReadOperation) throws IOException {
        if (nonBlockingReadOperation && !readAheadEnabled) {
            throw new IllegalArgumentException("Non blocking semantics require read-ahead");
        }

        checkClosedOrInError("LogReader#readNext");

        LogRecordWithDLSN record = null;
        boolean advancedOnce = false;
        while (!advancedOnce) {
            advancedOnce = createOrPositionReader(nonBlockingReadOperation);

            if (null != currentReader) {

                record = currentReader.readOp(nonBlockingReadOperation);

                if (null == record) {
                    if (handleEndOfCurrentStream()) {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                LOG.debug("{} No reader at specified start point: {}", bkLedgerManager.getFullyQualifiedName(), nextDLSN);
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

    protected boolean createOrPositionReader(boolean nonBlocking) throws IOException {
        boolean advancedOnce = false;
        if (null == currentReader) {
            currentReader = getCurrentReader();
            if ((null != currentReader)) {
                if(readAheadEnabled) {
                    bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead(), simulateErrors);
                }
                LOG.debug("Opened reader on partition {}", bkLedgerManager.getFullyQualifiedName());
            }
            advancedOnce = (currentReader == null);
        } else {
            currentReader.resume(!nonBlocking);
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
    public synchronized List<LogRecordWithDLSN> readBulk(boolean nonBlocking, int numLogRecords) throws IOException{
        LinkedList<LogRecordWithDLSN> retList = new LinkedList<LogRecordWithDLSN>();

        int numRead = 0;
        LogRecordWithDLSN record = readNext(nonBlocking);
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

    @VisibleForTesting
    public void simulateErrors() {
        simulateErrors = true;
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
    public synchronized void registerNotification(LogReader.ReaderNotification readNotification) {
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
