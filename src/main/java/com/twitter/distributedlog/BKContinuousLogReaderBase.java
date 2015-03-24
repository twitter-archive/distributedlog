package com.twitter.distributedlog;

import java.io.Closeable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.IdleReaderException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BKContinuousLogReaderBase implements ZooKeeperClient.ZooKeeperSessionExpireNotifier, Closeable {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReaderBase.class);

    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogPartitionReadHandler bkLedgerManager;
    protected ResumableBKPerStreamLogReader currentReader = null;
    protected final boolean readAheadEnabled;
    private final boolean forceReadEnabled;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;
    private volatile boolean endOfStreamEncountered = false;
    protected DLSN nextDLSN = DLSN.InvalidDLSN;
    protected boolean simulateErrors = false;
    private final int idleWarnThresholdMillis;
    private final int idleErrorThresholdMillis;
    private Stopwatch idleReaderLastWarnSw = Stopwatch.createStarted();
    private Stopwatch idleReaderLastLogRecordSw = Stopwatch.createStarted();
    private boolean isReaderIdle = false;
    private boolean forceBlockingRead = false;
    private boolean disableReadAheadZKNotification = false;

    private final Counter idleReaderWarnCounter;
    private final Counter idleReaderErrorCounter;

    public BKContinuousLogReaderBase(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     final DistributedLogConfiguration conf,
                                     StatsLogger statsLogger) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        Optional<String> subscriberId = Optional.absent();
        this.bkLedgerManager =
                bkDistributedLogManager.createReadLedgerHandler(streamIdentifier, subscriberId, true);
        this.readAheadEnabled = conf.getEnableReadAhead();
        this.forceReadEnabled = conf.getEnableForceRead();
        this.idleWarnThresholdMillis = conf.getReaderIdleWarnThresholdMillis();
        this.idleErrorThresholdMillis = conf.getReaderIdleErrorThresholdMillis();
        sessionExpireWatcher = this.bkLedgerManager.registerExpirationHandler(this);

        // Stats
        StatsLogger readerStatsLogger = statsLogger.scope("reader");
        idleReaderWarnCounter = readerStatsLogger.getCounter("idle_reader_warnings");
        idleReaderErrorCounter = readerStatsLogger.getCounter("idle_reader_errors");
    }

    /**
     * Close the stream.
     *
     * @throws IOException if an error occurred while closing
     */
    @Override
    public synchronized void close() throws IOException {
        if (null != currentReader) {
            currentReader.close();
        }
        if (null != bkLedgerManager) {
            bkLedgerManager.unregister(sessionExpireWatcher);
            bkLedgerManager.close();
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
            advancedOnce = createOrPositionReader(nonBlockingReadOperation && !forceBlockingRead);

            if (null != currentReader) {
                currentReader.setEnableTrace(forceBlockingRead);

                record = currentReader.readOp(nonBlockingReadOperation && !forceBlockingRead);

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

            if (forceBlockingRead) {
                LOG.info("Reader on stream {}; resumed from idle state when forced blocking read", bkLedgerManager.getFullyQualifiedName());
            } else {
                idleReaderLastLogRecordSw.reset().start();
                idleReaderLastWarnSw.reset().start();
                if (isReaderIdle) {
                    LOG.info("Reader on stream {}; resumed from idle state", bkLedgerManager.getFullyQualifiedName());
                    isReaderIdle = false;
                }
            }
            forceBlockingRead = false;
        } else {
            forceBlockingRead = false;
            long idleDuration = idleReaderLastLogRecordSw.elapsed(TimeUnit.MILLISECONDS);
            if (idleDuration > idleErrorThresholdMillis) {
                LOG.error("Idle Reader on stream {} for {} ms; Current Reader {}",
                    new Object[] { bkLedgerManager.getFullyQualifiedName(),
                        idleDuration, currentReader });
                bkLedgerManager.dumpReadAheadState(true /*error*/);
                idleReaderErrorCounter.inc();
                throw new IdleReaderException("Reader on stream" +
                    bkLedgerManager.getFullyQualifiedName()
                    + "is idle for " + idleDuration +"ms");
            }
            // If we have exceeded the warning threshold generate the warning and then reset
            // the timer so as to avoid flooding the log with idle reader messages
            else if (idleDuration > idleWarnThresholdMillis &&
                idleReaderLastWarnSw.elapsed(TimeUnit.MILLISECONDS) > idleWarnThresholdMillis) {
                LOG.warn("Idle Reader on stream {} for {} ms; Current Reader {}",
                    new Object[] { bkLedgerManager.getFullyQualifiedName(),
                        idleDuration, currentReader });
                bkLedgerManager.dumpReadAheadState(false /*error*/);
                idleReaderWarnCounter.inc();
                idleReaderLastWarnSw.reset().start();
                isReaderIdle = true;
                forceBlockingRead = this.forceReadEnabled;
            }
        }

        return record;
    }

    protected boolean createOrPositionReader(boolean nonBlocking) throws IOException {
        boolean advancedOnce = false;
        if (null == currentReader) {
            LOG.info("Opening reader on partition {}", bkLedgerManager.getFullyQualifiedName());
            currentReader = getCurrentReader();
            if ((null != currentReader)) {
                if(readAheadEnabled) {
                    bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead(), simulateErrors);
                    if (disableReadAheadZKNotification) {
                        bkLedgerManager.disableReadAheadZKNotification();
                    }
                }
                LOG.info("Opened reader on partition {}", bkLedgerManager.getFullyQualifiedName());
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

    private void checkClosedOrInError(String operation)
        throws EndOfStreamException, AlreadyClosedException,
                LogReadException, DLInterruptedException,
                AlreadyTruncatedTransactionException {
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

    protected synchronized boolean getForceBlockingRead() {
        return forceBlockingRead;
    }

    @VisibleForTesting
    synchronized void setForceBlockingRead(boolean force) {
        forceBlockingRead = force;
    }

    @VisibleForTesting
    synchronized void disableReadAheadZKNotification() {
        disableReadAheadZKNotification = true;
        bkLedgerManager.disableReadAheadZKNotification();
    }
}
