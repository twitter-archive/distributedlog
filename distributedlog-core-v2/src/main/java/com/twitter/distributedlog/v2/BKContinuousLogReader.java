package com.twitter.distributedlog.v2;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AsyncNotification;
import com.twitter.distributedlog.LogReadException;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.IdleReaderException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.twitter.distributedlog.DLSNUtil.*;

public class BKContinuousLogReader implements LogReader, ZooKeeperClient.ZooKeeperSessionExpireNotifier, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReader.class);

    private final BKDistributedLogManager bkDistributedLogManager;
    private final long startTxId;
    private long lastTxId;
    private final BKLogPartitionReadHandler bkLedgerManager;
    private ResumableBKPerStreamLogReader currentReader = null;
    private final boolean readAheadEnabled;
    private final boolean forceReadEnabled;
    private final int idleWarnThresholdMillis;
    private final int idleErrorThresholdMillis;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;
    private boolean endOfStreamEncountered = false;
    private ReaderNotification notification = null;
    private Stopwatch openReaderLastAttemptSw = Stopwatch.createStarted();
    private long openReaderAttemptCount = 0;
    private Stopwatch idleReaderLastWarnSw = Stopwatch.createStarted();
    private Stopwatch idleReaderLastLogRecordSw = Stopwatch.createStarted();
    // If this watch is set then we can rely on the notification before we
    // attempt to open the reader again
    private AtomicBoolean watchSetOnListOfLogSegments = new AtomicBoolean(false);
    private boolean isReaderIdle = false;
    private boolean forceBlockingRead = false;
    private boolean disableReadAheadZKNotification = false;

    private final Counter idleReaderWarnCounter;
    private final Counter idleReaderErrorCounter;

    public BKContinuousLogReader (BKDistributedLogManager bkdlm,
                                  String streamIdentifier,
                                  long startTxId,
                                  final DistributedLogConfiguration conf,
                                  StatsLogger statsLogger) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier);
        this.startTxId = startTxId;
        lastTxId = startTxId - 1;
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

    @VisibleForTesting
    BKLogPartitionReadHandler getReadHandler() {
        return bkLedgerManager;
    }

    @VisibleForTesting
    synchronized ResumableBKPerStreamLogReader getCurrentReader() {
        return currentReader;
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
     * @param nonBlocking should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    @Override
    public synchronized LogRecordWithDLSN readNext(boolean nonBlocking) throws IOException {
        if (nonBlocking && !readAheadEnabled) {
            throw new IllegalArgumentException("Non blocking semantics require read-ahead");
        }

        checkClosedOrInError("LogReader#readNext");

        LogRecordWithDLSN record = null;
        boolean advancedOnce = false;
        while (!advancedOnce) {
            advancedOnce = createOrPositionReader(nonBlocking, forceBlockingRead);

            if (null != currentReader) {
                currentReader.setEnableTrace(forceBlockingRead);

                record = currentReader.readOp(nonBlocking && !forceBlockingRead);

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
            if (isEndOfStream(record)) {
                endOfStreamEncountered = true;
                throw new EndOfStreamException("End of Stream Reached for" + bkLedgerManager.getFullyQualifiedName());
            }

            lastTxId = record.getTransactionId();
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

    private boolean createOrPositionReader(boolean nonBlocking, boolean forceBlockingRead) throws IOException {
        boolean advancedOnce = false;
        if (null == currentReader) {
            if(!(nonBlocking && !forceBlockingRead) ||
                watchSetOnListOfLogSegments.compareAndSet(false, true) ||
                (openReaderLastAttemptSw.elapsed(TimeUnit.MILLISECONDS) > idleWarnThresholdMillis)) {
                LOG.info("Opening reader on partition {} starting at TxId: {}",
                    bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));

                Pair<ResumableBKPerStreamLogReader, Boolean> readerAndWatchSet = bkLedgerManager.getInputStream(lastTxId + 1, (lastTxId >= startTxId), new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        // If we get any notification, the safest action to take is to retry opening
                        // a reader
                        watchSetOnListOfLogSegments.set(false);
                    }
                });
                currentReader = readerAndWatchSet.getLeft();

                openReaderAttemptCount++;
                openReaderLastAttemptSw.reset().start();

                if (null != currentReader) {
                    watchSetOnListOfLogSegments.set(false);
                    if (readAheadEnabled) {
                        bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead());
                        if (disableReadAheadZKNotification) {
                            bkLedgerManager.disableReadAheadZKNotification();
                        }
                    }
                    LOG.info("Opened reader on partition {} starting at TxId: {}",
                        bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));
                } else {
                    LOG.info("Reader on partition {} starting at TxId: {} was not opened as there was nothing to read, watch set {}",
                        new Object[] { bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1), readerAndWatchSet.getRight() });
                    watchSetOnListOfLogSegments.compareAndSet(true, readerAndWatchSet.getRight());
                }
            }

            advancedOnce = (currentReader == null);
        } else {
            currentReader.resume(!(nonBlocking && !forceBlockingRead), forceBlockingRead);
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

    private void checkClosedOrInError(String operation) throws EndOfStreamException, AlreadyClosedException, LogReadException, DLInterruptedException {
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
     * Register for notifications of changes to background reader when using
     * non blocking semantics
     *
     * @param readNotification Implementation of the ReaderNotification interface whose methods
     * are called when new data is available or when the reader errors out
     */
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

    @VisibleForTesting
    synchronized void setForceBlockingRead(boolean force) {
        forceBlockingRead = force;
    }

    @VisibleForTesting
    synchronized boolean getForceBlockingRead() {
        return forceBlockingRead;
    }

    @VisibleForTesting
    long getOpenReaderAttemptCount() {
        return openReaderAttemptCount;
    }

    @VisibleForTesting
    synchronized void disableReadAheadZKNotification() {
        disableReadAheadZKNotification = true;
        bkLedgerManager.disableReadAheadZKNotification();
    }
}
