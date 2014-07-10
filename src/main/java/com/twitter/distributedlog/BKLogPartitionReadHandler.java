package com.twitter.distributedlog;

import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.stats.AlertStatsLogger;
import com.twitter.distributedlog.stats.BKExceptionStatsLogger;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class BKLogPartitionReadHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    private static final int LAYOUT_VERSION = -1;
    private LedgerDataAccessor ledgerDataAccessor = null;
    private final LedgerHandleCache handleCache;

    protected final ScheduledExecutorService readAheadExecutor;
    private ReadAheadWorker readAheadWorker = null;
    private volatile boolean readAheadError = false;
    private volatile boolean readAheadInterrupted = false;
    private final AlertStatsLogger alertStatsLogger;

    // stats
    private static Counter readAheadWorkerWaits;
    private static Counter readAheadEntryPiggyBackHits;
    private static Counter readAheadEntryPiggyBackMisses;
    private static Counter readAheadReadLACCounter;
    private static Counter readAheadReadLACAndEntryCounter;
    private static OpStatsLogger getInputStreamByTxIdStat;
    private static OpStatsLogger getInputStreamByDLSNStat;
    private static OpStatsLogger existsStat;
    private static OpStatsLogger readAheadReadEntriesStat;
    private static OpStatsLogger longPollInterruptionStat;
    private static OpStatsLogger metadataReinitializationStat;
    private static OpStatsLogger notificationExecutionStat;
    private static StatsLogger parentExceptionStatsLogger;
    private final static ConcurrentMap<String, BKExceptionStatsLogger> exceptionStatsLoggers =
            new ConcurrentHashMap<String, BKExceptionStatsLogger>();

    private static BKExceptionStatsLogger getBKExceptionStatsLogger(String phase) {
        assert null != parentExceptionStatsLogger;
        BKExceptionStatsLogger exceptionStatsLogger = exceptionStatsLoggers.get(phase);
        if (null == exceptionStatsLogger) {
            exceptionStatsLogger = new BKExceptionStatsLogger(parentExceptionStatsLogger.scope(phase));
            BKExceptionStatsLogger oldExceptionStatsLogger =
                    exceptionStatsLoggers.putIfAbsent(phase, exceptionStatsLogger);
            if (null != oldExceptionStatsLogger) {
                exceptionStatsLogger = oldExceptionStatsLogger;
            }
        }
        return exceptionStatsLogger;
    }

    public enum ReadLACOption {
        DEFAULT (0), LONGPOLL(1), READENTRYPIGGYBACK_PARALLEL (2), READENTRYPIGGYBACK_SEQUENTIAL(3), INVALID_OPTION(4);
        private final int value;

        private ReadLACOption(int value) {
            this.value = value;
        }
    }

    /**
     * Construct a Bookkeeper journal manager.
     */
    public BKLogPartitionReadHandler(String name,
                                     String streamIdentifier,
                                     DistributedLogConfiguration conf,
                                     URI uri,
                                     ZooKeeperClientBuilder zkcBuilder,
                                     BookKeeperClientBuilder bkcBuilder,
                                     ScheduledExecutorService executorService,
                                     ScheduledExecutorService readAheadExecutor,
                                     AlertStatsLogger alertStatsLogger,
                                     StatsLogger statsLogger,
                                     AsyncNotification notification) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder, executorService,
              statsLogger, notification, LogSegmentFilter.DEFAULT_FILTER);

        this.readAheadExecutor = readAheadExecutor;
        this.alertStatsLogger = alertStatsLogger;
        handleCache = new LedgerHandleCache(this.bookKeeperClient, this.digestpw, statsLogger);
        ledgerDataAccessor = new LedgerDataAccessor(
                handleCache, getFullyQualifiedName(), statsLogger, notification,
                conf.getTraceReadAheadDeliveryLatency(), conf.getDataLatencyWarnThresholdMillis());

        // Stats
        StatsLogger readAheadStatsLogger = statsLogger.scope("readahead_worker");
        if (null == readAheadWorkerWaits) {
            readAheadWorkerWaits = readAheadStatsLogger.getCounter("wait");
        }
        if (null == readAheadEntryPiggyBackHits) {
            readAheadEntryPiggyBackHits = readAheadStatsLogger.getCounter("entry_piggy_back_hits");
        }
        if (null == readAheadEntryPiggyBackMisses) {
            readAheadEntryPiggyBackMisses = readAheadStatsLogger.getCounter("entry_piggy_back_misses");
        }
        if (null == readAheadReadEntriesStat) {
            readAheadReadEntriesStat = readAheadStatsLogger.getOpStatsLogger("read_entries");
        }
        if (null == readAheadReadLACCounter) {
            readAheadReadLACCounter = readAheadStatsLogger.getCounter("read_lac_counter");
        }
        if (null == readAheadReadLACAndEntryCounter) {
            readAheadReadLACAndEntryCounter = readAheadStatsLogger.getCounter("read_lac_and_entry_counter");
        }
        if (null == longPollInterruptionStat) {
            longPollInterruptionStat = readAheadStatsLogger.getOpStatsLogger("long_poll_interruption");
        }
        if (null == notificationExecutionStat) {
            notificationExecutionStat = readAheadStatsLogger.getOpStatsLogger("notification_execution");
        }
        if (null == metadataReinitializationStat) {
            metadataReinitializationStat = readAheadStatsLogger.getOpStatsLogger("metadata_reinitialization");
        }
        if (null == parentExceptionStatsLogger) {
            parentExceptionStatsLogger = statsLogger.scope("exceptions");
        }

        StatsLogger readerStatsLogger = statsLogger.scope("reader");
        if (null == getInputStreamByDLSNStat) {
            getInputStreamByDLSNStat = readerStatsLogger.getOpStatsLogger("open_stream_by_dlsn");
        }
        if (null == getInputStreamByTxIdStat) {
            getInputStreamByTxIdStat = readerStatsLogger.getOpStatsLogger("open_stream_by_txid");
        }
        if (null == existsStat) {
            existsStat = readerStatsLogger.getOpStatsLogger("check_stream_exists");
        }
    }

    public ResumableBKPerStreamLogReader getInputStream(DLSN fromDLSN,
                                                        boolean fThrowOnEmpty,
                                                        boolean simulateErrors)
        throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        try {
            ResumableBKPerStreamLogReader reader =
                    doGetInputStream(fromDLSN, fThrowOnEmpty, simulateErrors);
            getInputStreamByDLSNStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            return reader;
        } catch (IOException ioe) {
            getInputStreamByDLSNStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            throw ioe;
        }
    }

    private ResumableBKPerStreamLogReader doGetInputStream(DLSN fromDLSN,
                                                           boolean fThrowOnEmpty,
                                                           boolean simulateErrors) throws IOException {
        if (doesLogExist()) {
            List<LogSegmentLedgerMetadata> segments = getFilteredLedgerList(false, false);
            for (LogSegmentLedgerMetadata l : segments) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Inspecting Ledger: {} for {}", l, fromDLSN);
                }
                DLSN lastDLSN = new DLSN(l.getLedgerSequenceNumber(), l.getLastEntryId(), l.getLastSlotId());
                if (l.isInProgress()) {
                    try {
                        // as mostly doGetInputStream is to position on reader, so we disable forwardReading
                        // on readLastTxIdInLedger, then the reader could be positioned in the reader quickly
                        lastDLSN = readLastTxIdInLedger(l, false).getRight();
                    } catch (IOException exc) {
                        lastDLSN = new DLSN(l.getLedgerSequenceNumber(), -1, -1);
                        LOG.info("Reading beyond flush point");
                    }

                    if (lastDLSN == DLSN.InvalidDLSN) {
                        lastDLSN = new DLSN(l.getLedgerSequenceNumber(), -1, -1);
                    }
                }

                if (fromDLSN.compareTo(lastDLSN) <= 0) {
                    try {
                        long startBKEntry = 0;
                        if(l.getLedgerSequenceNumber() == fromDLSN.getLedgerSequenceNo()) {
                            startBKEntry = Math.max(0, fromDLSN.getEntryId());
                        }

                        ResumableBKPerStreamLogReader s = new ResumableBKPerStreamLogReader(this,
                                                                    zooKeeperClient,
                                                                    ledgerDataAccessor,
                                                                    l,
                                                                    startBKEntry,
                                                                    statsLogger);


                        if (s.skipTo(fromDLSN)) {
                            return s;
                        } else {
                            s.close();
                            return null;
                        }
                    } catch (Exception e) {
                        LOG.error("Could not open ledger for the stream " + getFullyQualifiedName() + " for startDLSN " + fromDLSN, e);
                        throw new IOException("Could not open ledger for " + fromDLSN, e);
                    }
                } else {
                    if (fromDLSN.getLedgerSequenceNo() == lastDLSN.getLedgerSequenceNo()) {
                        // We specified a position past the end of the current ledger; position on the first record of the
                        // next ledger
                        fromDLSN = fromDLSN.positionOnTheNextLedger();
                    }

                    ledgerDataAccessor.purgeReadAheadCache(new LedgerReadPosition(l.getLedgerId(), l.getLedgerSequenceNumber(), Long.MAX_VALUE));
                }
            }
        } else {
            if (fThrowOnEmpty) {
                throw new LogNotFoundException(String.format("Log %s does not exist or has been deleted", getFullyQualifiedName()));
            }
        }

        return null;
    }

    public ResumableBKPerStreamLogReader getInputStream(long fromTxId,
                                                        boolean fThrowOnEmpty,
                                                        boolean simulateErrors)
        throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        try {
            ResumableBKPerStreamLogReader reader =
                    doGetInputStream(fromTxId, fThrowOnEmpty, simulateErrors);
            getInputStreamByTxIdStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            return reader;
        } catch (IOException ioe) {
            getInputStreamByTxIdStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            throw ioe;
        }
    }


    private ResumableBKPerStreamLogReader doGetInputStream(long fromTxId,
                                                           boolean fThrowOnEmpty,
                                                           boolean simulateErrors)
            throws IOException {
        if (doesLogExist()) {
            List<LogSegmentLedgerMetadata> segments = getFilteredLedgerList(false, false);
            for (int i = 0; i < segments.size(); i++) {
                LogSegmentLedgerMetadata l = segments.get(i);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Inspecting Ledger: {}", l);
                }
                long lastTxId = l.getLastTxId();
                if (l.isInProgress()) {
                    try {
                        // as mostly doGetInputStream is to position on reader, so we disable forwardReading
                        // on readLastTxIdInLedger, then the reader could be positioned in the reader quickly
                        lastTxId = readLastTxIdInLedger(l, false).getLeft();
                    } catch (DLInterruptedException die) {
                        throw die;
                    } catch (IOException exc) {
                        lastTxId = l.getFirstTxId();
                        LOG.info("Reading beyond flush point");
                    }

                    if (lastTxId == DistributedLogConstants.INVALID_TXID) {
                        lastTxId = l.getFirstTxId();
                    }

                    // if the inprogress log segment isn't the last one
                    // we should not move on until it is closed and completed.
                    if (i != segments.size() - 1 && fromTxId > lastTxId) {
                        fromTxId = lastTxId;
                    }
                }

                if (fromTxId <= lastTxId) {
                    try {
                        ResumableBKPerStreamLogReader s
                            = new ResumableBKPerStreamLogReader(this, zooKeeperClient, ledgerDataAccessor, l, statsLogger);

                        if (s.skipTo(fromTxId)) {
                            return s;
                        } else {
                            s.close();
                            return null;
                        }
                    } catch (IOException e) {
                        LOG.error("Could not open ledger for the stream " + getFullyQualifiedName() + " for startTxId " + fromTxId, e);
                        throw e;
                    }
                } else {
                        ledgerDataAccessor.purgeReadAheadCache(new LedgerReadPosition(l.getLedgerId(), l.getLedgerSequenceNumber(), Long.MAX_VALUE));
                }
            }
        } else {
            if (fThrowOnEmpty) {
                throw new LogNotFoundException(String.format("Log %s does not exist or has been deleted", getFullyQualifiedName()));
            }
        }

        return null;
    }

    public void close() throws IOException {
        if (null != readAheadWorker) {
            readAheadWorker.stop();
        }
        if (null != ledgerDataAccessor) {
            ledgerDataAccessor.clear();
        }
        super.close();
    }

    private void setWatcherOnLedgerRoot(Watcher watcher) throws ZooKeeperClient.ZooKeeperConnectionException, KeeperException, InterruptedException {
        zooKeeperClient.get().getChildren(ledgerPath, watcher);
    }

    public void startReadAhead(LedgerReadPosition startPosition, boolean simulateErrors) {
        if (null == readAheadWorker) {
            readAheadWorker = new ReadAheadWorker(getFullyQualifiedName(),
                this,
                startPosition,
                ledgerDataAccessor,
                simulateErrors,
                conf);
            readAheadWorker.start();
            ledgerDataAccessor.setReadAheadEnabled(true, conf.getReadAheadWaitTime(), conf.getReadAheadBatchSize());
        }
    }

    public void simulateErrors() {
        if (null != readAheadWorker) {
            readAheadWorker.simulateErrors();
        }
    }

    void dumpReadAheadState(boolean isError) {
        if (isError) {
            LOG.error("Stream {}; Read Ahead state: {}", getFullyQualifiedName(), readAheadWorker);
        } else {
            LOG.warn("Stream {}; Read Ahead state: {}", getFullyQualifiedName(), readAheadWorker);
        }
    }

    public LedgerHandleCache getHandleCache() {
        return handleCache;
    }

    public void setReadAheadError() {
        readAheadError = true;
        if (null != notification) {
            notification.notifyOnError();
        }
    }

    public boolean doesLogExist() throws IOException {
        boolean logExists = false;
        boolean success = false;
        Stopwatch stopwatch = new Stopwatch().start();
        try {
            if (null != zooKeeperClient.get().exists(ledgerPath, false)) {
                logExists = true;
            }
            success = true;
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while checking " + ledgerPath, ie);
            throw new DLInterruptedException("Interrupted while checking " + ledgerPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error deleting" + ledgerPath + "entry in zookeeper", ke);
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
        } finally {
            if (success) {
                existsStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                existsStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
        return logExists;
    }

    public void setReadAheadInterrupted() {
        readAheadInterrupted = true;
        if (null != notification) {
            notification.notifyOnError();
        }
    }


    public void setNotification(final AsyncNotification notification) {
        // If we are setting a notification, then we must ensure that
        // read ahead has already been started. Once we have the read
        // ahead setup, it will notify on errors or successful reads
        // If we don't have read ahead setup then there are two possibilities
        // 1. There is nothing in the log => we must exit immediately, this is
        // mostly applicable only for tests
        // 2. We don't have a log segment beyond the start read point, in this case
        // we set a watcher on the ledger root and wait for changes
        //
        if ((null != notification) && (null == readAheadWorker)) {
            try {
                setWatcherOnLedgerRoot(new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        notification.notifyOnOperationComplete();
                    }
                });
            } catch (InterruptedException exc) {
                // Propagate the interrupt bit
                Thread.currentThread().interrupt();
                notification.notifyOnError();
            } catch (KeeperException exc) {
                notification.notifyOnError();
            } catch (ZooKeeperClient.ZooKeeperConnectionException exc) {
                notification.notifyOnError();
            }
        }

        ledgerDataAccessor.setNotification(notification);
    }

    public void checkClosedOrInError() throws LogReadException, DLInterruptedException {
        if (readAheadInterrupted) {
            throw new DLInterruptedException("ReadAhead Thread was interrupted");
        }
        else if (readAheadError) {
            throw new LogReadException("ReadAhead Thread encountered exceptions");
        }
    }

    public void raiseAlert(String msg, Object... args) {
        alertStatsLogger.raise(msg, args);
    }

    public LogRecordWithDLSN getNextReadAheadRecord() throws IOException {
        return ledgerDataAccessor.getNextReadAheadRecord();
    }

    static interface ReadAheadCallback {
        void resumeReadAhead();
    }

    static enum ReadAheadPhase {
        GET_LEDGERS,
        OPEN_LEDGER,
        CLOSE_LEDGER,
        READ_LAST_CONFIRMED,
        READ_ENTRIES
    }

    /**
     * ReadAhead Worker process readahead in asynchronous way. The whole readahead process are chained into
     * different phases:
     *
     * <p>
     * ScheduleReadAheadPhase: Schedule the readahead request based on previous state (e.g. whether to backoff).
     * After the readahead request was scheduled, the worker enters ReadAhead phase.
     * </p>
     * <p>
     * ReadAhead Phase: This phase is divided into several sub-phases. All the sub-phases are chained into the
     * execution flow. If errors happened during execution, the worker enters Exceptions Handling Phase.
     * <br>
     * CheckInProgressChangedPhase: check whether there is in progress ledgers changed and updated the metadata.
     * <br>
     * OpenLedgerPhase: open ledgers if necessary for following read requests.
     * <br>
     * ReadEntriesPhase: read entries from bookkeeper and fill the readahead cache.
     * <br>
     * After that, the worker goes back to Schedule Phase to schedule next readahead request.
     * </p>
     * <p>
     * Exceptions Handling Phase: Handle all the exceptions and properly schedule next readahead request.
     * </p>
     */
    class ReadAheadWorker implements ReadAheadCallback, Runnable, Watcher {

        private final String fullyQualifiedName;
        private final BKLogPartitionReadHandler bkLedgerManager;
        private volatile boolean reInitializeMetadata = true;
        private LedgerReadPosition nextReadPosition;
        private LogSegmentLedgerMetadata currentMetadata = null;
        private int currentMetadataIndex;
        private LedgerDescriptor currentLH;
        private final LedgerDataAccessor ledgerDataAccessor;
        private volatile List<LogSegmentLedgerMetadata> ledgerList;
        private boolean simulateErrors;
        private final DistributedLogConfiguration conf;
        private static final int BKC_ZK_EXCEPTION_THRESHOLD_IN_SECONDS = 30;
        private static final int BKC_UNEXPECTED_EXCEPTION_THRESHOLD = 3;

        // Parameters for the state for this readahead worker.
        volatile boolean running = true;
        volatile boolean encounteredException = false;
        private final AtomicInteger bkcZkExceptions = new AtomicInteger(0);
        private final AtomicInteger bkcUnExpectedExceptions = new AtomicInteger(0);
        volatile boolean inProgressChanged = false;

        // Metadata Notification
        final Object notificationLock = new Object();
        AsyncNotification metadataNotification = null;
        volatile long metadataNotificationTimeMillis = -1L;

        // ReadAhead Phases
        final Phase schedulePhase = new ScheduleReadAheadPhase();
        final Phase exceptionHandler = new ExceptionHandlePhase(schedulePhase);
        final Phase readAheadPhase =
                new CheckInProgressChangedPhase(new OpenLedgerPhase(new ReadEntriesPhase(schedulePhase)));

        // ReadAhead Status
        private volatile boolean isCatchingUp = true;

        public ReadAheadWorker(String fullyQualifiedName,
                               BKLogPartitionReadHandler ledgerManager,
                               LedgerReadPosition startPosition,
                               LedgerDataAccessor ledgerDataAccessor,
                               boolean simulateErrors,
                               DistributedLogConfiguration conf) {
            this.bkLedgerManager = ledgerManager;
            this.fullyQualifiedName = fullyQualifiedName;
            this.nextReadPosition = startPosition;
            this.ledgerDataAccessor = ledgerDataAccessor;
            this.simulateErrors = simulateErrors;
            if ((conf.getReadLACOption() >= ReadLACOption.INVALID_OPTION.value) ||
                (conf.getReadLACOption() < ReadLACOption.DEFAULT.value)) {
                LOG.warn("Invalid value of ReadLACOption configured {}", conf.getReadLACOption());
                conf.setReadLACOption(ReadLACOption.DEFAULT.value);
            }
            this.conf = conf;
        }

        public void simulateErrors() {
            this.simulateErrors = true;
        }

        public void start() {
            LOG.debug("Starting ReadAhead Worker for {}", fullyQualifiedName);
            running = true;
            schedulePhase.process(BKException.Code.OK);
        }

        public void stop() {
            running = false;
        }

        @Override
        public String toString() {
            return "Running:" + running +
                ", NextReadPosition:" + nextReadPosition +
                ", BKZKExceptions:" + bkcZkExceptions.get() +
                ", BKUnexpectedExceptions:" + bkcUnExpectedExceptions.get() +
                ", EncounteredException:" + encounteredException +
                ", readAheadError:" + readAheadError +
                ", readAheadInterrupted" + readAheadInterrupted +
                ", CurrentMetadata:" + ((null != currentMetadata) ? currentMetadata : "NONE");
        }

        @Override
        public void resumeReadAhead() {
            submit(this);
        }

        Runnable addRTEHandler(final Runnable runnable) {
            return new Runnable() {
                @Override
                public void run() {
                    try {
                        runnable.run();
                    } catch (RuntimeException rte) {
                        LOG.error("ReadAhead on stream {} encountered runtime exception", getFullyQualifiedName(), rte);
                        setReadAheadError();
                        throw rte;
                    }
                }
            };
        }

        void submit(Runnable runnable) {
            try {
                readAheadExecutor.submit(addRTEHandler(runnable));
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError();
            }
        }

        private void schedule(Runnable runnable, long timeInMillis) {
            try {
                readAheadWorkerWaits.inc();
                readAheadExecutor.schedule(addRTEHandler(runnable), timeInMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError();
            }
        }

        private void handleException(ReadAheadPhase phase, int returnCode) {
            getBKExceptionStatsLogger(phase.name()).getExceptionCounter(returnCode).inc();
            exceptionHandler.process(returnCode);
        }

        abstract class Phase {

            final Phase next;

            Phase(Phase next) {
                this.next = next;
            }

            abstract void process(int rc);
        }

        /**
         * Schedule next readahead request. If we need to backoff, schedule in a backoff delay.
         */
        final class ScheduleReadAheadPhase extends Phase {

            ScheduleReadAheadPhase() {
                super(null);
            }

            @Override
            void process(int rc) {
                if (!running) {
                    LOG.info("Stopped ReadAheadWorker for {}", fullyQualifiedName);
                    return;
                }
                boolean simulate = simulateErrors && Utils.randomPercent(2);
                if (encounteredException || simulate) {
                    int zkErrorThreshold = BKC_ZK_EXCEPTION_THRESHOLD_IN_SECONDS * 1000 * 4 / conf.getReadAheadWaitTime();

                    if ((bkcZkExceptions.get() > zkErrorThreshold) || simulate) {
                        LOG.error("BookKeeper Client used by the ReadAhead Thread has encountered zookeeper exception");
                        running = false;
                        bkLedgerManager.setReadAheadError();
                    } else if (bkcUnExpectedExceptions.get() > BKC_UNEXPECTED_EXCEPTION_THRESHOLD) {
                        LOG.error("ReadAhead Thread has encountered an unexpected BK exception");
                        running = false;
                        bkLedgerManager.setReadAheadError();
                    } else {
                        // We must always reinitialize metadata if the last attempt to read failed.
                        reInitializeMetadata = true;
                        encounteredException = false;
                        // Backoff before resuming
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Scheduling read ahead for {} after {} ms.", fullyQualifiedName, conf.getReadAheadWaitTime() / 4);
                        }
                        schedule(ReadAheadWorker.this, conf.getReadAheadWaitTime() / 4);
                    }
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Scheduling read ahead for {} now.", fullyQualifiedName);
                    }
                    submit(ReadAheadWorker.this);
                }
            }

        }

        /**
         * Phase on handling exceptions.
         */
        final class ExceptionHandlePhase extends Phase {

            ExceptionHandlePhase(Phase next) {
                super(next);
            }

            @Override
            void process(int rc) {
                if (BKException.Code.InterruptedException == rc) {
                    LOG.trace("ReadAhead Worker for {} is interrupted.", fullyQualifiedName);
                    running = false;
                    bkLedgerManager.setReadAheadInterrupted();
                    return;
                } else if (BKException.Code.ZKException == rc) {
                    encounteredException = true;
                    int numExceptions = bkcZkExceptions.incrementAndGet();
                    LOG.info("ReadAhead Worker for {} encountered zookeeper exception : total exceptions are {}.",
                            fullyQualifiedName, numExceptions);
                } else if (BKException.Code.OK != rc) {
                    encounteredException = true;
                    switch(rc) {
                        case BKException.Code.NoSuchEntryException:
                        case BKException.Code.LedgerRecoveryException:
                        case BKException.Code.NoSuchLedgerExistsException:
                            break;
                        default:
                            bkcUnExpectedExceptions.incrementAndGet();
                    }
                    LOG.info("ReadAhead Worker for {} encountered exception : {}",
                            fullyQualifiedName, BKException.create(rc));
                }
                // schedule next read ahead
                next.process(BKException.Code.OK);
            }
        }

        /**
         * Phase on checking in progress changed.
         */
        final class CheckInProgressChangedPhase extends Phase
            implements BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>> {

            CheckInProgressChangedPhase(Phase next) {
                super(next);
            }

            @Override
            public void operationComplete(final int rc, final List<LogSegmentLedgerMetadata> result) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (KeeperException.Code.OK.intValue() != rc) {
                            LOG.info("ZK Exception {} while reading ledger list", rc);
                            reInitializeMetadata = true;
                            if (DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE == rc) {
                                handleException(ReadAheadPhase.GET_LEDGERS, BKException.Code.InterruptedException);
                            } else {
                                handleException(ReadAheadPhase.GET_LEDGERS, BKException.Code.ZKException);
                            }
                            return;
                        }
                        ledgerList = result;
                        for (int i = 0; i < ledgerList.size(); i++) {
                            LogSegmentLedgerMetadata l = ledgerList.get(i);
                            if ((l.getLastDLSN().compareTo(
                                new DLSN(nextReadPosition.getLedgerSequenceNumber(), nextReadPosition.getEntryId(), -1)) >= 0) ||
                                (l.isInProgress() && l.getLedgerSequenceNumber() == nextReadPosition.getLedgerSequenceNumber())) {
                                long startBKEntry = 0;
                                if(l.getLedgerSequenceNumber() == nextReadPosition.getLedgerSequenceNumber()) {
                                    startBKEntry = Math.max(0, nextReadPosition.getEntryId());
                                    if (currentMetadata != null) {
                                        inProgressChanged = currentMetadata.isInProgress() && !l.isInProgress();
                                    }
                                } else {
                                    try {
                                        // We are positioning on a new ledger => reset the current ledger handle
                                        if (LOG.isTraceEnabled()) {
                                            LOG.trace("Positioning {} on a new ledger {}", fullyQualifiedName, l);
                                        }
                                        bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                        currentLH = null;
                                    } catch (InterruptedException ie) {
                                        LOG.debug("Interrupted during repositioning", ie);
                                        handleException(ReadAheadPhase.CLOSE_LEDGER, BKException.Code.InterruptedException);
                                    } catch (BKException bke) {
                                        LOG.debug("BK Exception during repositioning", bke);
                                        handleException(ReadAheadPhase.CLOSE_LEDGER, bke.getCode());
                                    }
                                }

                                nextReadPosition = new LedgerReadPosition(l.getLedgerId(), l.getLedgerSequenceNumber(), startBKEntry);
                                if (conf.getTraceReadAheadMetadataChanges()) {
                                    LOG.info("Moved read position to {} for stream {} at {}.",
                                             new Object[] { nextReadPosition, getFullyQualifiedName(), System.currentTimeMillis() });
                                }
                                currentMetadata = l;
                                currentMetadataIndex = i;
                                break;
                            }

                            // Handle multiple in progress => stop at the first in progress
                            if (l.isInProgress()) {
                                break;
                            }
                        }

                        if (null == currentMetadata) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("No log segment to position on for {}. Backing off for {} millseconds",
                                            fullyQualifiedName, conf.getReadAheadWaitTime());
                            }

                            schedule(ReadAheadWorker.this, conf.getReadAheadWaitTime());
                        } else  {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Initialized metadata for {}, starting reading ahead from {} : {}.",
                                        new Object[] { fullyQualifiedName, currentMetadataIndex, currentMetadata });
                            }
                            next.process(BKException.Code.OK);
                        }

                        // Once we have read the ledger list for the first time, subsequent segments
                        // should be read in a streaming manner and we should get the new ledgers as
                        // they close in ZK through watchers.
                        // So lets start observing the latency
                        bkLedgerManager.reportGetSegmentStats = true;
                    }
                });
            }

            @Override
            void process(int rc) {
                inProgressChanged = false;
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Checking {} if InProgress changed.", fullyQualifiedName);
                }
                if (reInitializeMetadata || null == currentMetadata) {
                    reInitializeMetadata = false;
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Reinitializing metadata for {}.", fullyQualifiedName);
                    }
                    if (metadataNotificationTimeMillis > 0) {
                        long metadataReinitializeTimeMillis = System.currentTimeMillis();
                        long elapsedMillisSinceMetadataChanged = metadataReinitializeTimeMillis - metadataNotificationTimeMillis;
                        if (elapsedMillisSinceMetadataChanged >= BKLogPartitionReadHandler.this.metadataLatencyWarnThresholdMillis) {
                            LOG.warn("{} reinitialize metadata at {}, which is {} millis after receiving notification at {}.",
                                     new Object[] { getFullyQualifiedName(), metadataReinitializeTimeMillis,
                                                    elapsedMillisSinceMetadataChanged, metadataNotificationTimeMillis});
                        }
                        metadataReinitializationStat.registerSuccessfulEvent(
                                TimeUnit.MILLISECONDS.toMicros(elapsedMillisSinceMetadataChanged));
                        metadataNotificationTimeMillis = -1L;
                    }
                    bkLedgerManager.asyncGetLedgerList(LogSegmentLedgerMetadata.COMPARATOR, ReadAheadWorker.this, this);
                } else {
                    next.process(BKException.Code.OK);
                }
            }
        }

        final class OpenLedgerPhase extends Phase
                implements BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>,
                            AsyncCallback.ReadLastConfirmedCallback,
                            AsyncCallback.ReadLastConfirmedAndEntryCallback {

            OpenLedgerPhase(Phase next) {
                super(next);
            }

            @Override
            void process(int rc) {
                if (currentMetadata.isInProgress()) { // we don't want to fence the current journal
                    if (null == currentLH) {
                        if (conf.getTraceReadAheadMetadataChanges()) {
                            LOG.info("Opening inprogress ledger of {} for {} at {}.",
                                     new Object[] { currentMetadata, fullyQualifiedName, System.currentTimeMillis() });
                        }
                        bkLedgerManager.getHandleCache().asyncOpenLedger(currentMetadata, false, this);
                    } else {
                        long lastAddConfirmed;
                        try {
                            lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                        } catch (IOException ie) {
                            // Exception is thrown due to no ledger handle
                            handleException(ReadAheadPhase.OPEN_LEDGER, BKException.Code.NoSuchLedgerExistsException);
                            return;
                        }
                        if (lastAddConfirmed < nextReadPosition.getEntryId()) {
                            // the readahead is caught up if current ledger is in progress and read position moves over last add confirmed
                            if (isCatchingUp) {
                                isCatchingUp = false;
                                ledgerDataAccessor.setSuppressDeliveryLatency(false);
                            }

                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Reading last add confirmed of {} for {}, as read poistion has moved over {} : {}, lac option: {}",
                                        new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadPosition, conf.getReadLACOption()});
                            }
                            if (nextReadPosition.getEntryId() == 0 && conf.getTraceReadAheadMetadataChanges()) {
                                // we are waiting for first entry to arrive
                                LOG.info("Reading last add confirmed for {} at {}: lac = {}, position = {}.",
                                         new Object[] { fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadPosition });
                            }
                            String ctx;
                            boolean callbackImmediately;
                            switch(ReadLACOption.values()[conf.getReadLACOption()]) {
                                case LONGPOLL:
                                    ctx = String.format("LongPoll(%d)", lastAddConfirmed);
                                    ReadLastConfirmedCallbackWithNotification lpCallback =
                                            new ReadLastConfirmedCallbackWithNotification(lastAddConfirmed, this, ctx);
                                    callbackImmediately = setMetadataNotification(lpCallback);
                                    bkLedgerManager.getHandleCache().asyncReadLastConfirmedLongPoll(currentLH, conf.getReadLACLongPollTimeout(), lpCallback, ctx);
                                    lpCallback.callbackImmediately(callbackImmediately);
                                    readAheadReadLACCounter.inc();
                                    break;
                                case READENTRYPIGGYBACK_PARALLEL:
                                    ctx = String.format("ReadLastConfirmedAndEntry(Parallel, %d)", lastAddConfirmed);
                                    ReadLastConfirmedAndEntryCallbackWithNotification pCallback =
                                            new ReadLastConfirmedAndEntryCallbackWithNotification(lastAddConfirmed, this, ctx);
                                    callbackImmediately = setMetadataNotification(pCallback);
                                    bkLedgerManager.getHandleCache().asyncReadLastConfirmedAndEntry(currentLH, conf.getReadLACLongPollTimeout(), true, pCallback, ctx);
                                    pCallback.callbackImmediately(callbackImmediately);
                                    readAheadReadLACAndEntryCounter.inc();
                                    break;
                                case READENTRYPIGGYBACK_SEQUENTIAL:
                                    ctx = String.format("ReadLastConfirmedAndEntry(Sequential, %d)", lastAddConfirmed);
                                    ReadLastConfirmedAndEntryCallbackWithNotification sCallback =
                                            new ReadLastConfirmedAndEntryCallbackWithNotification(lastAddConfirmed, this, ctx);
                                    callbackImmediately = setMetadataNotification(sCallback);
                                    bkLedgerManager.getHandleCache().asyncReadLastConfirmedAndEntry(currentLH, conf.getReadLACLongPollTimeout(), false, sCallback, ctx);
                                    sCallback.callbackImmediately(callbackImmediately);
                                    readAheadReadLACAndEntryCounter.inc();
                                    break;
                                default:
                                    ctx = String.format("ReadLastConfirmed(%d)", lastAddConfirmed);
                                    setMetadataNotification(null);
                                    bkLedgerManager.getHandleCache().asyncTryReadLastConfirmed(currentLH, this, ctx);
                                    readAheadReadLACCounter.inc();
                                    break;
                            }
                        } else {
                            next.process(BKException.Code.OK);
                        }
                    }
                } else {
                    if (null != currentLH) {
                        try {
                            if (inProgressChanged) {
                                if (LOG.isTraceEnabled()) {
                                    LOG.trace("Closing completed ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                                }
                                bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                inProgressChanged = false;
                                currentLH = null;
                            } else {
                                long lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                                if (nextReadPosition.getEntryId() > lastAddConfirmed) {
                                    // Its possible that the last entryId does not account for the control
                                    // log record, but the lastAddConfirmed should never be short of the
                                    // last entry id; else we maybe missing an entry
                                    if (lastAddConfirmed < currentMetadata.getLastEntryId()) {
                                        bkLedgerManager.raiseAlert("Unexpected last entry id during read ahead; {} , {}",
                                            currentMetadata, lastAddConfirmed);
                                        setReadAheadError();
                                    } else {
                                        // This disconnect will only surface during repositioning and
                                        // will not silently miss records; therefore its safe to not halt
                                        // reading, but we should print a warning for easy diagnosis
                                        if (lastAddConfirmed > (currentMetadata.getLastEntryId() + 1)) {
                                            LOG.warn("Metadata {} is corrupt for stream {}, lastAddConfirmed {}",
                                                    new Object[] {currentMetadata, getFullyQualifiedName(), lastAddConfirmed});
                                        }

                                        if (LOG.isTraceEnabled()) {
                                            LOG.trace("Past the last Add Confirmed in ledger {} for {}", currentMetadata, fullyQualifiedName);
                                        }
                                        bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                        LogSegmentLedgerMetadata oldMetadata = currentMetadata;
                                        currentLH = null;
                                        currentMetadata = null;
                                        if (currentMetadataIndex + 1 < ledgerList.size()) {
                                            currentMetadata = ledgerList.get(++currentMetadataIndex);
                                            if (currentMetadata.getLedgerSequenceNumber() != (oldMetadata.getLedgerSequenceNumber() + 1)) {
                                                // We should never get here as we should have exited the loop if
                                                // pendingRequests were empty
                                                bkLedgerManager.raiseAlert("Unexpected condition during read ahead; {} , {}",
                                                    currentMetadata, oldMetadata);
                                                setReadAheadError();
                                            } else {
                                                if (LOG.isTraceEnabled()) {
                                                    LOG.trace("Moving read position to a new ledger {} for {}.",
                                                        currentMetadata, fullyQualifiedName);
                                                }
                                                nextReadPosition.positionOnNewLedger(currentMetadata.getLedgerId(), currentMetadata.getLedgerSequenceNumber());
                                            }
                                        }
                                    }
                                }
                            }
                            if (!readAheadError) {
                                next.process(BKException.Code.OK);
                            }
                        } catch (InterruptedException ie) {
                            LOG.debug("Interrupted while repositioning", ie);
                            handleException(ReadAheadPhase.CLOSE_LEDGER, BKException.Code.InterruptedException);
                        } catch (IOException ioe) {
                            LOG.debug("Exception while repositioning", ioe);
                            handleException(ReadAheadPhase.CLOSE_LEDGER, BKException.Code.NoSuchLedgerExistsException);
                        } catch (BKException bke) {
                            LOG.debug("Exception while repositioning", bke);
                            handleException(ReadAheadPhase.CLOSE_LEDGER, bke.getCode());
                        }
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Opening completed ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                        }
                        bkLedgerManager.getHandleCache().asyncOpenLedger(currentMetadata, true, this);
                    }
                }

            }

            @Override
            public void operationComplete(final int rc, final LedgerDescriptor result) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            LOG.debug("BK Exception {} while opening ledger", rc);
                            handleException(ReadAheadPhase.OPEN_LEDGER, rc);
                            return;
                        }
                        currentLH = result;
                        if (conf.getTraceReadAheadMetadataChanges()) {
                            LOG.info("Opened inprogress ledger of {} for {} at {}.",
                                    new Object[] { currentMetadata, fullyQualifiedName, System.currentTimeMillis() });
                        }
                        bkcZkExceptions.set(0);
                        bkcUnExpectedExceptions.set(0);
                        next.process(rc);
                    }
                });
            }

            @Override
            public void readLastConfirmedComplete(final int rc, final long lastConfirmed, final Object ctx) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            handleException(ReadAheadPhase.READ_LAST_CONFIRMED, rc);
                            return;
                        }
                        bkcZkExceptions.set(0);
                        bkcUnExpectedExceptions.set(0);
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Advancing Last Add Confirmed of {} for {} : {}",
                                    new Object[] { currentMetadata, fullyQualifiedName, lastConfirmed });
                        }
                        next.process(rc);
                    }
                });
            }

            public void readLastConfirmedAndEntryComplete(final int rc, final long lastConfirmed, final LedgerEntry entry,
                                                          final Object ctx) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            LOG.debug("BK Exception {} in read last add confirmed and entry", rc);
                            handleException(ReadAheadPhase.READ_LAST_CONFIRMED, rc);
                            return;
                        }
                        bkcZkExceptions.set(0);
                        bkcUnExpectedExceptions.set(0);
                        if (LOG.isTraceEnabled()) {
                            try {
                                LOG.trace("Advancing Last Add Confirmed of {} for {} : {}, {}",
                                          new Object[] { currentMetadata, fullyQualifiedName, lastConfirmed,
                                                         bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH) });
                            } catch (IOException exc) {
                                // Ignore
                            }
                        }

                        if ((null != entry)
                            && (nextReadPosition.getEntryId() == entry.getEntryId())
                            && (nextReadPosition.getLedgerId() == entry.getLedgerId())) {

                            nextReadPosition.advance();

                            if (lastConfirmed <= 4 && conf.getTraceReadAheadMetadataChanges()) {
                                LOG.info("Hit readLastConfirmedAndEntry for {} at {} : entry = {}, lac = {}, position = {}.",
                                        new Object[] { fullyQualifiedName, System.currentTimeMillis(),
                                                       entry.getEntryId(), lastConfirmed, nextReadPosition });
                            }

                            ledgerDataAccessor.set(new LedgerReadPosition(entry.getLedgerId(), currentLH.getLedgerSequenceNo(), entry.getEntryId()),
                                                   entry, null != ctx ? ctx.toString() : "");

                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Reading the value received {} for {} : entryId {}",
                                    new Object[] { currentMetadata, fullyQualifiedName, entry.getEntryId() });
                            }
                            readAheadEntryPiggyBackHits.inc();
                        } else {
                            readAheadEntryPiggyBackMisses.inc();
                        }
                        next.process(rc);
                    }
                });
            }
        }

        final class ReadEntriesPhase extends Phase implements AsyncCallback.ReadCallback, Runnable {

            boolean cacheFull = false;
            long lastAddConfirmed = -1;

            ReadEntriesPhase(Phase next) {
                super(next);
            }

            @Override
            void process(int rc) {
                cacheFull = false;
                lastAddConfirmed = -1;
                if (null != currentLH) {
                    try {
                        lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                    } catch (IOException e) {
                        handleException(ReadAheadPhase.READ_LAST_CONFIRMED, BKException.Code.NoSuchLedgerExistsException);
                        return;
                    }
                    read();
                } else {
                    complete();
                }
            }

            private void read() {
                if (lastAddConfirmed < nextReadPosition.getEntryId()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Nothing to read for {} of {} : lastAddConfirmed = {}, nextReadPosition = {}",
                                new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadPosition });
                    }
                    complete();
                    return;
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Reading entry {} for {} of {}.",
                            new Object[] { nextReadPosition, currentMetadata, fullyQualifiedName });
                }
                long startEntryId = nextReadPosition.getEntryId();
                long endEntryId = Math.min(lastAddConfirmed, (nextReadPosition.getEntryId() + conf.getReadAheadBatchSize() - 1));

                if (endEntryId <= conf.getReadAheadBatchSize() && conf.getTraceReadAheadMetadataChanges()) {
                    // trace first read batch
                    LOG.info("Reading entries ({} - {}) for {} at {} : lac = {}, nextReadPosition = {}.",
                             new Object[] { startEntryId, endEntryId, fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadPosition });
                }

                bkLedgerManager.getHandleCache().asyncReadEntries(currentLH, startEntryId, endEntryId , this,
                                                                  String.format("ReadEntries(%d-%d)", startEntryId, endEntryId));
            }

            @Override
            public void readComplete(final int rc, final LedgerHandle lh,
                                     final Enumeration<LedgerEntry> seq, final Object ctx) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc || null == lh) {
                            readAheadReadEntriesStat.registerFailedEvent(0);
                            LOG.debug("BK Exception {} while reading entry", rc);
                            handleException(ReadAheadPhase.READ_ENTRIES, rc);
                            return;
                        }
                        int numReads = 0;
                        while (seq.hasMoreElements()) {
                            bkcZkExceptions.set(0);
                            bkcUnExpectedExceptions.set(0);
                            nextReadPosition.advance();
                            LedgerEntry e = seq.nextElement();
                            LedgerReadPosition readPosition = new LedgerReadPosition(e.getLedgerId(), currentMetadata.getLedgerSequenceNumber(), e.getEntryId());
                            ledgerDataAccessor.set(readPosition, e, null != ctx ? ctx.toString() : "");
                            ++numReads;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Read entry {} of {}.", readPosition, fullyQualifiedName);
                            }
                        }
                        readAheadReadEntriesStat.registerSuccessfulEvent(numReads);
                        if (ledgerDataAccessor.getNumCacheEntries() >= conf.getReadAheadMaxEntries()) {
                            cacheFull = true;
                            complete();
                        } else {
                            read();
                        }
                    }
                });
            }

            private void complete() {
                if (cacheFull) {
                    if (LOG.isTraceEnabled()) {
                        LOG.info("Cache for {} is full. Backoff reading ahead for {} ms.",
                            fullyQualifiedName, conf.getReadAheadWaitTime());
                    }
                    ledgerDataAccessor.setReadAheadCallback(ReadAheadWorker.this, conf.getReadAheadWaitTime());
                } else if ((null != currentMetadata) && currentMetadata.isInProgress() && (ReadLACOption.DEFAULT.value == conf.getReadLACOption())) {
                    if (LOG.isTraceEnabled()) {
                        LOG.info("Reached End of inprogress ledger {}. Backoff reading ahead for {} ms.",
                            fullyQualifiedName, conf.getReadAheadWaitTime());
                    }
                    // Backoff before resuming
                    schedule(ReadAheadWorker.this, conf.getReadAheadWaitTime());
                } else {
                    run();
                }
            }

            @Override
            public void run() {
                next.process(BKException.Code.OK);
            }
        }

        @Override
        public void run() {
            readAheadPhase.process(BKException.Code.OK);
        }

        @Override
        public void process(WatchedEvent event) {
            if ((event.getType() == Watcher.Event.EventType.None)
                    && (event.getState() == Watcher.Event.KeeperState.SyncConnected)) {
                LOG.debug("Reconnected ...");
            } else if (((event.getType() == Event.EventType.None) && (event.getState() == Event.KeeperState.Expired)) ||
                       ((event.getType() == Event.EventType.NodeChildrenChanged))) {
                AsyncNotification notification;
                synchronized (notificationLock) {
                    reInitializeMetadata = true;
                    LOG.debug("{} Read ahead node changed", fullyQualifiedName);
                    notification = metadataNotification;
                    metadataNotification = null;
                }
                metadataNotificationTimeMillis = System.currentTimeMillis();
                if (null != notification) {
                    notification.notifyOnOperationComplete();
                }
            }
        }

        /**
         * Set metadata notification and return the flag indicating whether to reinitialize metadata.
         *
         * @param notification
         *          metadata notification
         * @return flag indicating whether to reinitialize metadata.
         */
        private boolean setMetadataNotification(AsyncNotification notification) {
            synchronized (notificationLock) {
                this.metadataNotification = notification;
                return reInitializeMetadata;
            }
        }

        abstract class LongPollNotification<T> implements AsyncNotification {

            final long lac;
            final T cb;
            final Object ctx;
            final AtomicBoolean called = new AtomicBoolean(false);
            final long startNanos;

            LongPollNotification(long lac, T cb, Object ctx) {
                this.lac = lac;
                this.cb = cb;
                this.ctx = ctx;
                this.startNanos = MathUtils.nowInNano();
            }

            void complete(boolean success) {
                long startTime = MathUtils.nowInNano();
                doComplete(success);
                if (success) {
                    notificationExecutionStat.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTime));
                } else {
                    notificationExecutionStat.registerFailedEvent(MathUtils.elapsedMicroSec(startTime));
                }
            }

            abstract void doComplete(boolean success);

            @Override
            public void notifyOnError() {
                longPollInterruptionStat.registerFailedEvent(MathUtils.elapsedMicroSec(startNanos));
                complete(false);
            }

            @Override
            public void notifyOnOperationComplete() {
                longPollInterruptionStat.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startNanos));
                complete(true);
            }

            void callbackImmediately(boolean immediate) {
                if (immediate) {
                    complete(true);
                }
            }
        }

        class ReadLastConfirmedCallbackWithNotification
                extends LongPollNotification<AsyncCallback.ReadLastConfirmedCallback>
                implements AsyncCallback.ReadLastConfirmedCallback {

            ReadLastConfirmedCallbackWithNotification(
                    long lac, AsyncCallback.ReadLastConfirmedCallback cb, Object ctx) {
                super(lac, cb, ctx);
            }

            @Override
            public void readLastConfirmedComplete(int rc, long lac, Object ctx) {
                if (called.compareAndSet(false, true)) {
                    // clear the notification when callback
                    synchronized (notificationLock) {
                        metadataNotification = null;
                    }
                    this.cb.readLastConfirmedComplete(rc, lac, ctx);
                }
            }

            @Override
            void doComplete(boolean success) {
                readLastConfirmedComplete(BKException.Code.OK, lac, ctx);
            }

        }

        class ReadLastConfirmedAndEntryCallbackWithNotification
                extends LongPollNotification<AsyncCallback.ReadLastConfirmedAndEntryCallback>
                implements AsyncCallback.ReadLastConfirmedAndEntryCallback {

            ReadLastConfirmedAndEntryCallbackWithNotification(
                    long lac, AsyncCallback.ReadLastConfirmedAndEntryCallback cb, Object ctx) {
                super(lac, cb, ctx);
            }

            @Override
            public void readLastConfirmedAndEntryComplete(int rc, long lac, LedgerEntry entry, Object ctx) {
                if (called.compareAndSet(false, true)) {
                    // clear the notification when callback
                    synchronized (notificationLock) {
                        metadataNotification = null;
                    }
                    this.cb.readLastConfirmedAndEntryComplete(rc, lac, entry, ctx);
                }
            }

            @Override
            void doComplete(boolean success) {
                readLastConfirmedAndEntryComplete(BKException.Code.OK, lac, null, ctx);
            }

        }
    }

}
