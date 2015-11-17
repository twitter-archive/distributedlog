package com.twitter.distributedlog.v2;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.LogReadException;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.v2.util.FutureUtils;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import static com.twitter.distributedlog.DLSNUtil.*;

class BKLogPartitionReadHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    private static final int LAYOUT_VERSION = -1;
    private LedgerDataAccessor ledgerDataAccessor = null;
    private final LedgerHandleCache handleCache;

    private ReadAheadWorker readAheadWorker = null;
    private volatile boolean readAheadError = false;
    private volatile boolean readAheadInterrupted = false;

    private final long checkLogExistenceBackoffStartMs;
    private final long checkLogExistenceBackoffMaxMs;
    private long checkLogExistenceBackoffMs;

    // stats
    private final Counter readAheadWorkerWaits;
    private final Counter readAheadRepositions;

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
                                     StatsLogger statsLogger) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder, executorService, statsLogger);

        handleCache = new LedgerHandleCache(this.bookKeeperClient, this.digestpw);
        ledgerDataAccessor = new LedgerDataAccessor(handleCache, getFullyQualifiedName(), statsLogger);

        this.checkLogExistenceBackoffStartMs = conf.getCheckLogExistenceBackoffStartMillis();
        this.checkLogExistenceBackoffMaxMs = conf.getCheckLogExistenceBackoffMaxMillis();
        this.checkLogExistenceBackoffMs = this.checkLogExistenceBackoffStartMs;

        // Stats
        StatsLogger readAheadStatsLogger = statsLogger.scope("readahead_worker");
        readAheadWorkerWaits = readAheadStatsLogger.getCounter("wait");
        readAheadRepositions = readAheadStatsLogger.getCounter("repositions");
    }

    Pair<ResumableBKPerStreamLogReader, Boolean> getInputStream(long fromTxId, boolean fThrowOnEmpty, Watcher watch)
        throws IOException {
        boolean logExists = false;
        try {
            if (null != zooKeeperClient.get().exists(ledgerPath, false)) {
                logExists = true;
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while checking existence of {}", ledgerPath, ie);
            throw new DLInterruptedException("Interrupted while checking existence of " + ledgerPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Encountered zookeeper exception while checking existence of {} : ", ledgerPath, ke);
            throw new IOException("Encountered zookeeper exception while checking existence of " + ledgerPath, ke);
        }

        if (logExists) {
            for (LogSegmentLedgerMetadata l : getLedgerList(watch)) {
                LOG.debug("Inspecting Ledger: {}", l);
                long lastTxId = l.getLastTxId();
                LogRecordWithDLSN lastRecord = null;
                if (l.isInProgress()) {
                    try {
                        lastRecord = recoverLastRecordInLedger(l, false, false, true);
                        if (null == lastRecord) {
                            lastTxId = DistributedLogConstants.EMPTY_LEDGER_TX_ID;
                        } else {
                            lastTxId = lastRecord.getTransactionId();
                        }
                    } catch (DLInterruptedException die) {
                        throw die;
                    } catch (IOException exc) {
                        lastTxId = l.getFirstTxId();
                        LOG.info("Reading beyond flush point");
                    }

                    if (lastTxId == DistributedLogConstants.INVALID_TXID) {
                        lastTxId = l.getFirstTxId();
                    }

                    long firstEntryId;
                    if (null == lastRecord) {
                        firstEntryId = 0L;
                    } else {
                        firstEntryId = getEntryId(lastRecord.getDlsn());
                    }

                    if (fromTxId > lastTxId) {
                        LOG.info("Opened resumable reader on partition {} starting at TxId: {}, EntryId: {} which is beyond the last txid {}",
                            new Object[] { getFullyQualifiedName(), fromTxId, firstEntryId, lastTxId });
                        ResumableBKPerStreamLogReader s
                            = new ResumableBKPerStreamLogReader(this, zooKeeperClient, ledgerDataAccessor, l, firstEntryId, statsLogger);
                        s.setSkipThreshold(fromTxId);
                        return Pair.of(s, true);
                    }
                }

                if (fromTxId <= lastTxId) {
                    try {
                        LOG.info("Search transaction id {} in log segment {} : from tx id = {}, last tx id = {}, last record = {}",
                                new Object[] { fromTxId, l, fromTxId, lastTxId, lastRecord });
                        long firstEntryId;
                        if (fromTxId < l.getFirstTxId() || null == lastRecord || getEntryId(lastRecord.getDlsn()) == 0) {
                            firstEntryId = 0L;
                        } else {
                            try {
                                Optional<LogRecordWithDLSN> record = FutureUtils.result(ReadUtils.getLogRecordNotLessThanTxId(
                                        name,
                                        l,
                                        fromTxId,
                                        executorService,
                                        handleCache,
                                        Math.max(2, conf.getReadAheadMaxEntries())));
                                firstEntryId = record.isPresent() ? getEntryId(record.get().getDlsn()) : 0L;
                            } catch (IOException ioe) {
                                LOG.warn("Failed to search transaction id {} in log segment {}, fall back to search from begin ...",
                                        fromTxId, l);
                                firstEntryId = 0L;
                            }
                        }
                        ResumableBKPerStreamLogReader s
                            = new ResumableBKPerStreamLogReader(this, zooKeeperClient, ledgerDataAccessor, l,
                                firstEntryId, statsLogger);
                        if (s.skipTo(fromTxId)) {
                            return Pair.of(s, true);
                        } else {
                            s.close();
                            if (!l.isInProgress()) {
                                LOG.warn("Detected a segment with corrupt metadata {} while positioning on {}, continuing to the next segment", l, fromTxId);
                            }
                        }
                    } catch (IOException e) {
                        LOG.error("Could not open ledger for the stream " + getFullyQualifiedName() + " for startTxId " + fromTxId, e);
                        throw e;
                    }
                }
                ledgerDataAccessor.purgeReadAheadCache(new LedgerReadPosition(l.getLedgerId(), l.getLedgerSequenceNumber(), Long.MAX_VALUE));
            }
        } else {
            if (fThrowOnEmpty) {
                throw new LogNotFoundException(String.format("Log %s does not exist or has been deleted", getFullyQualifiedName()));
            } else if (checkLogExistenceBackoffStartMs > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(checkLogExistenceBackoffMs);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted on backoff checking existence of stream {} : ", getFullyQualifiedName(), e);
                    Thread.currentThread().interrupt();
                }
                checkLogExistenceBackoffMs = Math.min(checkLogExistenceBackoffMs * 2, checkLogExistenceBackoffMaxMs);
            }
        }

        return Pair.of(null, logExists);
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

    public void startReadAhead(LedgerReadPosition startPosition) {
        if (null == readAheadWorker) {
            readAheadWorker = new ReadAheadWorker(getFullyQualifiedName(),
                this,
                startPosition,
                ledgerDataAccessor,
                conf);
            readAheadWorker.start();
            ledgerDataAccessor.setReadAheadEnabled(true, conf.getReadAheadWaitTime(), conf.getReadAheadBatchSize());
        } else {
            readAheadWorker.advanceReadAhead(startPosition);
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
    }

    public void setReadAheadInterrupted() {
        readAheadInterrupted = true;
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

    @VisibleForTesting
    void disableReadAheadZKNotification() {
        if (null != readAheadWorker) {
            readAheadWorker.disableZKNotification();
        }
    }

    static interface ReadAheadCallback {
        void resumeReadAhead();
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

        private final DistributedLogConfiguration conf;
        private final String fullyQualifiedName;
        private final BKLogPartitionReadHandler bkLedgerManager;
        private volatile boolean reInitializeMetadata = true;
        private LedgerReadPosition nextReadAheadPosition;
        // Indicates the position to which the continuous log reader has been positioned. Its not
        // an accurate position as its only updated when necessary
        private AtomicReference<LedgerReadPosition> continuousReaderPosition = new AtomicReference<LedgerReadPosition>(null);
        private LogSegmentLedgerMetadata currentMetadata = null;
        private int currentMetadataIndex;
        private LedgerDescriptor currentLH;
        private final LedgerDataAccessor ledgerDataAccessor;
        private List<LogSegmentLedgerMetadata> ledgerList;
        private final long readAheadBatchSize;
        private final long readAheadMaxEntries;
        private final long readAheadWaitTime;
        private static final int BKC_ZK_EXCEPTION_THRESHOLD_IN_SECONDS = 30;
        private static final int BKC_UNEXPECTED_EXCEPTION_THRESHOLD = 3;

        // Parameters for the state for this readahead worker.
        volatile boolean running = true;
        volatile boolean encounteredException = false;
        private final AtomicInteger bkcZkExceptions = new AtomicInteger(0);
        private final AtomicInteger bkcUnExpectedExceptions = new AtomicInteger(0);
        private final int noLedgerExceptionOnReadLACThreshold;
        private final AtomicInteger bkcNoLedgerExceptionsOnReadLAC = new AtomicInteger(0);
        final Object stopNotification = new Object();
        volatile boolean inProgressChanged = false;
        volatile boolean zkNotificationDisabled = false;

        // Metadata Notification
        final Object notificationLock = new Object();
        AsyncNotification metadataNotification = null;

        // ReadAhead Phases
        final Phase schedulePhase = new ScheduleReadAheadPhase();
        final Phase exceptionHandler = new ExceptionHandlePhase(schedulePhase);
        final Phase readAheadPhase =
                new CheckInProgressChangedPhase(new OpenLedgerPhase(new ReadEntriesPhase(schedulePhase)));
        final Stopwatch lastLedgerCloseDetected = Stopwatch.createUnstarted();

        public ReadAheadWorker(String fullyQualifiedName,
                               BKLogPartitionReadHandler ledgerManager,
                               LedgerReadPosition startPosition,
                               LedgerDataAccessor ledgerDataAccessor,
                               DistributedLogConfiguration conf) {
            this.bkLedgerManager = ledgerManager;
            this.fullyQualifiedName = fullyQualifiedName;
            this.nextReadAheadPosition = startPosition;
            this.ledgerDataAccessor = ledgerDataAccessor;
            this.conf = conf;
            this.readAheadBatchSize = conf.getReadAheadBatchSize();
            this.readAheadMaxEntries = conf.getReadAheadMaxEntries();
            this.readAheadWaitTime = conf.getReadAheadWaitTime();
            this.noLedgerExceptionOnReadLACThreshold =
                    conf.getReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis() / conf.getReadAheadWaitTime();
        }

        public void start() {
            running = true;
            schedulePhase.process(BKException.Code.OK);
        }

        public void stop() {
            running = false;

            // Aside from unfortunate naming of variables, this allows
            // the currently active long poll to be interrupted and completed
            AsyncNotification notification;
            synchronized (notificationLock) {
                notification = metadataNotification;
                metadataNotification = null;
            }
            if (null != notification) {
                notification.notifyOnOperationComplete();
            }
            try {
                synchronized (stopNotification) {
                    // It would take at most 2 times the read ahead wait time for the schedule phase
                    // to be executed
                    stopNotification.wait(2 * conf.getReadAheadWaitTime());
                }
                LOG.info("Done waiting for ReadAhead Worker to stop {}", fullyQualifiedName);
            } catch (InterruptedException exc) {
                LOG.info("{}: Interrupted while waiting for ReadAhead Worker to stop", fullyQualifiedName, exc);
            }
        }

        public void advanceReadAhead(LedgerReadPosition readerPosition) {
            continuousReaderPosition.set(readerPosition);
        }

        @Override
        public String toString() {
            return "Running:" + running +
                ", NextReadPosition:" + nextReadAheadPosition +
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
                executorService.submit(addRTEHandler(runnable));
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError();
            }
        }

        private void schedule(Runnable runnable, long timeInMillis) {
            try {
                readAheadWorkerWaits.inc();
                executorService.schedule(addRTEHandler(runnable), timeInMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError();
            }
        }

        private void handleException(int returnCode) {
            exceptionHandler.process(returnCode);
        }

        private boolean closeCurrentLedgerHandle() {
            if (currentLH == null) {
                return true;
            }
            boolean retVal = false;
            LedgerDescriptor ld = currentLH;
            try {
                bkLedgerManager.getHandleCache().closeLedger(ld);
                currentLH = null;
                retVal = true;
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted during closing {} : ", ld, ie);
                handleException(BKException.Code.InterruptedException);
            } catch (BKException bke) {
                LOG.debug("BK Exception during closing {} : ", ld, bke);
                handleException(bke.getCode());
            }

            return retVal;
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
                    synchronized (stopNotification) {
                        stopNotification.notifyAll();
                    }
                    LOG.info("Stopped ReadAheadWorker for {}", fullyQualifiedName);
                    return;
                }
                if (encounteredException) {
                    if (bkcZkExceptions.get() > (BKC_ZK_EXCEPTION_THRESHOLD_IN_SECONDS * 1000 * 4 / readAheadWaitTime)) {
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
                            LOG.trace("Scheduling read ahead for {} after {} ms.", fullyQualifiedName, readAheadWaitTime/4);
                        }
                        schedule(ReadAheadWorker.this, readAheadWaitTime / 4);
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
                    LOG.debug("ReadAhead Worker for {} encountered zookeeper exception : total exceptions are {}.",
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
                    LOG.debug("ReadAhead Worker for {} encountered exception : ",
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
                        if (BKException.Code.OK != rc) {
                            reInitializeMetadata = true;
                            exceptionHandler.process(rc);
                            return;
                        }
                        ledgerList = result;
                        for (int i = 0; i < ledgerList.size(); i++) {
                            LogSegmentLedgerMetadata l = ledgerList.get(i);
                            if (l.getLedgerId() == nextReadAheadPosition.getLedgerId()) {
                                if (currentMetadata != null) {
                                    inProgressChanged = currentMetadata.isInProgress() && !l.isInProgress();
                                }
                                currentMetadata = l;
                                currentMetadataIndex = i;
                                break;
                            }
                        }
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Initialized metadata for {}, starting reading ahead from {} : {}.",
                                    new Object[]{fullyQualifiedName, currentMetadataIndex, currentMetadata});
                        }
                        next.process(BKException.Code.OK);
                    }
                });
            }

            @Override
            void process(int rc) {
                if (!running) {
                    synchronized (stopNotification) {
                        stopNotification.notifyAll();
                    }
                    LOG.info("Stopped ReadAheadWorker for {}", fullyQualifiedName);
                    return;
                }

                inProgressChanged = false;
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Checking {} if InProgress changed.", fullyQualifiedName);
                }

                // If the read ahead thread is not advancing to a new log segment, we
                // must check if it needs to be forced to advance (in cases when its
                // stuck)
                if (!reInitializeMetadata && null != currentMetadata) {
                    LedgerReadPosition readerPosition = continuousReaderPosition.get();
                    if ((null != readerPosition) &&
                        nextReadAheadPosition.definitelyLessThan(readerPosition)) {
                        LOG.info("{}: Advancing ReadAhead position to {}", fullyQualifiedName, readerPosition);
                        nextReadAheadPosition = readerPosition;
                        if (!closeCurrentLedgerHandle()) {
                            return;
                        }
                        currentMetadata = null;
                        ledgerDataAccessor.purgeReadAheadCache(readerPosition);
                        readAheadRepositions.inc();
                    }
                }

                if (reInitializeMetadata || null == currentMetadata) {
                    reInitializeMetadata = false;
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Reinitializing metadata for {}.", fullyQualifiedName);
                    }
                    bkLedgerManager.getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, ReadAheadWorker.this, this);
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

            private void issueReadLastConfirmedAndEntry(boolean parallel, long lastAddConfirmed) {
                String ctx = String.format("ReadLastConfirmedAndEntry(%s, %d)", parallel? "Parallel":"Sequential", lastAddConfirmed);
                ReadLastConfirmedAndEntryCallbackWithNotification callback =
                    new ReadLastConfirmedAndEntryCallbackWithNotification(lastAddConfirmed, this, ctx);
                boolean callbackImmediately = setMetadataNotification(callback);
                bkLedgerManager.getHandleCache().asyncReadLastConfirmedAndEntry(currentLH, nextReadAheadPosition.getEntryId(),
                    conf.getReadLACLongPollTimeout(), parallel, callback, ctx);
                callback.callbackImmediately(callbackImmediately);
            }

            @Override
            void process(int rc) {
                if (currentMetadata.isInProgress()) { // we don't want to fence the current journal
                    if (null == currentLH) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Opening ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                        }
                        bkLedgerManager.getHandleCache().asyncOpenLedger(currentMetadata, false, this);
                    } else {
                        long lastAddConfirmed;
                        boolean isClosed;
                        try {
                            isClosed = bkLedgerManager.getHandleCache().isLedgerHandleClosed(currentLH);
                            lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                        } catch (IOException ie) {
                            // Exception is thrown due to no ledger handle
                            handleException(BKException.Code.NoSuchLedgerExistsException);
                            return;
                        }
                        if (lastAddConfirmed < nextReadAheadPosition.getEntryId()) {
                            if (isClosed) {
                                // This indicates that the currentMetadata is still marked in
                                // progress while the ledger has been closed. This specific ledger
                                // is not going to produce any more entries - so we should
                                // be reading metadata entries to mark the current metadata
                                // as complete
                                if (lastLedgerCloseDetected.isRunning()) {
                                    if (lastLedgerCloseDetected.elapsed(TimeUnit.MILLISECONDS)
                                        > conf.getReaderIdleWarnThresholdMillis()) {
                                        LOG.info("{} Ledger {} for inprogress segment {} closed for idle reader warn threshold",
                                            new Object[] { fullyQualifiedName, currentMetadata, currentLH });
                                        reInitializeMetadata = true;
                                    }
                                } else {
                                    lastLedgerCloseDetected.reset().start();
                                }
                            } else {
                                lastLedgerCloseDetected.reset();
                            }

                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Reading last add confirmed of {} for {}, as read poistion has moved over {} : {}",
                                        new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadAheadPosition});
                            }

                            switch (ReadLACOption.values()[conf.getReadLACOption()]) {
                                case LONGPOLL:
                                    // Using deprecated option, falling back to read_entry_piggyback
                                case READENTRYPIGGYBACK_PARALLEL:
                                    issueReadLastConfirmedAndEntry(true, lastAddConfirmed);
                                    break;
                                case READENTRYPIGGYBACK_SEQUENTIAL:
                                    issueReadLastConfirmedAndEntry(false, lastAddConfirmed);
                                    break;
                                default:
                                    String ctx = String.format("ReadLastConfirmed(%d)", lastAddConfirmed);
                                    setMetadataNotification(null);
                                    bkLedgerManager.getHandleCache().asyncTryReadLastConfirmed(currentLH, this, ctx);
                                    break;
                            }
                        } else {
                            next.process(BKException.Code.OK);
                        }
                    }
                } else {
                    lastLedgerCloseDetected.reset();
                    if (null != currentLH) {
                        try {
                            if (inProgressChanged) {
                                if (!closeCurrentLedgerHandle()) {
                                    return;
                                }
                                inProgressChanged = false;
                            } else if (nextReadAheadPosition.getEntryId() > bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH)) {
                                bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                currentLH = null;
                                currentMetadata = null;
                                if (currentMetadataIndex + 1 < ledgerList.size()) {
                                    currentMetadata = ledgerList.get(++currentMetadataIndex);
                                    if (LOG.isTraceEnabled()) {
                                        LOG.trace("Moving read position to a new ledger {} for {}.",
                                                currentMetadata, fullyQualifiedName);
                                    }
                                    nextReadAheadPosition.positionOnNewLedger(currentMetadata.getLedgerId(), currentMetadata.getLedgerSequenceNumber());
                                }
                            }
                            next.process(BKException.Code.OK);
                        } catch (InterruptedException ie) {
                            handleException(BKException.Code.InterruptedException);
                        } catch (IOException ioe) {
                            handleException(BKException.Code.NoSuchLedgerExistsException);
                        } catch (BKException bke) {
                            handleException(bke.getCode());
                        }
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Opening ledger of {} for {}.", currentMetadata, fullyQualifiedName);
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
                            exceptionHandler.process(rc);
                            return;
                        }
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Opened ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                        }
                        currentLH = result;
                        bkcZkExceptions.set(0);
                        bkcUnExpectedExceptions.set(0);
                        bkcNoLedgerExceptionsOnReadLAC.set(0);
                        next.process(rc);
                    }
                });
            }

            /**
             * Handle the result of reading last add confirmed.
             *
             * @param rc
             *          result of reading last add confirmed
             */
            private void handleReadLastConfirmedError(int rc) {
                if (BKException.Code.NoSuchLedgerExistsException == rc) {
                    if (bkcNoLedgerExceptionsOnReadLAC.incrementAndGet() > noLedgerExceptionOnReadLACThreshold) {
                        LOG.info("{} No entries published to ledger {} yet for {} millis.",
                                new Object[] { fullyQualifiedName, currentLH, conf.getReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis() });
                        bkcNoLedgerExceptionsOnReadLAC.set(0);
                        // set current ledger handle to null, so it would be re-opened again.
                        // if the ledger does disappear, it would trigger re-initialize log segments on handling openLedger exceptions
                        if (closeCurrentLedgerHandle()) {
                            next.process(BKException.Code.OK);
                        }
                        return;
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.info("{} No entries published to ledger {} yet. Backoff reading ahead for {} ms.",
                                    new Object[] { fullyQualifiedName, currentLH, conf.getReadAheadWaitTime() });
                        }
                        // Backoff before resuming
                        schedule(ReadAheadWorker.this, conf.getReadAheadWaitTime());
                        return;
                    }
                } else if (BKException.Code.OK != rc) {
                    handleException(rc);
                    return;
                }
            }

            @Override
            public void readLastConfirmedComplete(final int rc, final long lastConfirmed, final Object ctx) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            handleException(rc);
                            return;
                        }
                        bkcZkExceptions.set(0);
                        bkcUnExpectedExceptions.set(0);
                        bkcNoLedgerExceptionsOnReadLAC.set(0);
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Advancing Last Add Confirmed of {} for {} : {}",
                                    new Object[] { currentMetadata, fullyQualifiedName, lastConfirmed });
                        }
                        next.process(rc);
                    }
                });
            }

            @Override
            public void readLastConfirmedAndEntryComplete(final int rc, final long lastConfirmed, final LedgerEntry entry,
                                                          final Object ctx) {
                // submit callback execution to dlg executor to avoid deadlock
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            handleReadLastConfirmedError(rc);
                            return;
                        }
                        bkcZkExceptions.set(0);
                        bkcUnExpectedExceptions.set(0);
                        bkcNoLedgerExceptionsOnReadLAC.set(0);
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
                            && (nextReadAheadPosition.getEntryId() == entry.getEntryId())
                            && (nextReadAheadPosition.getLedgerId() == entry.getLedgerId())) {

                            nextReadAheadPosition.advance();

                            ledgerDataAccessor.set(new LedgerReadPosition(entry.getLedgerId(), currentLH.getLedgerSequenceNo(), entry.getEntryId()), entry);

                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Reading the value received {} for {} : entryId {}",
                                    new Object[] { currentMetadata, fullyQualifiedName, entry.getEntryId() });
                            }
                        } else {
                            if (lastConfirmed > nextReadAheadPosition.getEntryId()) {
                                LOG.info("{} : entry {} isn't piggybacked but last add confirmed already moves to {}.",
                                         new Object[] { getFullyQualifiedName(), nextReadAheadPosition.getEntryId(), lastConfirmed });
                            }
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
                        handleException(BKException.Code.NoSuchLedgerExistsException);
                        return;
                    }
                    read();
                } else {
                    complete();
                }
            }

            private void read() {
                if (lastAddConfirmed < nextReadAheadPosition.getEntryId()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Nothing to read for {} of {} : lastAddConfirmed = {}, nextReadPosition = {}",
                                new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadAheadPosition});
                    }
                    complete();
                    return;
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Reading entry {} for {} of {}.",
                            new Object[] {nextReadAheadPosition, currentMetadata, fullyQualifiedName });
                }
                bkLedgerManager.getHandleCache().asyncReadEntries(currentLH, nextReadAheadPosition.getEntryId(),
                    Math.min(lastAddConfirmed, (nextReadAheadPosition.getEntryId() + readAheadBatchSize - 1)), this, null);
            }

            @Override
            public void readComplete(final int rc, final LedgerHandle lh,
                                     final Enumeration<LedgerEntry> seq, final Object ctx) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc || null == lh) {
                            exceptionHandler.process(rc);
                            return;
                        }
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Read entries for {} of {}.", currentMetadata, fullyQualifiedName);
                        }
                        while (seq.hasMoreElements()) {
                            bkcZkExceptions.set(0);
                            bkcUnExpectedExceptions.set(0);
                            nextReadAheadPosition.advance();
                            LedgerEntry e = seq.nextElement();
                            ledgerDataAccessor.set(new LedgerReadPosition(e.getLedgerId(), currentLH.getLedgerSequenceNo(), e.getEntryId()), e);
                        }
                        if (ledgerDataAccessor.getNumCacheEntries() >= readAheadMaxEntries) {
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
                        LOG.trace("Cache for {} is full. Backoff reading ahead for {} ms.",
                            fullyQualifiedName, readAheadWaitTime);
                    }
                    ledgerDataAccessor.setReadAheadCallback(ReadAheadWorker.this, readAheadMaxEntries);
                } else if ((null != currentMetadata) && currentMetadata.isInProgress() && (ReadLACOption.DEFAULT.value == conf.getReadLACOption())) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Reached End of inprogress ledger. Backoff reading ahead for {} ms.",
                                fullyQualifiedName, readAheadWaitTime);
                    }
                    // Backoff before resuming
                    schedule(ReadAheadWorker.this, readAheadWaitTime);
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
        synchronized public void process(WatchedEvent event) {
            if ((event.getType() == Watcher.Event.EventType.None)
                    && (event.getState() == Watcher.Event.KeeperState.SyncConnected)) {
                LOG.debug("Reconnected ...");
            } else if (((event.getType() == Event.EventType.None) && (event.getState() == Event.KeeperState.Expired)) ||
                       ((event.getType() == Event.EventType.NodeChildrenChanged))) {
                AsyncNotification notification = null;
                synchronized (notificationLock) {
                    if (!zkNotificationDisabled) {
                        reInitializeMetadata = true;
                        notification = metadataNotification;
                        metadataNotification = null;
                        LOG.debug("Read ahead node changed");
                    } else {
                        LOG.info("{} ZK Notification was skipped", fullyQualifiedName);
                    }
                }
                if (null != notification) {
                    notification.notifyOnOperationComplete();
                }
            }
        }

        @VisibleForTesting
        void disableZKNotification() {
            LOG.info("{} ZK Notification was disabled", fullyQualifiedName);
            zkNotificationDisabled = true;
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

        @VisibleForTesting
        AsyncNotification getMetadataNotification() {
            synchronized (notificationLock) {
                return metadataNotification;
            }
        }

        /**
         * A scheduled runnable that could be waken and executed immediately when notification arrives.
         *
         * E.g
         * <p>
         * The reader reaches end of stream, it backs off to schedule next read in 2 seconds.
         * <br/>
         * if a new log segment is created, without this change, reader has to wait 2 seconds to read
         * entries in new log segment, which means delivery latency of entries in new log segment could
         * be up to 2 seconds. but with this change, the task would be executed immediately, which reader
         * would be waken up from backoff, which would reduce the delivery latency.
         * </p>
         */
        class InterruptibleScheduledRunnable implements AsyncNotification, Runnable {

            final Runnable task;
            final AtomicBoolean called = new AtomicBoolean(false);
            final long startNanos;

            InterruptibleScheduledRunnable(Runnable task) {
                this.task = task;
                this.startNanos = MathUtils.nowInNano();
            }

            @Override
            public void notifyOnError() {
                execute();
            }

            @Override
            public void notifyOnOperationComplete() {
                execute();
            }

            @Override
            public void run() {
                if (called.compareAndSet(false, true)) {
                    task.run();
                }
            }

            void execute() {
                if (called.compareAndSet(false, true)) {
                    submit(task);
                }
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
            }

            abstract void doComplete(boolean success);

            @Override
            public void notifyOnError() {
                complete(false);
            }

            @Override
            public void notifyOnOperationComplete() {
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
