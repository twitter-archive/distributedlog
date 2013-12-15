package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
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
import java.util.concurrent.atomic.AtomicInteger;


class BKLogPartitionReadHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    private static final int LAYOUT_VERSION = -1;
    private LedgerDataAccessor ledgerDataAccessor = null;
    private final LedgerHandleCache handleCache;

    private ReadAheadWorker readAheadWorker = null;
    private boolean readAheadError = false;

    // stats
    private final Counter readAheadWorkerWaits;

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
        ledgerDataAccessor = new LedgerDataAccessor(handleCache, statsLogger);

        // Stats
        StatsLogger readAheadStatsLogger = statsLogger.scope("readahead_worker");
        readAheadWorkerWaits = readAheadStatsLogger.getCounter("wait");
    }

    ResumableBKPerStreamLogReader getInputStream(long fromTxId, boolean fThrowOnEmpty)
        throws IOException {
        boolean logExists = false;
        try {
            if (null != zooKeeperClient.get().exists(ledgerPath, false)) {
                logExists = true;
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while checking " + ledgerPath, ie);
            throw new DLInterruptedException("Interrupted while checking " + ledgerPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error deleting" + ledgerPath + "entry in zookeeper", ke);
            throw new LogEmptyException("Log " + getFullyQualifiedName() + " is empty");
        }

        if (logExists) {
            for (LogSegmentLedgerMetadata l : getLedgerList()) {
                LOG.debug("Inspecting Ledger: {}", l);
                long lastTxId = l.getLastTxId();
                if (l.isInProgress()) {
                    try {
                        lastTxId = readLastTxIdInLedger(l);
                    } catch (DLInterruptedException die) {
                        throw die;
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
                    ledgerDataAccessor.removeLedger(l.getLedgerId());
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

    private void setWatcherOnLedgerRoot(Watcher watcher) throws IOException, KeeperException, InterruptedException {
        zooKeeperClient.get().getChildren(ledgerPath, watcher);
    }

    public boolean startReadAhead(LedgerReadPosition startPosition) {
        if (null == readAheadWorker) {
            readAheadWorker = new ReadAheadWorker(getFullyQualifiedName(),
                this,
                startPosition,
                ledgerDataAccessor,
                conf.getReadAheadBatchSize(),
                conf.getReadAheadMaxEntries(),
                conf.getReadAheadWaitTime());
            readAheadWorker.start();
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

    public void setReadAheadError() {
        readAheadError = true;
    }

    public void checkClosedOrInError() throws LogReadException {
        if (readAheadError) {
            throw new LogReadException("ReadAhead Thread encountered exceptions");
        }
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
    class ReadAheadWorker implements Runnable, Watcher {

        private final String fullyQualifiedName;
        private final BKLogPartitionReadHandler bkLedgerManager;
        private volatile boolean reInitializeMetadata = true;
        private LedgerReadPosition nextReadPosition;
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
        volatile boolean inProgressChanged = false;

        // ReadAhead Phases
        final Phase schedulePhase = new ScheduleReadAheadPhase();
        final Phase exceptionHandler = new ExceptionHandlePhase(schedulePhase);
        final Phase readAheadPhase =
                new CheckInProgressChangedPhase(new OpenLedgerPhase(new ReadEntriesPhase(schedulePhase)));

        public ReadAheadWorker(String fullyQualifiedName,
                               BKLogPartitionReadHandler ledgerManager,
                               LedgerReadPosition startPosition,
                               LedgerDataAccessor ledgerDataAccessor,
                               int readAheadBatchSize,
                               int readAheadMaxEntries,
                               int readAheadWaitTime) {
            this.bkLedgerManager = ledgerManager;
            this.fullyQualifiedName = fullyQualifiedName;
            this.nextReadPosition = startPosition;
            this.ledgerDataAccessor = ledgerDataAccessor;
            this.readAheadBatchSize = readAheadBatchSize;
            this.readAheadMaxEntries = readAheadMaxEntries;
            this.readAheadWaitTime = readAheadWaitTime;
        }

        public void start() {
            running = true;
            schedulePhase.process(BKException.Code.OK);
        }

        public void stop() {
            running = false;

        }

        void submit(Runnable runnable) {
            try {
                executorService.submit(runnable);
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError();
            }
        }

        private void schedule(Runnable runnable, long timeInMillis) {
            try {
                readAheadWorkerWaits.inc();
                executorService.schedule(runnable, timeInMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError();
            }
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
                            if (l.getLedgerId() == nextReadPosition.getLedgerId()) {
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
                                    new Object[] { fullyQualifiedName, currentMetadataIndex, currentMetadata });
                        }
                        next.process(BKException.Code.OK);
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
                    bkLedgerManager.getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, ReadAheadWorker.this, this);
                } else {
                    next.process(BKException.Code.OK);
                }
            }
        }

        final class OpenLedgerPhase extends Phase
                implements BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>,
                            AsyncCallback.ReadLastConfirmedCallback {

            OpenLedgerPhase(Phase next) {
                super(next);
            }

            @Override
            void process(int rc) {
                if (currentMetadata.isInProgress()) { // we don't want to fence the current journal
                    if (null == currentLH) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Opening ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                        }
                        bkLedgerManager.getHandleCache().asyncOpenLedger(currentMetadata.getLedgerId(), false, this);
                    } else {
                        long lastAddConfirmed;
                        try {
                            lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                        } catch (IOException ie) {
                            // Exception is thrown due to no ledger handle
                            exceptionHandler.process(BKException.Code.NoSuchLedgerExistsException);
                            return;
                        }
                        if (lastAddConfirmed < nextReadPosition.getEntryId()) {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Reading last add confirmed of {} for {}, as read poistion has moved over {} : {}",
                                        new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadPosition });
                            }
                            bkLedgerManager.getHandleCache().asyncReadLastConfirmed(currentLH, this, null);
                        } else {
                            next.process(BKException.Code.OK);
                        }
                    }
                } else {
                    if (null != currentLH) {
                        try {
                            if (inProgressChanged) {
                                bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                currentLH = null;
                            } else if (nextReadPosition.getEntryId() > bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH)) {
                                bkLedgerManager.getHandleCache().closeLedger(currentLH);
                                currentLH = null;
                                currentMetadata = null;
                                if (currentMetadataIndex + 1 < ledgerList.size()) {
                                    currentMetadata = ledgerList.get(++currentMetadataIndex);
                                    if (LOG.isTraceEnabled()) {
                                        LOG.trace("Moving read position to a new ledger {} for {}.",
                                                currentMetadata, fullyQualifiedName);
                                    }
                                    nextReadPosition.positionOnNewLedger(currentMetadata.getLedgerId());
                                }
                            }
                            next.process(BKException.Code.OK);
                        } catch (InterruptedException ie) {
                            exceptionHandler.process(BKException.Code.InterruptedException);
                        } catch (IOException ioe) {
                            exceptionHandler.process(BKException.Code.NoSuchLedgerExistsException);
                        } catch (BKException bke) {
                            exceptionHandler.process(bke.getCode());
                        }
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Opening ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                        }
                        bkLedgerManager.getHandleCache().asyncOpenLedger(currentMetadata.getLedgerId(), true, this);
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
                            exceptionHandler.process(rc);
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
                        exceptionHandler.process(BKException.Code.NoSuchLedgerExistsException);
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
                bkLedgerManager.getHandleCache().asyncReadEntries(currentLH, nextReadPosition.getEntryId(),
                        Math.min(lastAddConfirmed, (nextReadPosition.getEntryId() + readAheadBatchSize - 1)), this, null);
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
                            nextReadPosition.advance();
                            LedgerEntry e = seq.nextElement();
                            ledgerDataAccessor.set(new LedgerReadPosition(e.getLedgerId(), e.getEntryId()), e);
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
                if ((null != currentMetadata) && currentMetadata.isInProgress()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Reached End of inprogress ledger. Backoff reading ahead for {} ms.",
                                fullyQualifiedName, readAheadWaitTime);
                    }
                    // Backoff before resuming
                    schedule(ReadAheadWorker.this, readAheadWaitTime);
                } else if (cacheFull) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Cache for {} is full. Backoff reading ahead for {} ms.",
                            fullyQualifiedName, readAheadWaitTime);
                    }
                    ledgerDataAccessor.setReadAheadCallback(ReadAheadWorker.this, readAheadMaxEntries);
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
            } else {
                reInitializeMetadata = true;
                LOG.debug("Read ahead node changed");
            }
        }
    }

}
