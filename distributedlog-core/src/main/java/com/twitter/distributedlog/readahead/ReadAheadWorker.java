/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.readahead;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.AlreadyTruncatedTransactionException;
import com.twitter.distributedlog.AsyncNotification;
import com.twitter.distributedlog.BKLogHandler;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LedgerDescriptor;
import com.twitter.distributedlog.LedgerHandleCache;
import com.twitter.distributedlog.LedgerReadPosition;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.LogReadException;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.ReadAheadCache;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.callback.ReadAheadCallback;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForReader;
import com.twitter.distributedlog.injector.AsyncFailureInjector;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.stats.ReadAheadExceptionsLogger;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
public class ReadAheadWorker implements ReadAheadCallback, Runnable, Watcher, AsyncCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReadAheadWorker.class);

    private static final int BKC_ZK_EXCEPTION_THRESHOLD_IN_SECONDS = 30;
    private static final int BKC_UNEXPECTED_EXCEPTION_THRESHOLD = 3;

    // Stream information
    private final String fullyQualifiedName;
    private final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    private final ZKLogMetadataForReader logMetadata;
    private final BKLogHandler bkLedgerManager;
    private final boolean isHandleForReading;
    // Notification to notify readahead status
    protected final AsyncNotification notification;

    // resources
    private final ZooKeeperClient zkc;
    protected final OrderedScheduler scheduler;
    private final LedgerHandleCache handleCache;
    private final ReadAheadCache readAheadCache;

    // ReadAhead Status
    volatile boolean running = true;
    Promise<Void> stopPromise = null;
    private volatile boolean isCatchingUp = true;
    private volatile boolean logDeleted = false;
    private volatile boolean readAheadError = false;
    private volatile boolean readAheadInterrupted = false;
    private volatile boolean readingFromTruncated = false;

    // Exceptions Handling
    volatile boolean encounteredException = false;
    private final AtomicInteger bkcZkExceptions = new AtomicInteger(0);
    private final AtomicInteger bkcUnExpectedExceptions = new AtomicInteger(0);
    private final int noLedgerExceptionOnReadLACThreshold;
    private final AtomicInteger bkcNoLedgerExceptionsOnReadLAC = new AtomicInteger(0);

    // Read Ahead Positions
    private final LedgerReadPosition startReadPosition;
    protected LedgerReadPosition nextReadAheadPosition;

    //
    // LogSegments & Metadata Notification
    //

    // variables related to getting log segments from zookeeper
    volatile boolean zkNotificationDisabled = false;
    private final Watcher getLedgersWatcher;

    // variables related to zookeeper watcher notification to interrupt long poll waits
    final Object notificationLock = new Object();
    AsyncNotification metadataNotification = null;
    volatile long metadataNotificationTimeMillis = -1L;

    // variables related to log segments
    private volatile boolean reInitializeMetadata = true;
    volatile boolean inProgressChanged = false;
    private LogSegmentMetadata currentMetadata = null;
    private int currentMetadataIndex;
    protected LedgerDescriptor currentLH;
    private volatile List<LogSegmentMetadata> ledgerList;

    //
    // ReadAhead Phases
    //

    final Phase schedulePhase = new ScheduleReadAheadPhase();
    final Phase exceptionHandler = new ExceptionHandlePhase(schedulePhase);
    final Phase readAheadPhase =
            new StoppablePhase(
                    new CheckInProgressChangedPhase(
                            new OpenLedgerPhase(
                                    new ReadEntriesPhase(schedulePhase))));

    //
    // Stats, Tracing and Failure Injection
    //

    // failure injector
    private final AsyncFailureInjector failureInjector;
    // trace
    protected final long metadataLatencyWarnThresholdMillis;
    final ReadAheadTracker tracker;
    final Stopwatch resumeStopWatch;
    final Stopwatch lastLedgerCloseDetected = Stopwatch.createUnstarted();
    // Misc
    private final boolean readAheadSkipBrokenEntries;
    // Stats
    private final AlertStatsLogger alertStatsLogger;
    private final StatsLogger readAheadPerStreamStatsLogger;
    private final Counter readAheadWorkerWaits;
    private final Counter readAheadEntryPiggyBackHits;
    private final Counter readAheadEntryPiggyBackMisses;
    private final Counter readAheadReadLACAndEntryCounter;
    private final Counter readAheadCacheFullCounter;
    private final Counter readAheadSkippedBrokenEntries;
    private final Counter idleReaderWarn;
    private final OpStatsLogger readAheadReadEntriesStat;
    private final OpStatsLogger readAheadCacheResumeStat;
    private final OpStatsLogger readAheadLacLagStats;
    private final OpStatsLogger longPollInterruptionStat;
    private final OpStatsLogger metadataReinitializationStat;
    private final OpStatsLogger notificationExecutionStat;
    private final ReadAheadExceptionsLogger readAheadExceptionsLogger;

    public ReadAheadWorker(DistributedLogConfiguration conf,
                           DynamicDistributedLogConfiguration dynConf,
                           ZKLogMetadataForReader logMetadata,
                           BKLogHandler ledgerManager,
                           ZooKeeperClient zkc,
                           OrderedScheduler scheduler,
                           LedgerHandleCache handleCache,
                           LedgerReadPosition startPosition,
                           ReadAheadCache readAheadCache,
                           boolean isHandleForReading,
                           ReadAheadExceptionsLogger readAheadExceptionsLogger,
                           StatsLogger handlerStatsLogger,
                           StatsLogger readAheadPerStreamStatsLogger,
                           AlertStatsLogger alertStatsLogger,
                           AsyncFailureInjector failureInjector,
                           AsyncNotification notification) {
        // Log information
        this.fullyQualifiedName = logMetadata.getFullyQualifiedName();
        this.conf = conf;
        this.dynConf = dynConf;
        this.logMetadata = logMetadata;
        this.bkLedgerManager = ledgerManager;
        this.isHandleForReading = isHandleForReading;
        this.notification = notification;
        // Resources
        this.zkc = zkc;
        this.scheduler = scheduler;
        this.handleCache = handleCache;
        this.readAheadCache = readAheadCache;
        // Readahead status
        this.startReadPosition = new LedgerReadPosition(startPosition);
        this.nextReadAheadPosition = new LedgerReadPosition(startPosition);
        // LogSegments
        this.getLedgersWatcher = this.zkc.getWatcherManager()
                .registerChildWatcher(logMetadata.getLogSegmentsPath(), this);
        // Failure Detection
        this.failureInjector = failureInjector;
        // Tracing
        this.metadataLatencyWarnThresholdMillis = conf.getMetadataLatencyWarnThresholdMillis();
        this.noLedgerExceptionOnReadLACThreshold =
                conf.getReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis() / conf.getReadAheadWaitTime();
        this.tracker = new ReadAheadTracker(logMetadata.getLogName(), readAheadCache,
                ReadAheadPhase.SCHEDULE_READAHEAD, readAheadPerStreamStatsLogger);
        this.resumeStopWatch = Stopwatch.createUnstarted();
        // Misc
        this.readAheadSkipBrokenEntries = conf.getReadAheadSkipBrokenEntries();
        // Stats
        this.alertStatsLogger = alertStatsLogger;
        this.readAheadPerStreamStatsLogger = readAheadPerStreamStatsLogger;
        StatsLogger readAheadStatsLogger = handlerStatsLogger.scope("readahead_worker");
        readAheadWorkerWaits = readAheadStatsLogger.getCounter("wait");
        readAheadEntryPiggyBackHits = readAheadStatsLogger.getCounter("entry_piggy_back_hits");
        readAheadEntryPiggyBackMisses = readAheadStatsLogger.getCounter("entry_piggy_back_misses");
        readAheadReadEntriesStat = readAheadStatsLogger.getOpStatsLogger("read_entries");
        readAheadReadLACAndEntryCounter = readAheadStatsLogger.getCounter("read_lac_and_entry_counter");
        readAheadCacheFullCounter = readAheadStatsLogger.getCounter("cache_full");
        readAheadSkippedBrokenEntries = readAheadStatsLogger.getCounter("skipped_broken_entries");
        readAheadCacheResumeStat = readAheadStatsLogger.getOpStatsLogger("resume");
        readAheadLacLagStats = readAheadStatsLogger.getOpStatsLogger("read_lac_lag");
        longPollInterruptionStat = readAheadStatsLogger.getOpStatsLogger("long_poll_interruption");
        notificationExecutionStat = readAheadStatsLogger.getOpStatsLogger("notification_execution");
        metadataReinitializationStat = readAheadStatsLogger.getOpStatsLogger("metadata_reinitialization");
        idleReaderWarn = readAheadStatsLogger.getCounter("idle_reader_warn");
        this.readAheadExceptionsLogger = readAheadExceptionsLogger;
    }

    @VisibleForTesting
    public LedgerReadPosition getNextReadAheadPosition() {
        return nextReadAheadPosition;
    }

    public LedgerDescriptor getCurrentLedgerDescriptor() {
        return currentLH;
    }

    //
    // ReadAhead Status
    //

    void setReadAheadError(ReadAheadTracker tracker) {
        LOG.error("Read Ahead for {} is set to error.", logMetadata.getFullyQualifiedName());
        readAheadError = true;
        tracker.enterPhase(ReadAheadPhase.ERROR);
        if (null != notification) {
            notification.notifyOnError();
        }
        if (null != stopPromise) {
            FutureUtils.setValue(stopPromise, null);
        }
    }

    void setReadAheadInterrupted(ReadAheadTracker tracker) {
        readAheadInterrupted = true;
        tracker.enterPhase(ReadAheadPhase.INTERRUPTED);
        if (null != notification) {
            notification.notifyOnError();
        }
        if (null != stopPromise) {
            FutureUtils.setValue(stopPromise, null);
        }
    }

    void setReadingFromTruncated(ReadAheadTracker tracker) {
        readingFromTruncated = true;
        tracker.enterPhase(ReadAheadPhase.TRUNCATED);
        if (null != notification) {
            notification.notifyOnError();
        }
        if (null != stopPromise) {
            FutureUtils.setValue(stopPromise, null);
        }
    }

    private void setReadAheadStopped() {
        tracker.enterPhase(ReadAheadPhase.STOPPED);
        if (null != stopPromise) {
            FutureUtils.setValue(stopPromise, null);
        }
        LOG.info("Stopped ReadAheadWorker for {}", fullyQualifiedName);
    }

    public void checkClosedOrInError()
            throws LogNotFoundException, LogReadException, DLInterruptedException,
            AlreadyTruncatedTransactionException {
        if (logDeleted) {
            throw new LogNotFoundException(logMetadata.getFullyQualifiedName() + " is already deleted.");
        } else if (readingFromTruncated) {
            throw new AlreadyTruncatedTransactionException(
                String.format("%s: Trying to position read ahead a segment that is marked truncated",
                    logMetadata.getFullyQualifiedName()));
        } else if (readAheadInterrupted) {
            throw new DLInterruptedException(String.format("%s: ReadAhead Thread was interrupted",
                logMetadata.getFullyQualifiedName()));
        } else if (readAheadError) {
            throw new LogReadException(String.format("%s: ReadAhead Thread encountered exceptions",
                logMetadata.getFullyQualifiedName()));
        }
    }

    public boolean isCaughtUp() {
        return !isCatchingUp;
    }

    public void start() {
        LOG.debug("Starting ReadAhead Worker for {}", fullyQualifiedName);
        running = true;
        schedulePhase.process(BKException.Code.OK);
    }

    @Override
    public Future<Void> asyncClose() {
        LOG.info("Stopping Readahead worker for {}", fullyQualifiedName);
        running = false;

        this.zkc.getWatcherManager()
                .unregisterChildWatcher(this.logMetadata.getLogSegmentsPath(), this);

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
        if (null == stopPromise) {
            return Future.Void();
        }
        return FutureUtils.ignore(FutureUtils.within(
                stopPromise,
                2 * conf.getReadAheadWaitTime(),
                TimeUnit.MILLISECONDS,
                new TimeoutException("Timeout on waiting for ReadAhead worker to stop " + fullyQualifiedName),
                scheduler,
                fullyQualifiedName));
    }

    @Override
    public String toString() {
        return "Running:" + running +
            ", NextReadAheadPosition:" + nextReadAheadPosition +
            ", BKZKExceptions:" + bkcZkExceptions.get() +
            ", BKUnexpectedExceptions:" + bkcUnExpectedExceptions.get() +
            ", EncounteredException:" + encounteredException +
            ", readAheadError:" + readAheadError +
            ", readAheadInterrupted" + readAheadInterrupted +
            ", CurrentMetadata:" + ((null != currentMetadata) ? currentMetadata : "NONE") +
            ", FailureInjector:" + failureInjector;
    }

    @Override
    public void resumeReadAhead() {
        try {
            long cacheResumeLatency = resumeStopWatch.stop().elapsed(TimeUnit.MICROSECONDS);
            readAheadCacheResumeStat.registerSuccessfulEvent(cacheResumeLatency);
        } catch (IllegalStateException ise) {
            LOG.error("Encountered illegal state when stopping resume stop watch for {} : ",
                    logMetadata.getFullyQualifiedName(), ise);
        }
        submit(this);
    }

    Runnable addRTEHandler(final Runnable runnable) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (RuntimeException rte) {
                    LOG.error("ReadAhead on stream {} encountered runtime exception",
                            logMetadata.getFullyQualifiedName(), rte);
                    setReadAheadError(tracker);
                    throw rte;
                }
            }
        };
    }

    <T> Function1<T, BoxedUnit> submit(final Function1<T, BoxedUnit> function) {
        return new AbstractFunction1<T, BoxedUnit>() {
            @Override
            public BoxedUnit apply(final T input) {
                submit(new Runnable() {
                    @Override
                    public void run() {
                        function.apply(input);
                    }
                });
                return BoxedUnit.UNIT;
            }
        };
    }

    void submit(Runnable runnable) {
        if (failureInjector.shouldInjectStops()) {
            LOG.warn("Error injected: read ahead for stream {} is going to stall.",
                    logMetadata.getFullyQualifiedName());
            return;
        }

        if (failureInjector.shouldInjectDelays()) {
            int delayMs = failureInjector.getInjectedDelayMs();
            schedule(runnable, delayMs);
            return;
        }

        try {
            scheduler.submit(addRTEHandler(runnable));
        } catch (RejectedExecutionException ree) {
            setReadAheadError(tracker);
        }
    }

    private void schedule(Runnable runnable, long timeInMillis) {
        try {
            InterruptibleScheduledRunnable task = new InterruptibleScheduledRunnable(runnable);
            boolean executeImmediately = setMetadataNotification(task);
            if (executeImmediately) {
                scheduler.submit(addRTEHandler(task));
                return;
            }
            scheduler.schedule(addRTEHandler(task), timeInMillis, TimeUnit.MILLISECONDS);
            readAheadWorkerWaits.inc();
        } catch (RejectedExecutionException ree) {
            setReadAheadError(tracker);
        }
    }

    private void handleException(ReadAheadPhase phase, int returnCode) {
        readAheadExceptionsLogger.getBKExceptionStatsLogger(phase.name()).getExceptionCounter(returnCode).inc();
        exceptionHandler.process(returnCode);
    }

    private boolean closeCurrentLedgerHandle() {
        if (currentLH == null) {
            return true;
        }
        boolean retVal = false;
        LedgerDescriptor ld = currentLH;
        try {
            handleCache.closeLedger(ld);
            currentLH = null;
            retVal = true;
        } catch (BKException bke) {
            LOG.debug("BK Exception during closing {} : ", ld, bke);
            handleException(ReadAheadPhase.CLOSE_LEDGER, bke.getCode());
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
                setReadAheadStopped();
                return;
            }
            tracker.enterPhase(ReadAheadPhase.SCHEDULE_READAHEAD);

            boolean injectErrors = failureInjector.shouldInjectErrors();
            if (encounteredException || injectErrors) {
                int zkErrorThreshold = BKC_ZK_EXCEPTION_THRESHOLD_IN_SECONDS * 1000 * 4 / conf.getReadAheadWaitTime();

                if ((bkcZkExceptions.get() > zkErrorThreshold) || injectErrors) {
                    LOG.error("{} : BookKeeper Client used by the ReadAhead Thread has encountered {} zookeeper exceptions : simulate = {}",
                              new Object[] { fullyQualifiedName, bkcZkExceptions.get(), injectErrors });
                    running = false;
                    setReadAheadError(tracker);
                } else if (bkcUnExpectedExceptions.get() > BKC_UNEXPECTED_EXCEPTION_THRESHOLD) {
                    LOG.error("{} : ReadAhead Thread has encountered {} unexpected BK exceptions.",
                              fullyQualifiedName, bkcUnExpectedExceptions.get());
                    running = false;
                    setReadAheadError(tracker);
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
            tracker.enterPhase(ReadAheadPhase.EXCEPTION_HANDLING);

            if (BKException.Code.InterruptedException == rc) {
                LOG.trace("ReadAhead Worker for {} is interrupted.", fullyQualifiedName);
                running = false;
                setReadAheadInterrupted(tracker);
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
                LOG.info("ReadAhead Worker for {} encountered exception : {}",
                        fullyQualifiedName, BKException.create(rc));
            }
            // schedule next read ahead
            next.process(BKException.Code.OK);
        }
    }

    /**
     * A phase that could be stopped by a stopPromise
     */
    final class StoppablePhase extends Phase {

        StoppablePhase(Phase next) {
            super(next);
        }

        @Override
        void process(int rc) {
            if (!running) {
                setReadAheadStopped();
                return;
            }

            if (null == stopPromise) {
                stopPromise = new Promise<Void>();
            }

            // proceed the readahead request
            next.process(BKException.Code.OK);
        }
    }

    /**
     * Phase on checking in progress changed.
     */
    final class CheckInProgressChangedPhase extends Phase
        implements BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentMetadata>> {

        CheckInProgressChangedPhase(Phase next) {
            super(next);
        }

        @Override
        public void operationComplete(final int rc, final List<LogSegmentMetadata> result) {
            // submit callback execution to dlg executor to avoid deadlock.
            submit(new Runnable() {
                @Override
                public void run() {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        if (KeeperException.Code.NONODE.intValue() == rc) {
                            LOG.info("Log {} has been deleted. Set ReadAhead to error to stop reading.",
                                    logMetadata.getFullyQualifiedName());
                            logDeleted = true;
                            setReadAheadError(tracker);
                            return;
                        }
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
                    boolean isInitialPositioning = nextReadAheadPosition.definitelyLessThanOrEqualTo(startReadPosition);
                    for (int i = 0; i < ledgerList.size(); i++) {
                        LogSegmentMetadata l = ledgerList.get(i);
                        // By default we should skip truncated segments during initial positioning
                        if (l.isTruncated() &&
                            isInitialPositioning &&
                            !conf.getIgnoreTruncationStatus()) {
                            continue;
                        }

                        DLSN nextReadDLSN = new DLSN(nextReadAheadPosition.getLogSegmentSequenceNumber(),
                                nextReadAheadPosition.getEntryId(), -1);

                        // next read position still inside a log segment
                        final boolean hasDataToRead = (l.getLastDLSN().compareTo(nextReadDLSN) >= 0);

                        // either there is data to read in current log segment or we are moving over a log segment that is
                        // still inprogress or was inprogress, we have check (or maybe close) this log segment.
                        final boolean checkOrCloseLedger = hasDataToRead ||
                            // next read position move over a log segment, if l is still inprogress or it was inprogress
                            ((l.isInProgress() || (null != currentMetadata && currentMetadata.isInProgress())) &&
                                    l.getLogSegmentSequenceNumber() == nextReadAheadPosition.getLogSegmentSequenceNumber());

                        // If we are positioning on a partially truncated log segment then the truncation point should
                        // be before the nextReadPosition
                        if (l.isPartiallyTruncated() &&
                            !isInitialPositioning &&
                            (l.getMinActiveDLSN().compareTo(nextReadDLSN) > 0)) {
                            if (conf.getAlertWhenPositioningOnTruncated()) {
                                alertStatsLogger.raise("Trying to position reader on {} when {} is marked partially truncated",
                                    nextReadAheadPosition, l);
                            }

                            if (!conf.getIgnoreTruncationStatus()) {
                                LOG.error("{}: Trying to position reader on {} when {} is marked partially truncated",
                                    new Object[]{ logMetadata.getFullyQualifiedName(), nextReadAheadPosition, l});
                                setReadingFromTruncated(tracker);
                                return;
                            }
                        }


                        if (LOG.isTraceEnabled()) {
                            LOG.trace("CheckLogSegment : newMetadata = {}, currentMetadata = {}, nextReadAheadPosition = {}",
                                      new Object[] { l, currentMetadata, nextReadAheadPosition});
                        }

                        if (checkOrCloseLedger) {
                            long startBKEntry = 0;
                            if (l.isPartiallyTruncated() && !conf.getIgnoreTruncationStatus()) {
                                startBKEntry = l.getMinActiveDLSN().getEntryId();
                                readAheadCache.setMinActiveDLSN(l.getMinActiveDLSN());
                            }

                            if(l.getLogSegmentSequenceNumber() == nextReadAheadPosition.getLogSegmentSequenceNumber()) {
                                startBKEntry = Math.max(startBKEntry, nextReadAheadPosition.getEntryId());
                                if (currentMetadata != null) {
                                    inProgressChanged = currentMetadata.isInProgress() && !l.isInProgress();
                                }
                            } else {
                                // We are positioning on a new ledger => reset the current ledger handle
                                LOG.trace("Positioning {} on a new ledger {}", fullyQualifiedName, l);

                                if (!closeCurrentLedgerHandle()) {
                                    return;
                                }
                            }

                            nextReadAheadPosition = new LedgerReadPosition(l.getLedgerId(), l.getLogSegmentSequenceNumber(), startBKEntry);
                            if (conf.getTraceReadAheadMetadataChanges()) {
                                LOG.info("Moved read position to {} for stream {} at {}.",
                                         new Object[] {nextReadAheadPosition, logMetadata.getFullyQualifiedName(), System.currentTimeMillis() });
                            }

                            if (l.isTruncated()) {
                                if (conf.getAlertWhenPositioningOnTruncated()) {
                                    alertStatsLogger.raise("Trying to position reader on {} when {} is marked truncated",
                                        nextReadAheadPosition, l);
                                }

                                if (!conf.getIgnoreTruncationStatus()) {
                                    LOG.error("{}: Trying to position reader on {} when {} is marked truncated",
                                        new Object[]{ logMetadata.getFullyQualifiedName(), nextReadAheadPosition, l});
                                    setReadingFromTruncated(tracker);
                                    return;
                                }
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
                        if (isCatchingUp) {
                            isCatchingUp = false;
                            readAheadCache.setSuppressDeliveryLatency(false);
                            if (isHandleForReading) {
                                LOG.info("{} caught up at {}: position = {} and no log segment to position on at this point.",
                                         new Object[] { fullyQualifiedName, System.currentTimeMillis(), nextReadAheadPosition });
                            }
                        }
                        schedule(ReadAheadWorker.this, conf.getReadAheadWaitTimeOnEndOfStream());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("No log segment to position on for {}. Backing off for {} millseconds",
                                    fullyQualifiedName, conf.getReadAheadWaitTimeOnEndOfStream());
                        }
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
                    bkLedgerManager.reportGetSegmentStats(true);
                }
            });
        }

        @Override
        void process(int rc) {
            if (!running) {
                setReadAheadStopped();
                return;
            }

            tracker.enterPhase(ReadAheadPhase.GET_LEDGERS);

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
                    if (elapsedMillisSinceMetadataChanged >= metadataLatencyWarnThresholdMillis) {
                        LOG.warn("{} reinitialize metadata at {}, which is {} millis after receiving notification at {}.",
                                 new Object[] { logMetadata.getFullyQualifiedName(), metadataReinitializeTimeMillis,
                                                elapsedMillisSinceMetadataChanged, metadataNotificationTimeMillis});
                    }
                    metadataReinitializationStat.registerSuccessfulEvent(
                            TimeUnit.MILLISECONDS.toMicros(elapsedMillisSinceMetadataChanged));
                    metadataNotificationTimeMillis = -1L;
                }
                bkLedgerManager.asyncGetLedgerList(LogSegmentMetadata.COMPARATOR, getLedgersWatcher, this);
            } else {
                next.process(BKException.Code.OK);
            }
        }
    }

    final class OpenLedgerPhase extends Phase
            implements BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>,
                        AsyncCallback.ReadLastConfirmedAndEntryCallback {

        OpenLedgerPhase(Phase next) {
            super(next);
        }

        private void issueReadLastConfirmedAndEntry(final boolean parallel,
                                                    final long lastAddConfirmed) {
            final String ctx = String.format("ReadLastConfirmedAndEntry(%s, %d)", parallel? "Parallel":"Sequential", lastAddConfirmed);
            final ReadLastConfirmedAndEntryCallbackWithNotification callback =
                new ReadLastConfirmedAndEntryCallbackWithNotification(lastAddConfirmed, this, ctx);
            boolean callbackImmediately = setMetadataNotification(callback);
            handleCache.asyncReadLastConfirmedAndEntry(
                    currentLH,
                    nextReadAheadPosition.getEntryId(),
                    conf.getReadLACLongPollTimeout(),
                    parallel
            ).addEventListener(new FutureEventListener<Pair<Long, LedgerEntry>>() {
                @Override
                public void onSuccess(Pair<Long, LedgerEntry> lacAndEntry) {
                    callback.readLastConfirmedAndEntryComplete(
                            BKException.Code.OK,
                            lacAndEntry.getLeft(),
                            lacAndEntry.getRight(),
                            ctx);
                }

                @Override
                public void onFailure(Throwable cause) {
                    callback.readLastConfirmedAndEntryComplete(
                            FutureUtils.bkResultCode(cause),
                            lastAddConfirmed,
                            null,
                            ctx);
                }
            });
            callback.callbackImmediately(callbackImmediately);
            readAheadReadLACAndEntryCounter.inc();
        }

        @Override
        void process(int rc) {
            if (!running) {
                setReadAheadStopped();
                return;
            }

            tracker.enterPhase(ReadAheadPhase.OPEN_LEDGER);

            if (currentMetadata.isInProgress()) { // we don't want to fence the current journal
                if (null == currentLH) {
                    if (conf.getTraceReadAheadMetadataChanges()) {
                        LOG.info("Opening inprogress ledger of {} for {} at {}.",
                                 new Object[] { currentMetadata, fullyQualifiedName, System.currentTimeMillis() });
                    }
                    handleCache.asyncOpenLedger(currentMetadata, false)
                            .addEventListener(new FutureEventListener<LedgerDescriptor>() {
                                @Override
                                public void onSuccess(LedgerDescriptor ld) {
                                    operationComplete(BKException.Code.OK, ld);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    operationComplete(FutureUtils.bkResultCode(cause), null);
                                }
                            });
                } else {
                    final long lastAddConfirmed;
                    boolean isClosed;
                    try {
                        isClosed = handleCache.isLedgerHandleClosed(currentLH);
                        lastAddConfirmed = handleCache.getLastAddConfirmed(currentLH);
                    } catch (BKException ie) {
                        // Exception is thrown due to no ledger handle
                        handleException(ReadAheadPhase.OPEN_LEDGER, ie.getCode());
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
                                    idleReaderWarn.inc();
                                    LOG.info("{} Ledger {} for inprogress segment {} closed for idle reader warn threshold",
                                        new Object[] { fullyQualifiedName, currentMetadata, currentLH });
                                    reInitializeMetadata = true;
                                }
                            } else {
                                lastLedgerCloseDetected.reset().start();
                                if (conf.getTraceReadAheadMetadataChanges()) {
                                    LOG.info("{} Ledger {} for inprogress segment {} closed",
                                        new Object[] { fullyQualifiedName, currentMetadata, currentLH });
                                }
                            }
                        } else {
                            lastLedgerCloseDetected.reset();
                        }

                        tracker.enterPhase(ReadAheadPhase.READ_LAST_CONFIRMED);

                        // the readahead is caught up if current ledger is in progress and read position moves over last add confirmed
                        if (isCatchingUp) {
                            isCatchingUp = false;
                            readAheadCache.setSuppressDeliveryLatency(false);
                            if (isHandleForReading) {
                                LOG.info("{} caught up at {}: lac = {}, position = {}.",
                                         new Object[] { fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition });
                            }
                        }

                        LOG.trace("Reading last add confirmed of {} for {}, as read poistion has moved over {} : {}",
                                new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadAheadPosition });

                        if (nextReadAheadPosition.getEntryId() == 0 && conf.getTraceReadAheadMetadataChanges()) {
                            // we are waiting for first entry to arrive
                            LOG.info("Reading last add confirmed for {} at {}: lac = {}, position = {}.",
                                     new Object[] { fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition});
                        } else {
                            LOG.trace("Reading last add confirmed for {} at {}: lac = {}, position = {}.",
                                      new Object[] { fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition});
                        }
                        issueReadLastConfirmedAndEntry(false, lastAddConfirmed);
                    } else {
                        next.process(BKException.Code.OK);
                    }
                }
            } else {
                lastLedgerCloseDetected.reset();
                if (null != currentLH) {
                    try {
                        if (inProgressChanged) {
                            LOG.trace("Closing completed ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                            if (!closeCurrentLedgerHandle()) {
                                return;
                            }
                            inProgressChanged = false;
                        } else {
                            long lastAddConfirmed = handleCache.getLastAddConfirmed(currentLH);
                            if (nextReadAheadPosition.getEntryId() > lastAddConfirmed) {
                                // Its possible that the last entryId does not account for the control
                                // log record, but the lastAddConfirmed should never be short of the
                                // last entry id; else we maybe missing an entry
                                boolean gapDetected = false;
                                if (lastAddConfirmed < currentMetadata.getLastEntryId()) {
                                    alertStatsLogger.raise("Unexpected last entry id during read ahead; {} , {}",
                                        currentMetadata, lastAddConfirmed);
                                    gapDetected = true;
                                }

                                if (conf.getPositionGapDetectionEnabled() && gapDetected) {
                                    setReadAheadError(tracker);
                                } else {
                                    // This disconnect will only surface during repositioning and
                                    // will not silently miss records; therefore its safe to not halt
                                    // reading, but we should print a warning for easy diagnosis
                                    if (conf.getTraceReadAheadMetadataChanges() && lastAddConfirmed > (currentMetadata.getLastEntryId() + 1)) {
                                        LOG.warn("Potential Metadata Corruption {} for stream {}, lastAddConfirmed {}",
                                                new Object[] {currentMetadata, logMetadata.getFullyQualifiedName(), lastAddConfirmed});
                                    }

                                    LOG.trace("Past the last Add Confirmed {} in ledger {} for {}",
                                              new Object[] { lastAddConfirmed, currentMetadata, fullyQualifiedName });
                                    if (!closeCurrentLedgerHandle()) {
                                        return;
                                    }
                                    LogSegmentMetadata oldMetadata = currentMetadata;
                                    currentMetadata = null;
                                    if (currentMetadataIndex + 1 < ledgerList.size()) {
                                        currentMetadata = ledgerList.get(++currentMetadataIndex);
                                        if (currentMetadata.getLogSegmentSequenceNumber() != (oldMetadata.getLogSegmentSequenceNumber() + 1)) {
                                            // We should never get here as we should have exited the loop if
                                            // pendingRequests were empty
                                            alertStatsLogger.raise("Unexpected condition during read ahead; {} , {}",
                                                currentMetadata, oldMetadata);
                                            setReadAheadError(tracker);
                                        } else {
                                            if (currentMetadata.isTruncated()) {
                                                if (conf.getAlertWhenPositioningOnTruncated()) {
                                                    alertStatsLogger.raise("Trying to position reader on the log segment that is marked truncated : {}",
                                                        currentMetadata);
                                                }

                                                if (!conf.getIgnoreTruncationStatus()) {
                                                    LOG.error("{}: Trying to position reader on the log segment that is marked truncated : {}",
                                                            logMetadata.getFullyQualifiedName(), currentMetadata);
                                                    setReadingFromTruncated(tracker);
                                                }
                                            } else {
                                                if (LOG.isTraceEnabled()) {
                                                    LOG.trace("Moving read position to a new ledger {} for {}.",
                                                        currentMetadata, fullyQualifiedName);
                                                }
                                                nextReadAheadPosition.positionOnNewLogSegment(currentMetadata.getLedgerId(), currentMetadata.getLogSegmentSequenceNumber());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (!readAheadError) {
                            next.process(BKException.Code.OK);
                        }
                    } catch (BKException bke) {
                        LOG.debug("Exception while repositioning", bke);
                        handleException(ReadAheadPhase.CLOSE_LEDGER, bke.getCode());
                    }
                } else {
                    LOG.trace("Opening completed ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                    handleCache.asyncOpenLedger(currentMetadata, true)
                            .addEventListener(new FutureEventListener<LedgerDescriptor>() {
                                @Override
                                public void onSuccess(LedgerDescriptor ld) {
                                    operationComplete(BKException.Code.OK, ld);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    operationComplete(FutureUtils.bkResultCode(cause), null);
                                }
                            });
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
                        LOG.info("Opened ledger of {} for {} at {}.",
                                new Object[]{currentMetadata, fullyQualifiedName, System.currentTimeMillis()});
                    }
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
                                new Object[]{fullyQualifiedName, currentLH, conf.getReadAheadWaitTime()});
                    }
                    // Backoff before resuming
                    schedule(ReadAheadWorker.this, conf.getReadAheadWaitTime());
                    return;
                }
            } else if (BKException.Code.OK != rc) {
                handleException(ReadAheadPhase.READ_LAST_CONFIRMED, rc);
                return;
            }
        }

        public void readLastConfirmedAndEntryComplete(final int rc, final long lastConfirmed, final LedgerEntry entry,
                                                      final Object ctx) {
            // submit callback execution to dlg executor to avoid deadlock.
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
                                                     handleCache.getLastAddConfirmed(currentLH) });
                        } catch (BKException exc) {
                            // Ignore
                        }
                    }

                    if ((null != entry)
                        && (nextReadAheadPosition.getEntryId() == entry.getEntryId())
                        && (nextReadAheadPosition.getLedgerId() == entry.getLedgerId())) {
                        if (lastConfirmed <= 4 && conf.getTraceReadAheadMetadataChanges()) {
                            LOG.info("Hit readLastConfirmedAndEntry for {} at {} : entry = {}, lac = {}, position = {}.",
                                    new Object[] { fullyQualifiedName, System.currentTimeMillis(),
                                            entry.getEntryId(), lastConfirmed, nextReadAheadPosition });
                        }

                        if (!isCatchingUp) {
                            long lac = lastConfirmed - nextReadAheadPosition.getEntryId();
                            if (lac > 0) {
                                readAheadLacLagStats.registerSuccessfulEvent(lac);
                            }
                        }

                        nextReadAheadPosition.advance();

                        readAheadCache.set(new LedgerReadPosition(entry.getLedgerId(), currentLH.getLogSegmentSequenceNo(), entry.getEntryId()),
                                               entry, null != ctx ? ctx.toString() : "",
                                               currentMetadata.getEnvelopeEntries(), currentMetadata.getStartSequenceId());

                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Reading the value received {} for {} : entryId {}",
                                new Object[] { currentMetadata, fullyQualifiedName, entry.getEntryId() });
                        }
                        readAheadEntryPiggyBackHits.inc();
                    } else {
                        if (lastConfirmed > nextReadAheadPosition.getEntryId()) {
                            LOG.info("{} : entry {} isn't piggybacked but last add confirmed already moves to {}.",
                                     new Object[] { logMetadata.getFullyQualifiedName(), nextReadAheadPosition.getEntryId(), lastConfirmed });
                        }
                        readAheadEntryPiggyBackMisses.inc();
                    }
                    next.process(rc);
                }
            });
        }
    }

    final class ReadEntriesPhase extends Phase implements Runnable {

        boolean cacheFull = false;
        long lastAddConfirmed = -1;

        ReadEntriesPhase(Phase next) {
            super(next);
        }

        @Override
        void process(int rc) {
            if (!running) {
                setReadAheadStopped();
                return;
            }

            tracker.enterPhase(ReadAheadPhase.READ_ENTRIES);

            cacheFull = false;
            lastAddConfirmed = -1;
            if (null != currentLH) {
                try {
                    lastAddConfirmed = handleCache.getLastAddConfirmed(currentLH);
                } catch (BKException bke) {
                    handleException(ReadAheadPhase.READ_LAST_CONFIRMED, bke.getCode());
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
                    LOG.trace("Nothing to read for {} of {} : lastAddConfirmed = {}, nextReadAheadPosition = {}",
                            new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadAheadPosition});
                }
                complete();
                return;
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Reading entry {} for {} of {}.",
                        new Object[] {nextReadAheadPosition, currentMetadata, fullyQualifiedName });
            }
            int readAheadBatchSize = dynConf.getReadAheadBatchSize();
            final long startEntryId = nextReadAheadPosition.getEntryId();
            final long endEntryId = Math.min(lastAddConfirmed, (nextReadAheadPosition.getEntryId() + readAheadBatchSize - 1));

            if (endEntryId <= readAheadBatchSize && conf.getTraceReadAheadMetadataChanges()) {
                // trace first read batch
                LOG.info("Reading entries ({} - {}) for {} at {} : lac = {}, nextReadAheadPosition = {}.",
                         new Object[] { startEntryId, endEntryId, fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition});
            }

            final String readCtx = String.format("ReadEntries(%d-%d)", startEntryId, endEntryId);
            handleCache.asyncReadEntries(currentLH, startEntryId, endEntryId)
                    .addEventListener(new FutureEventListener<Enumeration<LedgerEntry>>() {

                        @Override
                        public void onSuccess(Enumeration<LedgerEntry> entries) {
                            int rc = BKException.Code.OK;

                            // If the range includes an entry id that is a multiple of 10, simulate corruption.
                            if (failureInjector.shouldInjectCorruption() && rangeContainsSimulatedBrokenEntry(startEntryId, endEntryId)) {
                                rc = BKException.Code.DigestMatchException;
                            }
                            readComplete(rc, null, entries, readCtx, startEntryId, endEntryId);
                        }

                        @Override
                        public void onFailure(Throwable cause) {
                            readComplete(FutureUtils.bkResultCode(cause), null, null, readCtx, startEntryId, endEntryId);
                        }
                    });
        }

        private boolean rangeContainsSimulatedBrokenEntry(long start, long end) {
            for (long i = start; i <= end; i++) {
                if (i % 10 == 0) {
                    return true;
                }
            }
            return false;
        }

        public void readComplete(final int rc, final LedgerHandle lh,
                                 final Enumeration<LedgerEntry> seq, final Object ctx,
                                 final long startEntryId, final long endEntryId) {
            // submit callback execution to dlg executor to avoid deadlock.
            submit(new Runnable() {
                @Override
                public void run() {
                    long numEntries = endEntryId - startEntryId + 1;

                    // If readAheadSkipBrokenEntries is enabled and we hit a corrupt entry, log and
                    // stat the issue and move forward.
                    if (BKException.Code.DigestMatchException == rc && readAheadSkipBrokenEntries) {
                        readAheadReadEntriesStat.registerFailedEvent(0);
                        LOG.error("BK DigestMatchException while reading entries {}-{} in stream {}, entry {} discarded",
                                new Object[] { startEntryId, endEntryId, fullyQualifiedName, startEntryId });
                        bkcZkExceptions.set(0);
                        bkcUnExpectedExceptions.set(0);
                        readAheadSkippedBrokenEntries.inc();
                        nextReadAheadPosition.advance();
                    } else if (BKException.Code.OK != rc) {
                        readAheadReadEntriesStat.registerFailedEvent(0);
                        LOG.debug("BK Exception {} while reading entry", rc);
                        handleException(ReadAheadPhase.READ_ENTRIES, rc);
                        return;
                    } else {
                        int numReads = 0;
                        while (seq.hasMoreElements()) {
                            bkcZkExceptions.set(0);
                            bkcUnExpectedExceptions.set(0);
                            nextReadAheadPosition.advance();
                            LedgerEntry e = seq.nextElement();
                            LedgerReadPosition readPosition = new LedgerReadPosition(e.getLedgerId(), currentMetadata.getLogSegmentSequenceNumber(), e.getEntryId());
                            readAheadCache.set(readPosition, e, null != ctx ? ctx.toString() : "",
                                    currentMetadata.getEnvelopeEntries(), currentMetadata.getStartSequenceId());
                            ++numReads;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Read entry {} of {}.", readPosition, fullyQualifiedName);
                            }
                        }
                        readAheadReadEntriesStat.registerSuccessfulEvent(numReads);
                    }
                    if (readAheadCache.isCacheFull()) {
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
                LOG.trace("Cache for {} is full. Backoff reading until notified", fullyQualifiedName);
                readAheadCacheFullCounter.inc();
                resumeStopWatch.reset().start();
                stopPromise = null;
                readAheadCache.setReadAheadCallback(ReadAheadWorker.this);
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
        if (!running) {
            setReadAheadStopped();
            return;
        }
        readAheadPhase.process(BKException.Code.OK);
    }

    @Override
    public void process(WatchedEvent event) {
        if (zkNotificationDisabled) {
            return;
        }

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
        } else if (event.getType() == Event.EventType.NodeDeleted) {
            logDeleted = true;
            setReadAheadError(tracker);
        }
    }

    @VisibleForTesting
    public void disableZKNotification() {
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
    public AsyncNotification getMetadataNotification() {
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
            longPollInterruptionStat.registerFailedEvent(MathUtils.elapsedMicroSec(startNanos));
            execute();
        }

        @Override
        public void notifyOnOperationComplete() {
            longPollInterruptionStat.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startNanos));
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
