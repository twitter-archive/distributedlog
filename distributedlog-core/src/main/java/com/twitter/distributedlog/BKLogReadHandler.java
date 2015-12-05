package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.google.common.base.Ticker;
import com.twitter.distributedlog.callback.ReadAheadCallback;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForReader;
import com.twitter.distributedlog.logsegment.LogSegmentFilter;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.lock.DistributedReentrantLock;
import com.twitter.distributedlog.util.Utils;
import org.apache.commons.lang3.tuple.Pair;
import scala.Function0;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.LockCancelledException;
import com.twitter.distributedlog.stats.ReadAheadExceptionsLogger;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Throw;
import com.twitter.util.Try;
import com.twitter.util.Return;

/**
 * Log Handler for Readers.
 * <h3>Metrics</h3>
 *
 * <h4>ReadAhead Worker</h4>
 * Most of readahead stats are exposed under scope `readahead_worker`. Only readahead exceptions are exposed
 * in parent scope via <code>readAheadExceptionsLogger</code>.
 * <ul>
 * <li> `readahead_worker`/wait: counter. number of waits that readahead worker is waiting. If this keeps increasing,
 * it usually means readahead keep getting full because of reader slows down reading.
 * <li> `readahead_worker`/repositions: counter. number of repositions that readhead worker encounters. reposition
 * means that a readahead worker finds that it isn't advancing to a new log segment and force re-positioning.
 * <li> `readahead_worker`/entry_piggy_back_hits: counter. it increases when the last add confirmed being advanced
 * because of the piggy-back lac.
 * <li> `readahead_worker`/entry_piggy_back_misses: counter. it increases when the last add confirmed isn't advanced
 * by a read entry because it doesn't piggy back a newer lac.
 * <li> `readahead_worker`/read_entries: opstats. stats on number of entries read per readahead read batch.
 * <li> `readahead_worker`/read_lac_counter: counter. stats on the number of readLastConfirmed operations
 * <li> `readahead_worker`/read_lac_and_entry_counter: counter. stats on the number of readLastConfirmedAndEntry
 * operations.
 * <li> `readahead_worker`/cache_full: counter. it increases each time readahead worker finds cache become full.
 * If it keeps increasing, that means reader slows down reading.
 * <li> `readahead_worker`/resume: opstats. stats on readahead worker resuming reading from wait state.
 * <li> `readahead_worker`/read_lac_lag: opstats. stats on the number of entries diff between the lac reader knew
 * last time and the lac that it received. if `lag` between two subsequent lacs is high, that might means delay
 * might be high. because reader is only allowed to read entries after lac is advanced.
 * <li> `readahead_worker`/long_poll_interruption: opstats. stats on the number of interruptions happened to long
 * poll. the interruptions are usually because of receiving zookeeper notifications.
 * <li> `readahead_worker`/notification_execution: opstats. stats on executions over the notifications received from
 * zookeeper.
 * <li> `readahead_worker`/metadata_reinitialization: opstats. stats on metadata reinitialization after receiving
 * notifcation from log segments updates.
 * <li> `readahead_worker`/idle_reader_warn: counter. it increases each time the readahead worker detects itself
 * becoming idle.
 * </ul>
 * <h4>Read Lock</h4>
 * All read lock related stats are exposed under scope `read_lock`. See {@link DistributedReentrantLock}
 * for detail stats.
 */
class BKLogReadHandler extends BKLogHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogReadHandler.class);

    private static final int LAYOUT_VERSION = -1;
    private static final Random random = new Random(System.currentTimeMillis());
    protected final LedgerDataAccessor ledgerDataAccessor;
    protected final LedgerHandleCache handleCache;

    protected final ScheduledExecutorService readAheadExecutor;
    protected ReadAheadWorker readAheadWorker = null;
    private volatile boolean logDeleted = false;
    private volatile boolean readAheadError = false;
    private volatile boolean readAheadInterrupted = false;
    private volatile boolean readingFromTruncated = false;
    private final AlertStatsLogger alertStatsLogger;

    private final boolean isHandleForReading;

    // stats
    private final StatsLogger readAheadPerStreamStatsLogger;
    private final Counter readAheadWorkerWaits;
    private final Counter readAheadRepositions;
    private final Counter readAheadEntryPiggyBackHits;
    private final Counter readAheadEntryPiggyBackMisses;
    private final Counter readAheadReadLACCounter;
    private final Counter readAheadReadLACAndEntryCounter;
    private final Counter readAheadCacheFullCounter;
    private final Counter idleReaderWarn;
    private final OpStatsLogger readAheadReadEntriesStat;
    private final OpStatsLogger readAheadCacheResumeStat;
    private final OpStatsLogger readAheadLacLagStats;
    private final OpStatsLogger longPollInterruptionStat;
    private final OpStatsLogger metadataReinitializationStat;
    private final OpStatsLogger notificationExecutionStat;
    private final ReadAheadExceptionsLogger readAheadExceptionsLogger;

    private final OrderedSafeExecutor lockStateExecutor;
    private final Optional<String> subscriberId;
    private final String readLockPath;
    private DistributedReentrantLock readLock;
    private Future<Void> lockAcquireFuture;

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
    public BKLogReadHandler(ZKLogMetadataForReader logMetadata,
                            Optional<String> subscriberId,
                            DistributedLogConfiguration conf,
                            ZooKeeperClientBuilder zkcBuilder,
                            BookKeeperClientBuilder bkcBuilder,
                            OrderedScheduler scheduler,
                            OrderedSafeExecutor lockStateExecutor,
                            ScheduledExecutorService readAheadExecutor,
                            AlertStatsLogger alertStatsLogger,
                            ReadAheadExceptionsLogger readAheadExceptionsLogger,
                            StatsLogger statsLogger,
                            StatsLogger perLogStatsLogger,
                            String clientId,
                            AsyncNotification notification,
                            boolean isHandleForReading) {
        super(logMetadata, conf, zkcBuilder, bkcBuilder, scheduler,
              statsLogger, alertStatsLogger, notification, LogSegmentFilter.DEFAULT_FILTER, clientId);
        this.readAheadExecutor = readAheadExecutor;
        this.alertStatsLogger = alertStatsLogger;
        this.readAheadPerStreamStatsLogger =
                isHandleForReading ? perLogStatsLogger : NullStatsLogger.INSTANCE;
        StatsLogger handlerStatsLogger =
                BroadCastStatsLogger.masterslave(readAheadPerStreamStatsLogger, statsLogger);

        handleCache = LedgerHandleCache.newBuilder()
                .bkc(this.bookKeeperClient)
                .conf(conf)
                .statsLogger(statsLogger)
                .build();
        ledgerDataAccessor = new LedgerDataAccessor(
                getFullyQualifiedName(),
                handlerStatsLogger,
                alertStatsLogger,
                notification,
                conf.getTraceReadAheadDeliveryLatency(),
                conf.getDataLatencyWarnThresholdMillis(),
                Ticker.systemTicker());

        this.subscriberId = subscriberId;
        this.readLockPath = logMetadata.getReadLockPath(subscriberId);
        this.lockStateExecutor = lockStateExecutor;

        this.isHandleForReading = isHandleForReading;
        // Stats

        StatsLogger readAheadStatsLogger = handlerStatsLogger.scope("readahead_worker");
        readAheadWorkerWaits = readAheadStatsLogger.getCounter("wait");
        readAheadRepositions = readAheadStatsLogger.getCounter("repositions");
        readAheadEntryPiggyBackHits = readAheadStatsLogger.getCounter("entry_piggy_back_hits");
        readAheadEntryPiggyBackMisses = readAheadStatsLogger.getCounter("entry_piggy_back_misses");
        readAheadReadEntriesStat = readAheadStatsLogger.getOpStatsLogger("read_entries");
        readAheadReadLACCounter = readAheadStatsLogger.getCounter("read_lac_counter");
        readAheadReadLACAndEntryCounter = readAheadStatsLogger.getCounter("read_lac_and_entry_counter");
        readAheadCacheFullCounter = readAheadStatsLogger.getCounter("cache_full");
        readAheadCacheResumeStat = readAheadStatsLogger.getOpStatsLogger("resume");
        readAheadLacLagStats = readAheadStatsLogger.getOpStatsLogger("read_lac_lag");
        longPollInterruptionStat = readAheadStatsLogger.getOpStatsLogger("long_poll_interruption");
        notificationExecutionStat = readAheadStatsLogger.getOpStatsLogger("notification_execution");
        metadataReinitializationStat = readAheadStatsLogger.getOpStatsLogger("metadata_reinitialization");
        idleReaderWarn = readAheadStatsLogger.getCounter("idle_reader_warn");
        this.readAheadExceptionsLogger = readAheadExceptionsLogger;
    }

    @VisibleForTesting
    String getReadLockPath() {
        return readLockPath;
    }

    <T> void satisfyPromiseAsync(final Promise<T> promise, final Try<T> result) {
        scheduler.submit(new SafeRunnable() {
            @Override
            public void safeRun() {
                promise.update(result);
            }
        });
    }

    /**
     * Elective stream lock--readers are not required to acquire the lock before using the stream.
     */
    synchronized Future<Void> lockStream() {
        if (null == lockAcquireFuture) {
            final Function0<DistributedReentrantLock> lockFunction =  new ExceptionalFunction0<DistributedReentrantLock>() {
                @Override
                public DistributedReentrantLock applyE() throws IOException {
                    // Unfortunately this has a blocking call which we should not execute on the
                    // ZK completion thread
                    BKLogReadHandler.this.readLock = new DistributedReentrantLock(
                            lockStateExecutor,
                            zooKeeperClient,
                            readLockPath,
                            conf.getLockTimeoutMilliSeconds(),
                            getLockClientId(),
                            statsLogger.scope("read_lock"),
                            conf.getZKNumRetries(),
                            conf.getLockReacquireTimeoutMilliSeconds(),
                            conf.getLockOpTimeoutMilliSeconds());

                    LOG.info("acquiring readlock {} at {}", getLockClientId(), readLockPath);
                    return BKLogReadHandler.this.readLock;
                }
            };
            lockAcquireFuture = ensureReadLockPathExist().flatMap(new ExceptionalFunction<Void, Future<Void>>() {
                @Override
                public Future<Void> applyE(Void in) throws Throwable {
                    return scheduler.apply(lockFunction).flatMap(new ExceptionalFunction<DistributedReentrantLock, Future<Void>>() {
                        @Override
                        public Future<Void> applyE(DistributedReentrantLock lock) throws IOException {
                            return acquireLockOnExecutorThread(lock);
                        }
                    });
                }
            });
        }
        return lockAcquireFuture;
    }

    /**
     * Begin asynchronous lock acquire, but ensure that the returned future is satisfied on an
     * executor service thread.
     */
    Future<Void> acquireLockOnExecutorThread(DistributedReentrantLock lock) throws LockingException {
        final Future<Void> acquireFuture = lock.asyncAcquire(
            DistributedReentrantLock.LockReason.READHANDLER);

        // The future we return must be satisfied on an executor service thread. If we simply
        // return the future returned by asyncAcquire, user callbacks may end up running in
        // the lock state executor thread, which will cause deadlocks and introduce latency
        // etc.
        final Promise<Void> threadAcquirePromise = new Promise<Void>();
        threadAcquirePromise.setInterruptHandler(new Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable t) {
                FutureUtils.cancel(acquireFuture);
                return null;
            }
        });
        acquireFuture.addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void complete) {
                LOG.info("acquired readlock {} at {}", getLockClientId(), readLockPath);
                satisfyPromiseAsync(threadAcquirePromise, new Return<Void>(null));
            }

            @Override
            public void onFailure(Throwable cause) {
                LOG.info("failed to acquire readlock {} at {}",
                        new Object[]{getLockClientId(), readLockPath, cause});
                satisfyPromiseAsync(threadAcquirePromise, new Throw<Void>(cause));
            }
        });
        return threadAcquirePromise;
    }

    /**
     * Check ownership of elective stream lock.
     */
    void checkReadLock() throws DLIllegalStateException, LockingException {
        synchronized (this) {
            if ((null == lockAcquireFuture) ||
                (!lockAcquireFuture.isDefined())) {
                throw new DLIllegalStateException("Attempt to check for lock before it has been acquired successfully");
            }
        }

        readLock.checkOwnership();
    }

    public void close() {
        try {
            if (null != readAheadWorker) {
                LOG.info("Stopping Readahead worker for {}", getFullyQualifiedName());
                readAheadWorker.stop();
            }
            if (null != ledgerDataAccessor) {
                ledgerDataAccessor.clear();
            }
            if (null != handleCache) {
                handleCache.clear();
            }
            super.close();
        } finally {
            synchronized (this) {
                if (null != lockAcquireFuture && !lockAcquireFuture.isDefined()) {
                    FutureUtils.cancel(lockAcquireFuture);
                }
                if (null != readLock) {
                    // Force close--we may or may not have
                    // actually acquired the lock by now. The
                    // effect will be to abort any acquire
                    // attempts.
                    LOG.info("closing readlock {} at {}", getLockClientId(), readLockPath);
                    readLock.close();
                }
            }
        }
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
        } else {
            readAheadWorker.advanceReadAhead(startPosition);
        }
    }

    public boolean isReadAheadCaughtUp() {
        return null != readAheadWorker && readAheadWorker.isCaughtUp();
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

    void setReadAheadError(ReadAheadTracker tracker) {
        LOG.error("Read Ahead for {} is set to error.", getFullyQualifiedName());
        readAheadError = true;
        tracker.enterPhase(ReadAheadPhase.ERROR);
        if (null != notification) {
            notification.notifyOnError();
        }
    }

    private Future<Void> ensureReadLockPathExist() {
        final Promise<Void> promise = new Promise<Void>();
        promise.setInterruptHandler(new com.twitter.util.Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable t) {
                promise.setException(new LockCancelledException(readLockPath, "Could not ensure read lock path", t));
                return null;
            }
        });
        Optional<String> parentPathShouldNotCreate = Optional.of(logMetadata.getLogRootPath());
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zooKeeperClient, readLockPath, parentPathShouldNotCreate,
                new byte[0], zooKeeperClient.getDefaultACL(), CreateMode.PERSISTENT,
                new org.apache.zookeeper.AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(final int rc, final String path, Object ctx, String name) {
                        scheduler.submit(new Runnable() {
                            @Override
                            public void run() {
                                if (KeeperException.Code.NONODE.intValue() == rc) {
                                    promise.setException(new LogNotFoundException(String.format("Log %s does not exist or has been deleted", getFullyQualifiedName())));
                                } else if (KeeperException.Code.OK.intValue() == rc) {
                                    promise.setValue(null);
                                    LOG.trace("Created path {}.", path);
                                } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                                    promise.setValue(null);
                                    LOG.trace("Path {} is already existed.", path);
                                } else if (DistributedLogConstants.ZK_CONNECTION_EXCEPTION_RESULT_CODE == rc) {
                                    promise.setException(new ZooKeeperClient.ZooKeeperConnectionException(path));
                                } else if (DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE == rc) {
                                    promise.setException(new DLInterruptedException(path));
                                } else {
                                    promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                                }
                            }
                        });
                    }
                }, null);
        return promise;
    }

    void setReadAheadInterrupted(ReadAheadTracker tracker) {
        readAheadInterrupted = true;
        tracker.enterPhase(ReadAheadPhase.INTERRUPTED);
        if (null != notification) {
            notification.notifyOnError();
        }
    }

    void setReadingFromTruncated(ReadAheadTracker tracker) {
        readingFromTruncated = true;
        tracker.enterPhase(ReadAheadPhase.TRUNCATED);
        if (null != notification) {
            notification.notifyOnError();
        }
    }

    public void checkClosedOrInError()
            throws LogNotFoundException, LogReadException, DLInterruptedException, AlreadyTruncatedTransactionException {
        if (logDeleted) {
            throw new LogNotFoundException(getFullyQualifiedName() + " is already deleted.");
        } else if (readingFromTruncated) {
            throw new AlreadyTruncatedTransactionException(
                String.format("%s: Trying to position read ahead a segment that is marked truncated",
                    getFullyQualifiedName()));
        } else if (readAheadInterrupted) {
            throw new DLInterruptedException(String.format("%s: ReadAhead Thread was interrupted",
                getFullyQualifiedName()));
        } else if (readAheadError) {
            throw new LogReadException(String.format("%s: ReadAhead Thread encountered exceptions",
                getFullyQualifiedName()));
        }
    }

    public void raiseAlert(String msg, Object... args) {
        alertStatsLogger.raise(msg, args);
    }

    public LogRecordWithDLSN getNextReadAheadRecord() throws IOException {
        return ledgerDataAccessor.getNextReadAheadRecord();
    }

    public boolean checkForReaderStall(int idleReaderErrorThreshold, TimeUnit timeUnit) {
        return ledgerDataAccessor.checkForReaderStall(idleReaderErrorThreshold, timeUnit);
    }

    @VisibleForTesting
    void disableReadAheadZKNotification() {
        if (null != readAheadWorker) {
            readAheadWorker.disableZKNotification();
        }
    }

    static enum ReadAheadPhase {
        ERROR(-5),
        TRUNCATED(-4),
        INTERRUPTED(-3),
        STOPPED(-2),
        EXCEPTION_HANDLING(-1),
        SCHEDULE_READAHEAD(0),
        GET_LEDGERS(1),
        OPEN_LEDGER(2),
        CLOSE_LEDGER(3),
        READ_LAST_CONFIRMED(4),
        READ_ENTRIES(5);

        int code;

        ReadAheadPhase(int code) {
            this.code = code;
        }

        int getCode() {
            return this.code;
        }
    }

    /**
     * ReadAheadTracker is tracking the progress of readahead worker. so we could use it to investigate where
     * the readahead worker is.
     */
    static class ReadAheadTracker {
        // ticks is used to differentiate that the worker enter same phase in different time.
        final AtomicLong ticks = new AtomicLong(0);
        // which phase that the worker is in.
        ReadAheadPhase phase;

        ReadAheadTracker(String streamName,
                         final LedgerDataAccessor ledgerDataAccessor,
                         ReadAheadPhase initialPhase,
                         StatsLogger statsLogger) {
            this.phase = initialPhase;
            statsLogger.registerGauge("phase", new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return ReadAheadPhase.SCHEDULE_READAHEAD.getCode();
                }

                @Override
                public Number getSample() {
                    return phase.getCode();
                }
            });
            statsLogger.registerGauge("ticks", new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return ticks.get();
                }
            });
            statsLogger.registerGauge("cache_entries", new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return ledgerDataAccessor.getNumCachedRecords();
                }
            });
        }

        ReadAheadPhase getPhase() {
            return this.phase;
        }

        void enterPhase(ReadAheadPhase readAheadPhase) {
            this.ticks.incrementAndGet();
            this.phase = readAheadPhase;
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
    class ReadAheadWorker implements ReadAheadCallback, Runnable, Watcher {

        private final String fullyQualifiedName;
        private final BKLogReadHandler bkLedgerManager;
        private volatile boolean reInitializeMetadata = true;
        private final LedgerReadPosition startReadPosition;
        protected LedgerReadPosition nextReadAheadPosition;
        // Indicates the position to which the continuous log reader has been positioned. Its not
        // an accurate position as its only updated when necessary
        private AtomicReference<LedgerReadPosition> continuousReaderPosition = new AtomicReference<LedgerReadPosition>(null);
        private LogSegmentMetadata currentMetadata = null;
        private int currentMetadataIndex;
        protected LedgerDescriptor currentLH;
        private final LedgerDataAccessor ledgerDataAccessor;
        private volatile List<LogSegmentMetadata> ledgerList;
        private boolean simulateErrors;
        private boolean simulateDelays;
        private boolean injectReadAheadStall;
        private int maxInjectedDelayMs;
        private int injectedDelayPercent;
        private final DistributedLogConfiguration conf;
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
        volatile long metadataNotificationTimeMillis = -1L;

        // ReadAhead Phases
        final Phase schedulePhase = new ScheduleReadAheadPhase();
        final Phase exceptionHandler = new ExceptionHandlePhase(schedulePhase);
        final Phase readAheadPhase =
                new CheckInProgressChangedPhase(new OpenLedgerPhase(new ReadEntriesPhase(schedulePhase)));
        final ReadAheadTracker tracker;
        final Stopwatch resumeStopWatch;
        final Stopwatch lastLedgerCloseDetected = Stopwatch.createUnstarted();

        // ReadAhead Status
        private volatile boolean isCatchingUp = true;

        // zookeeper watcher
        private final Watcher getLedgersWatcher;

        public ReadAheadWorker(String fullyQualifiedName,
                               BKLogReadHandler ledgerManager,
                               LedgerReadPosition startPosition,
                               LedgerDataAccessor ledgerDataAccessor,
                               boolean simulateErrors,
                               DistributedLogConfiguration conf) {
            this.bkLedgerManager = ledgerManager;
            this.fullyQualifiedName = fullyQualifiedName;
            this.startReadPosition = new LedgerReadPosition(startPosition);
            this.nextReadAheadPosition = new LedgerReadPosition(startPosition);
            this.ledgerDataAccessor = ledgerDataAccessor;
            this.simulateErrors = simulateErrors;
            this.simulateDelays = conf.getEIInjectReadAheadDelay();
            this.injectReadAheadStall = conf.getEIInjectReadAheadStall();
            this.maxInjectedDelayMs = conf.getEIInjectMaxReadAheadDelayMs();
            this.injectedDelayPercent = Math.max(0, Math.min(100, conf.getEIInjectReadAheadDelayPercent()));
            if ((conf.getReadLACOption() >= ReadLACOption.INVALID_OPTION.value) ||
                (conf.getReadLACOption() < ReadLACOption.DEFAULT.value)) {
                LOG.warn("Invalid value of ReadLACOption configured {}", conf.getReadLACOption());
                conf.setReadLACOption(ReadLACOption.DEFAULT.value);
            }
            this.conf = conf;
            this.noLedgerExceptionOnReadLACThreshold =
                    conf.getReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis() / conf.getReadAheadWaitTime();
            this.tracker = new ReadAheadTracker(logMetadata.getLogName(), ledgerDataAccessor,
                    ReadAheadPhase.SCHEDULE_READAHEAD, readAheadPerStreamStatsLogger);
            this.resumeStopWatch = Stopwatch.createUnstarted();
            this.getLedgersWatcher = this.bkLedgerManager.zooKeeperClient.getWatcherManager()
                    .registerChildWatcher(this.bkLedgerManager.logMetadata.getLogSegmentsPath(), this);
        }

        public void simulateErrors() {
            this.simulateErrors = true;
        }

        public boolean isCaughtUp() {
            return !isCatchingUp;
        }

        public void start() {
            LOG.debug("Starting ReadAhead Worker for {}", fullyQualifiedName);
            running = true;
            schedulePhase.process(BKException.Code.OK);
        }

        public void stop() {
            running = false;

            this.bkLedgerManager.zooKeeperClient.getWatcherManager()
                    .unregisterChildWatcher(this.bkLedgerManager.logMetadata.getLogSegmentsPath(), this);

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
                ", NextReadAheadPosition:" + nextReadAheadPosition +
                ", BKZKExceptions:" + bkcZkExceptions.get() +
                ", BKUnexpectedExceptions:" + bkcUnExpectedExceptions.get() +
                ", EncounteredException:" + encounteredException +
                ", readAheadError:" + readAheadError +
                ", readAheadInterrupted" + readAheadInterrupted +
                ", CurrentMetadata:" + ((null != currentMetadata) ? currentMetadata : "NONE") +
                ", SimulateDelays:" + simulateDelays;
        }

        @Override
        public void resumeReadAhead() {
            try {
                long cacheResumeLatency = resumeStopWatch.stop().elapsed(TimeUnit.MICROSECONDS);
                readAheadCacheResumeStat.registerSuccessfulEvent(cacheResumeLatency);
            } catch (IllegalStateException ise) {
                LOG.error("Encountered illegal state when stopping resume stop watch for {} : ", getFullyQualifiedName(), ise);
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
                        LOG.error("ReadAhead on stream {} encountered runtime exception", getFullyQualifiedName(), rte);
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
            if (injectReadAheadStall && Utils.randomPercent(10)) {
                LOG.warn("Error injected: read ahead for stream {} is going to stall.", getFullyQualifiedName());
                return;
            }

            if (simulateDelays && Utils.randomPercent(injectedDelayPercent)) {
                int delayMs = random.nextInt(maxInjectedDelayMs);
                schedule(runnable, delayMs);
                return;
            }

            try {
                readAheadExecutor.submit(addRTEHandler(runnable));
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError(tracker);
            }
        }

        private void schedule(Runnable runnable, long timeInMillis) {
            try {
                InterruptibleScheduledRunnable task = new InterruptibleScheduledRunnable(runnable);
                boolean executeImmediately = setMetadataNotification(task);
                if (executeImmediately) {
                    readAheadExecutor.submit(addRTEHandler(task));
                    return;
                }
                readAheadExecutor.schedule(addRTEHandler(task), timeInMillis, TimeUnit.MILLISECONDS);
                readAheadWorkerWaits.inc();
            } catch (RejectedExecutionException ree) {
                bkLedgerManager.setReadAheadError(tracker);
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
                bkLedgerManager.getHandleCache().closeLedger(ld);
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
                    tracker.enterPhase(ReadAheadPhase.STOPPED);
                    synchronized (stopNotification) {
                        stopNotification.notifyAll();
                    }
                    LOG.info("Stopped ReadAheadWorker for {}", fullyQualifiedName);
                    return;
                }
                tracker.enterPhase(ReadAheadPhase.SCHEDULE_READAHEAD);

                boolean simulate = simulateErrors && Utils.randomPercent(2);
                if (encounteredException || simulate) {
                    int zkErrorThreshold = BKC_ZK_EXCEPTION_THRESHOLD_IN_SECONDS * 1000 * 4 / conf.getReadAheadWaitTime();

                    if ((bkcZkExceptions.get() > zkErrorThreshold) || simulate) {
                        LOG.error("{} : BookKeeper Client used by the ReadAhead Thread has encountered {} zookeeper exceptions : simulate = {}",
                                  new Object[] { fullyQualifiedName, bkcZkExceptions.get(), simulate });
                        running = false;
                        bkLedgerManager.setReadAheadError(tracker);
                    } else if (bkcUnExpectedExceptions.get() > BKC_UNEXPECTED_EXCEPTION_THRESHOLD) {
                        LOG.error("{} : ReadAhead Thread has encountered {} unexpected BK exceptions.",
                                  fullyQualifiedName, bkcUnExpectedExceptions.get());
                        running = false;
                        bkLedgerManager.setReadAheadError(tracker);
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
                    bkLedgerManager.setReadAheadInterrupted(tracker);
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
                                        getFullyQualifiedName());
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

                            DLSN nextReadDLSN = new DLSN(nextReadAheadPosition.getLogSegmentSequenceNumber(), nextReadAheadPosition.getEntryId(), -1);

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
                                    bkLedgerManager.raiseAlert("Trying to position reader on {} when {} is marked partially truncated",
                                        nextReadAheadPosition, l);
                                }

                                if (!conf.getIgnoreTruncationStatus()) {
                                    LOG.error("{}: Trying to position reader on {} when {} is marked partially truncated",
                                        new Object[]{ getFullyQualifiedName(), nextReadAheadPosition, l});
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
                                    ledgerDataAccessor.setMinActiveDLSN(l.getMinActiveDLSN());
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
                                             new Object[] {nextReadAheadPosition, getFullyQualifiedName(), System.currentTimeMillis() });
                                }

                                if (l.isTruncated()) {
                                    if (conf.getAlertWhenPositioningOnTruncated()) {
                                        bkLedgerManager.raiseAlert("Trying to position reader on {} when {} is marked truncated",
                                            nextReadAheadPosition, l);
                                    }

                                    if (!conf.getIgnoreTruncationStatus()) {
                                        LOG.error("{}: Trying to position reader on {} when {} is marked truncated",
                                            new Object[]{getFullyQualifiedName(), nextReadAheadPosition, l});
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
                                ledgerDataAccessor.setSuppressDeliveryLatency(false);
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
                        bkLedgerManager.reportGetSegmentStats = true;
                    }
                });
            }

            @Override
            void process(int rc) {
                if (!running) {
                    tracker.enterPhase(ReadAheadPhase.STOPPED);
                    synchronized (stopNotification) {
                        stopNotification.notifyAll();
                    }
                    LOG.info("Stopped ReadAheadWorker for {}", fullyQualifiedName);
                    return;
                }

                tracker.enterPhase(ReadAheadPhase.GET_LEDGERS);

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
                        readAheadRepositions.inc();
                    }
                }

                if (reInitializeMetadata || null == currentMetadata) {
                    reInitializeMetadata = false;
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Reinitializing metadata for {}.", fullyQualifiedName);
                    }
                    if (metadataNotificationTimeMillis > 0) {
                        long metadataReinitializeTimeMillis = System.currentTimeMillis();
                        long elapsedMillisSinceMetadataChanged = metadataReinitializeTimeMillis - metadataNotificationTimeMillis;
                        if (elapsedMillisSinceMetadataChanged >= BKLogReadHandler.this.metadataLatencyWarnThresholdMillis) {
                            LOG.warn("{} reinitialize metadata at {}, which is {} millis after receiving notification at {}.",
                                     new Object[] { getFullyQualifiedName(), metadataReinitializeTimeMillis,
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
                            AsyncCallback.ReadLastConfirmedCallback,
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
                bkLedgerManager.getHandleCache().asyncReadLastConfirmedAndEntry(
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
                tracker.enterPhase(ReadAheadPhase.OPEN_LEDGER);

                if (currentMetadata.isInProgress()) { // we don't want to fence the current journal
                    if (null == currentLH) {
                        if (conf.getTraceReadAheadMetadataChanges()) {
                            LOG.info("Opening inprogress ledger of {} for {} at {}.",
                                     new Object[] { currentMetadata, fullyQualifiedName, System.currentTimeMillis() });
                        }
                        bkLedgerManager.getHandleCache().asyncOpenLedger(currentMetadata, false)
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
                            isClosed = bkLedgerManager.getHandleCache().isLedgerHandleClosed(currentLH);
                            lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
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
                                ledgerDataAccessor.setSuppressDeliveryLatency(false);
                                if (isHandleForReading) {
                                    LOG.info("{} caught up at {}: lac = {}, position = {}.",
                                             new Object[] { fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition });
                                }
                            }

                            LOG.trace("Reading last add confirmed of {} for {}, as read poistion has moved over {} : {}, lac option: {}",
                                    new Object[] { currentMetadata, fullyQualifiedName, lastAddConfirmed, nextReadAheadPosition, conf.getReadLACOption()});

                            if (nextReadAheadPosition.getEntryId() == 0 && conf.getTraceReadAheadMetadataChanges()) {
                                // we are waiting for first entry to arrive
                                LOG.info("Reading last add confirmed for {} at {}: lac = {}, position = {}.",
                                         new Object[] { fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition});
                            } else {
                                LOG.trace("Reading last add confirmed for {} at {}: lac = {}, position = {}.",
                                          new Object[] { fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition});
                            }
                            switch(ReadLACOption.values()[conf.getReadLACOption()]) {
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
                                    bkLedgerManager.getHandleCache().asyncTryReadLastConfirmed(currentLH)
                                            .addEventListener(new FutureEventListener<Long>() {
                                                @Override
                                                public void onSuccess(Long lac) {
                                                    readLastConfirmedComplete(BKException.Code.OK, lac, null);
                                                }

                                                @Override
                                                public void onFailure(Throwable cause) {
                                                    readLastConfirmedComplete(FutureUtils.bkResultCode(cause), lastAddConfirmed, null);
                                                }
                                            });
                                    readAheadReadLACCounter.inc();
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
                                LOG.trace("Closing completed ledger of {} for {}.", currentMetadata, fullyQualifiedName);
                                if (!closeCurrentLedgerHandle()) {
                                    return;
                                }
                                inProgressChanged = false;
                            } else {
                                long lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
                                if (nextReadAheadPosition.getEntryId() > lastAddConfirmed) {
                                    // Its possible that the last entryId does not account for the control
                                    // log record, but the lastAddConfirmed should never be short of the
                                    // last entry id; else we maybe missing an entry
                                    boolean gapDetected = false;
                                    if (lastAddConfirmed < currentMetadata.getLastEntryId()) {
                                        bkLedgerManager.raiseAlert("Unexpected last entry id during read ahead; {} , {}",
                                            currentMetadata, lastAddConfirmed);
                                        gapDetected = true;
                                    }

                                    if (conf.getPositionGapDetectionEnabled() && gapDetected) {
                                        setReadAheadError(tracker);
                                    } else {
                                        // This disconnect will only surface during repositioning and
                                        // will not silently miss records; therefore its safe to not halt
                                        // reading, but we should print a warning for easy diagnosis
                                        if (lastAddConfirmed > (currentMetadata.getLastEntryId() + 1)) {
                                            LOG.warn("Potential Metadata Corruption {} for stream {}, lastAddConfirmed {}",
                                                    new Object[] {currentMetadata, getFullyQualifiedName(), lastAddConfirmed});
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
                                                bkLedgerManager.raiseAlert("Unexpected condition during read ahead; {} , {}",
                                                    currentMetadata, oldMetadata);
                                                setReadAheadError(tracker);
                                            } else {
                                                if (currentMetadata.isTruncated()) {
                                                    if (conf.getAlertWhenPositioningOnTruncated()) {
                                                        bkLedgerManager.raiseAlert("Trying to position reader on the log segment that is marked truncated : {}",
                                                            currentMetadata);
                                                    }

                                                    if (!conf.getIgnoreTruncationStatus()) {
                                                        LOG.error("{}: Trying to position reader on the log segment that is marked truncated : {}",
                                                                getFullyQualifiedName(), currentMetadata);
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
                        bkLedgerManager.getHandleCache().asyncOpenLedger(currentMetadata, true)
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

            @Override
            public void readLastConfirmedComplete(final int rc, final long lastConfirmed, final Object ctx) {
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

                            ledgerDataAccessor.set(new LedgerReadPosition(entry.getLedgerId(), currentLH.getLogSegmentSequenceNo(), entry.getEntryId()),
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
                                         new Object[] { getFullyQualifiedName(), nextReadAheadPosition.getEntryId(), lastConfirmed });
                            }
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
                tracker.enterPhase(ReadAheadPhase.READ_ENTRIES);

                cacheFull = false;
                lastAddConfirmed = -1;
                if (null != currentLH) {
                    try {
                        lastAddConfirmed = bkLedgerManager.getHandleCache().getLastAddConfirmed(currentLH);
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
                long startEntryId = nextReadAheadPosition.getEntryId();
                long endEntryId = Math.min(lastAddConfirmed, (nextReadAheadPosition.getEntryId() + conf.getReadAheadBatchSize() - 1));

                if (endEntryId <= conf.getReadAheadBatchSize() && conf.getTraceReadAheadMetadataChanges()) {
                    // trace first read batch
                    LOG.info("Reading entries ({} - {}) for {} at {} : lac = {}, nextReadAheadPosition = {}.",
                             new Object[] { startEntryId, endEntryId, fullyQualifiedName, System.currentTimeMillis(), lastAddConfirmed, nextReadAheadPosition});
                }

                final String readCtx = String.format("ReadEntries(%d-%d)", startEntryId, endEntryId);
                bkLedgerManager.getHandleCache().asyncReadEntries(currentLH, startEntryId, endEntryId)
                        .addEventListener(new FutureEventListener<Enumeration<LedgerEntry>>() {

                            @Override
                            public void onSuccess(Enumeration<LedgerEntry> entries) {
                                readComplete(BKException.Code.OK, null, entries, readCtx);
                            }

                            @Override
                            public void onFailure(Throwable cause) {
                                readComplete(FutureUtils.bkResultCode(cause), null, null, readCtx);
                            }
                        });
            }

            @Override
            public void readComplete(final int rc, final LedgerHandle lh,
                                     final Enumeration<LedgerEntry> seq, final Object ctx) {
                // submit callback execution to dlg executor to avoid deadlock.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            readAheadReadEntriesStat.registerFailedEvent(0);
                            LOG.debug("BK Exception {} while reading entry", rc);
                            handleException(ReadAheadPhase.READ_ENTRIES, rc);
                            return;
                        }
                        int numReads = 0;
                        while (seq.hasMoreElements()) {
                            bkcZkExceptions.set(0);
                            bkcUnExpectedExceptions.set(0);
                            nextReadAheadPosition.advance();
                            LedgerEntry e = seq.nextElement();
                            LedgerReadPosition readPosition = new LedgerReadPosition(e.getLedgerId(), currentMetadata.getLogSegmentSequenceNumber(), e.getEntryId());
                            ledgerDataAccessor.set(readPosition, e, null != ctx ? ctx.toString() : "",
                                    currentMetadata.getEnvelopeEntries(), currentMetadata.getStartSequenceId());
                            ++numReads;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Read entry {} of {}.", readPosition, fullyQualifiedName);
                            }
                        }
                        readAheadReadEntriesStat.registerSuccessfulEvent(numReads);
                        if (ledgerDataAccessor.getNumCachedRecords() >= conf.getReadAheadMaxEntries()) {
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
                    ledgerDataAccessor.setReadAheadCallback(ReadAheadWorker.this, conf.getReadAheadMaxEntries());
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
