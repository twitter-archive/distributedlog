package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.LockCancelledException;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForReader;
import com.twitter.distributedlog.injector.AsyncFailureInjector;
import com.twitter.distributedlog.logsegment.LogSegmentFilter;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.readahead.ReadAheadWorker;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.stats.ReadAheadExceptionsLogger;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.lock.DistributedReentrantLock;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import com.twitter.util.Try;
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
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

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

    protected final ZKLogMetadataForReader logMetadataForReader;
    protected final ReadAheadCache readAheadCache;
    protected final LedgerHandleCache handleCache;

    protected final ScheduledExecutorService readAheadExecutor;
    protected final DynamicDistributedLogConfiguration dynConf;
    protected ReadAheadWorker readAheadWorker = null;
    private final boolean isHandleForReading;

    private final OrderedScheduler lockStateExecutor;
    private final Optional<String> subscriberId;
    private final String readLockPath;
    private DistributedReentrantLock readLock;
    private Future<Void> lockAcquireFuture;

    // stats
    private final AlertStatsLogger alertStatsLogger;
    private final StatsLogger handlerStatsLogger;
    private final StatsLogger perLogStatsLogger;
    private final ReadAheadExceptionsLogger readAheadExceptionsLogger;

    /**
     * Construct a Bookkeeper journal manager.
     */
    public BKLogReadHandler(ZKLogMetadataForReader logMetadata,
                            Optional<String> subscriberId,
                            DistributedLogConfiguration conf,
                            DynamicDistributedLogConfiguration dynConf,
                            ZooKeeperClientBuilder zkcBuilder,
                            BookKeeperClientBuilder bkcBuilder,
                            LogSegmentMetadataStore metadataStore,
                            OrderedScheduler scheduler,
                            OrderedScheduler lockStateExecutor,
                            ScheduledExecutorService readAheadExecutor,
                            AlertStatsLogger alertStatsLogger,
                            ReadAheadExceptionsLogger readAheadExceptionsLogger,
                            StatsLogger statsLogger,
                            StatsLogger perLogStatsLogger,
                            String clientId,
                            AsyncNotification notification,
                            boolean isHandleForReading) {
        super(logMetadata, conf, zkcBuilder, bkcBuilder, metadataStore, scheduler,
              statsLogger, alertStatsLogger, notification, LogSegmentFilter.DEFAULT_FILTER, clientId);
        this.logMetadataForReader = logMetadata;
        this.dynConf = dynConf;
        this.readAheadExecutor = readAheadExecutor;
        this.alertStatsLogger = alertStatsLogger;
        this.perLogStatsLogger =
                isHandleForReading ? perLogStatsLogger : NullStatsLogger.INSTANCE;
        this.handlerStatsLogger =
                BroadCastStatsLogger.masterslave(this.perLogStatsLogger, statsLogger);
        this.readAheadExceptionsLogger = readAheadExceptionsLogger;

        handleCache = LedgerHandleCache.newBuilder()
                .bkc(this.bookKeeperClient)
                .conf(conf)
                .statsLogger(statsLogger)
                .build();
        readAheadCache = new ReadAheadCache(
                getFullyQualifiedName(),
                handlerStatsLogger,
                alertStatsLogger,
                notification,
                dynConf.getReadAheadMaxRecords(),
                conf.getTraceReadAheadDeliveryLatency(),
                conf.getDataLatencyWarnThresholdMillis(),
                Ticker.systemTicker());

        this.subscriberId = subscriberId;
        this.readLockPath = logMetadata.getReadLockPath(subscriberId);
        this.lockStateExecutor = lockStateExecutor;

        this.isHandleForReading = isHandleForReading;
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
        final Future<Boolean> acquireFuture = lock.asyncAcquire(
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
        acquireFuture.addEventListener(new FutureEventListener<Boolean>() {
            @Override
            public void onSuccess(Boolean acquired) {
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
            if (null != readAheadCache) {
                readAheadCache.clear();
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

    public void startReadAhead(LedgerReadPosition startPosition,
                               AsyncFailureInjector failureInjector) {
        if (null == readAheadWorker) {
            readAheadWorker = new ReadAheadWorker(
                    conf,
                    dynConf,
                    logMetadataForReader,
                    this,
                    zooKeeperClient,
                    readAheadExecutor,
                    handleCache,
                    startPosition,
                    readAheadCache,
                    isHandleForReading,
                    readAheadExceptionsLogger,
                    handlerStatsLogger,
                    perLogStatsLogger,
                    alertStatsLogger,
                    failureInjector,
                    notification);
            readAheadWorker.start();
        }
    }

    public boolean isReadAheadCaughtUp() {
        return null != readAheadWorker && readAheadWorker.isCaughtUp();
    }

    public LedgerHandleCache getHandleCache() {
        return handleCache;
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

    public LogRecordWithDLSN getNextReadAheadRecord() throws IOException {
        return readAheadCache.getNextReadAheadRecord();
    }

    public ReadAheadCache getReadAheadCache() {
        return readAheadCache;
    }

    @VisibleForTesting
    void disableReadAheadZKNotification() {
        if (null != readAheadWorker) {
            readAheadWorker.disableZKNotification();
        }
    }

}
