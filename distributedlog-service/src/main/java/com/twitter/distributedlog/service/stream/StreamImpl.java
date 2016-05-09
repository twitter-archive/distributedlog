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
package com.twitter.distributedlog.service.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.exceptions.AlreadyClosedException;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.StreamNotReadyException;
import com.twitter.distributedlog.exceptions.StreamUnavailableException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.io.Abortables;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.service.FatalErrorHandler;
import com.twitter.distributedlog.service.ServerFeatureKeys;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.stream.limiter.StreamRequestLimiter;
import com.twitter.distributedlog.service.streamset.Partition;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.TimeSequencer;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamImpl implements Stream {
    static final Logger logger = LoggerFactory.getLogger(StreamImpl.class);

    public static enum StreamStatus {
        UNINITIALIZED(-1),
        INITIALIZING(0),
        INITIALIZED(1),
        // if a stream is in failed state, it could be retried immediately.
        // a stream will be put in failed state when encountered any stream exception.
        FAILED(-2),
        // if a stream is in backoff state, it would backoff for a while.
        // a stream will be put in backoff state when failed to acquire the ownership.
        BACKOFF(-3),
        CLOSING(-4),
        CLOSED(-5),
        // if a stream is in error state, it should be abort during closing.
        ERROR(-6);

        final int code;

        StreamStatus(int code) {
            this.code = code;
        }

        int getCode() {
            return code;
        }

        static boolean isUnavailable(StreamStatus status) {
            return StreamStatus.ERROR == status || StreamStatus.CLOSING == status || StreamStatus.CLOSED == status;
        }
    }

    private final String name;
    private final Partition partition;
    private DistributedLogManager manager;

    // A write has been attempted since the last stream acquire.
    private volatile boolean writeSinceLastAcquire = false;
    private volatile AsyncLogWriter writer;
    private volatile StreamStatus status;
    private volatile String owner;
    private volatile Throwable lastException;
    private volatile boolean running = true;
    private volatile boolean suspended = false;
    private volatile Queue<StreamOp> pendingOps = new ArrayDeque<StreamOp>();

    private final Promise<Void> closePromise = new Promise<Void>();
    private final Object txnLock = new Object();
    private final TimeSequencer sequencer = new TimeSequencer();
    // last acquire time
    private final Stopwatch lastAcquireWatch = Stopwatch.createUnstarted();
    // last acquire failure time
    private final Stopwatch lastAcquireFailureWatch = Stopwatch.createUnstarted();
    private final long nextAcquireWaitTimeMs;
    private ScheduledFuture<?> tryAcquireScheduledFuture = null;
    private long scheduledAcquireDelayMs = 0L;
    private final StreamRequestLimiter limiter;
    private final DynamicDistributedLogConfiguration dynConf;
    private final DistributedLogConfiguration dlConfig;
    private final DistributedLogNamespace dlNamespace;
    private final String clientId;
    private final OrderedScheduler scheduler;
    private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final Feature featureRateLimitDisabled;
    private final StreamManager streamManager;
    private final StreamConfigProvider streamConfigProvider;
    private final FatalErrorHandler fatalErrorHandler;
    private final long streamProbationTimeoutMs;
    private long serviceTimeoutMs;
    private final boolean failFastOnStreamNotReady;
    private final HashedWheelTimer requestTimer;

    // Stats
    private final StatsLogger streamLogger;
    private final StatsLogger streamExceptionStatLogger;
    private final StatsLogger limiterStatLogger;
    private final Counter serviceTimeout;
    private final OpStatsLogger streamAcquireStat;
    private final Counter pendingOpsCounter;
    private final Counter unexpectedExceptions;
    private final StatsLogger exceptionStatLogger;
    private final ConcurrentHashMap<String, Counter> exceptionCounters =
        new ConcurrentHashMap<String, Counter>();

    // Since we may create and discard streams at initialization if there's a race,
    // must not do any expensive intialization here (particularly any locking or
    // significant resource allocation etc.).
    StreamImpl(final String name,
               final Partition partition,
               String clientId,
               StreamManager streamManager,
               StreamOpStats streamOpStats,
               ServerConfiguration serverConfig,
               DistributedLogConfiguration dlConfig,
               DynamicDistributedLogConfiguration streamConf,
               FeatureProvider featureProvider,
               StreamConfigProvider streamConfigProvider,
               DistributedLogNamespace dlNamespace,
               OrderedScheduler scheduler,
               FatalErrorHandler fatalErrorHandler,
               HashedWheelTimer requestTimer) {
        this.clientId = clientId;
        this.dlConfig = dlConfig;
        this.streamManager = streamManager;
        this.name = name;
        this.partition = partition;
        this.status = StreamStatus.UNINITIALIZED;
        this.lastException = new IOException("Fail to write record to stream " + name);
        this.nextAcquireWaitTimeMs = dlConfig.getZKSessionTimeoutMilliseconds() * 3 / 5;
        this.streamConfigProvider = streamConfigProvider;
        this.dlNamespace = dlNamespace;
        this.featureRateLimitDisabled = featureProvider.getFeature(
            ServerFeatureKeys.SERVICE_RATE_LIMIT_DISABLED.name().toLowerCase());
        this.scheduler = scheduler;
        this.serviceTimeoutMs = serverConfig.getServiceTimeoutMs();
        this.streamProbationTimeoutMs = serverConfig.getStreamProbationTimeoutMs();
        this.failFastOnStreamNotReady = dlConfig.getFailFastOnStreamNotReady();
        this.fatalErrorHandler = fatalErrorHandler;
        this.dynConf = streamConf;
        StatsLogger limiterStatsLogger = BroadCastStatsLogger.two(
            streamOpStats.baseScope("stream_limiter"),
            streamOpStats.streamRequestScope(name, "limiter"));
        this.limiter = new StreamRequestLimiter(name, dynConf, limiterStatsLogger, featureRateLimitDisabled);
        this.requestTimer = requestTimer;

        // Stats
        this.streamLogger = streamOpStats.streamRequestStatsLogger(name);
        this.limiterStatLogger = streamOpStats.baseScope("request_limiter");
        this.streamExceptionStatLogger = streamLogger.scope("exceptions");
        this.serviceTimeout = streamOpStats.baseCounter("serviceTimeout");
        StatsLogger streamsStatsLogger = streamOpStats.baseScope("streams");
        this.streamAcquireStat = streamsStatsLogger.getOpStatsLogger("acquire");
        this.pendingOpsCounter = streamOpStats.baseCounter("pending_ops");
        this.unexpectedExceptions = streamOpStats.baseCounter("unexpected_exceptions");
        this.exceptionStatLogger = streamOpStats.requestScope("exceptions");
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public String getStreamName() {
        return name;
    }

    @Override
    public DynamicDistributedLogConfiguration getStreamConfiguration() {
        return dynConf;
    }

    @Override
    public Partition getPartition() {
        return partition;
    }

    private DistributedLogManager openLog(String name) throws IOException {
        Optional<DistributedLogConfiguration> dlConf = Optional.<DistributedLogConfiguration>absent();
        Optional<DynamicDistributedLogConfiguration> dynDlConf = Optional.of(dynConf);
        return dlNamespace.openLog(name, dlConf, dynDlConf);
    }

    // Expensive initialization, only called once per stream.
    @Override
    public void initialize() throws IOException {
        manager = openLog(name);

        // Better to avoid registering the gauge multiple times, so do this in init
        // which only gets called once.
        streamLogger.registerGauge("stream_status", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return StreamStatus.UNINITIALIZED.getCode();
            }
            @Override
            public Number getSample() {
                return status.getCode();
            }
        });

        // Signal initialization is complete, should be last in this method.
        status = StreamStatus.INITIALIZING;
    }

    @Override
    public String toString() {
        return String.format("Stream:%s, %s, %s Status:%s", name, manager, writer, status);
    }

    // schedule stream acquistion
    private void tryAcquireStreamOnce() {
        if (!running) {
            return;
        }

        boolean needAcquire = false;
        boolean checkNextTime = false;
        synchronized (this) {
            switch (this.status) {
            case INITIALIZING:
                streamManager.notifyReleased(this);
                needAcquire = true;
                break;
            case FAILED:
                this.status = StreamStatus.INITIALIZING;
                streamManager.notifyReleased(this);
                needAcquire = true;
                break;
            case BACKOFF:
                // We may end up here after timeout on streamLock. To avoid acquire on every timeout
                // we should only try again if a write has been attempted since the last acquire
                // attempt. If we end up here because the request handler woke us up, the flag will
                // be set and we will try to acquire as intended.
                if (writeSinceLastAcquire) {
                    this.status = StreamStatus.INITIALIZING;
                    streamManager.notifyReleased(this);
                    needAcquire = true;
                } else {
                    checkNextTime = true;
                }
                break;
            default:
                break;
            }
        }
        if (needAcquire) {
            lastAcquireWatch.reset().start();
            acquireStream().addEventListener(new FutureEventListener<Boolean>() {
                @Override
                public void onSuccess(Boolean success) {
                    synchronized (StreamImpl.this) {
                        scheduledAcquireDelayMs = 0L;
                        tryAcquireScheduledFuture = null;
                    }
                    if (!success) {
                        // schedule acquire in nextAcquireWaitTimeMs
                        scheduleTryAcquireOnce(nextAcquireWaitTimeMs);
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    // unhandled exceptions
                    logger.error("Stream {} threw unhandled exception : ", name, cause);
                    setStreamInErrorStatus();
                    requestClose("Unhandled exception");
                }
            });
        } else if (StreamStatus.isUnavailable(status)) {
            // if the stream is unavailable, stop the thread and close the stream
            requestClose("Stream is unavailable anymore");
        } else if (StreamStatus.INITIALIZED != status && lastAcquireWatch.elapsed(TimeUnit.HOURS) > 2) {
            // if the stream isn't in initialized state and no writes coming in, then close the stream
            requestClose("Stream not used anymore");
        } else if (checkNextTime) {
            synchronized (StreamImpl.this) {
                scheduledAcquireDelayMs = 0L;
                tryAcquireScheduledFuture = null;
            }
            // schedule acquire in nextAcquireWaitTimeMs
            scheduleTryAcquireOnce(nextAcquireWaitTimeMs);
        }
    }

    private synchronized void scheduleTryAcquireOnce(long delayMs) {
        if (null != tryAcquireScheduledFuture) {
            if (delayMs <= 0) {
                if (scheduledAcquireDelayMs <= 0L ||
                        (scheduledAcquireDelayMs > 0L
                                && !tryAcquireScheduledFuture.cancel(false))) {
                    return;
                }
                // if the scheduled one could be cancelled, re-submit one
            } else {
                return;
            }
        }
        tryAcquireScheduledFuture = schedule(new Runnable() {
            @Override
            public void run() {
                tryAcquireStreamOnce();
            }
        }, delayMs);
        scheduledAcquireDelayMs = delayMs;
    }

    @Override
    public void start() {
        scheduleTryAcquireOnce(0);
    }

    ScheduledFuture<?> schedule(Runnable runnable, long delayMs) {
        if (!running) {
            return null;
        }
        try {
            return scheduler.schedule(name, runnable, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            logger.error("Failed to schedule task {} in {} ms : ",
                    new Object[] { runnable, delayMs, ree });
            return null;
        }
    }

    void scheduleTimeout(final StreamOp op) {
        final Timeout timeout = requestTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                if (!timeout.isCancelled()) {
                    serviceTimeout.inc();
                    handleServiceTimeout("Operation " + op.getClass().getName() + " timeout");
                }
            }
        }, serviceTimeoutMs, TimeUnit.MILLISECONDS);
        op.responseHeader().ensure(new Function0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                timeout.cancel();
                return null;
            }
        });
    }

    /**
     * Close the stream and schedule cache eviction at some point in the future.
     * We delay this as a way to place the stream in a probationary state--cached
     * in the proxy but unusable.
     * This mechanism helps the cluster adapt to situations where a proxy has
     * persistent connectivity/availability issues, because it keeps an affected
     * stream off the proxy for a period of time, hopefully long enough for the
     * issues to be resolved, or for whoop to kick in and kill the shard.
     */
    synchronized void handleServiceTimeout(String reason) {
        if (StreamStatus.isUnavailable(status)) {
            return;
        }
        // Mark stream in error state
        setStreamInErrorStatus();

        // Async close request, and schedule eviction when its done.
        Future<Void> closeFuture = requestClose(reason, false /* dont remove */);
        closeFuture.onSuccess(new AbstractFunction1<Void, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Void result) {
                streamManager.scheduleRemoval(StreamImpl.this, streamProbationTimeoutMs);
                return BoxedUnit.UNIT;
            }
        });
    }

    /**
     * Execute the StreamOp. If reacquire is needed, this may initiate reacquire and queue the op for
     * execution once complete.
     *
     * @param op
     *          stream operation to execute.
     */
    @Override
    public void submit(StreamOp op) {
        // Let stream acquire thread know a write has been attempted.
        writeSinceLastAcquire = true;

        try {
            limiter.apply(op);
        } catch (OverCapacityException ex) {
            op.fail(ex);
            return;
        }

        // Timeout stream op if requested.
        if (serviceTimeoutMs > 0) {
            scheduleTimeout(op);
        }

        boolean notifyAcquireThread = false;
        boolean completeOpNow = false;
        boolean success = true;
        if (StreamStatus.isUnavailable(status)) {
            // Stream is closed, fail the op immediately
            op.fail(new StreamUnavailableException("Stream " + name + " is closed."));
            return;
        } if (StreamStatus.INITIALIZED == status && writer != null) {
            completeOpNow = true;
            success = true;
        } else {
            synchronized (this) {
                if (StreamStatus.isUnavailable(status)) {
                    // complete the write op as {@link #executeOp(op, success)} will handle closed case.
                    completeOpNow = true;
                    success = true;
                } if (StreamStatus.INITIALIZED == status) {
                    completeOpNow = true;
                    success = true;
                } else if (StreamStatus.BACKOFF == status &&
                        lastAcquireFailureWatch.elapsed(TimeUnit.MILLISECONDS) < nextAcquireWaitTimeMs) {
                    completeOpNow = true;
                    success = false;
                } else if (failFastOnStreamNotReady) {
                    notifyAcquireThread = true;
                    completeOpNow = false;
                    success = false;
                    op.fail(new StreamNotReadyException("Stream " + name + " is not ready; status = " + status));
                } else { // closing & initializing
                    notifyAcquireThread = true;
                    pendingOps.add(op);
                    pendingOpsCounter.inc();
                    if (1 == pendingOps.size()) {
                        if (op instanceof HeartbeatOp) {
                            ((HeartbeatOp) op).setWriteControlRecord(true);
                        }
                    }
                }
            }
        }
        if (notifyAcquireThread && !suspended) {
            scheduleTryAcquireOnce(0L);
        }
        if (completeOpNow) {
            executeOp(op, success);
        }
    }

    /**
     * Execute the <i>op</i> immediately.
     *
     * @param op
     *          stream operation to execute.
     * @param success
     *          whether the operation is success or not.
     */
    void executeOp(StreamOp op, boolean success) {
        closeLock.readLock().lock();
        try {
            if (StreamStatus.isUnavailable(status)) {
                op.fail(new StreamUnavailableException("Stream " + name + " is closed."));
                return;
            }
            doExecuteOp(op, success);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private void doExecuteOp(final StreamOp op, boolean success) {
        final AsyncLogWriter writer;
        final Throwable lastException;
        synchronized (this) {
            writer = this.writer;
            lastException = this.lastException;
        }
        if (null != writer && success) {
            op.execute(writer, sequencer, txnLock)
                    .addEventListener(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    // nop
                }
                @Override
                public void onFailure(Throwable cause) {
                    boolean countAsException = true;
                    if (cause instanceof DLException) {
                        final DLException dle = (DLException) cause;
                        switch (dle.getCode()) {
                        case FOUND:
                            assert(cause instanceof OwnershipAcquireFailedException);
                            countAsException = false;
                            handleOwnershipAcquireFailedException(op, (OwnershipAcquireFailedException) cause);
                            break;
                        case ALREADY_CLOSED:
                            assert(cause instanceof AlreadyClosedException);
                            op.fail(cause);
                            handleAlreadyClosedException((AlreadyClosedException) cause);
                            break;
                        // exceptions that mostly from client (e.g. too large record)
                        case NOT_IMPLEMENTED:
                        case METADATA_EXCEPTION:
                        case LOG_EMPTY:
                        case LOG_NOT_FOUND:
                        case TRUNCATED_TRANSACTION:
                        case END_OF_STREAM:
                        case TRANSACTION_OUT_OF_ORDER:
                        case INVALID_STREAM_NAME:
                        case TOO_LARGE_RECORD:
                        case STREAM_NOT_READY:
                        case OVER_CAPACITY:
                            op.fail(cause);
                            break;
                        // exceptions that *could* / *might* be recovered by creating a new writer
                        default:
                            handleRecoverableDLException(op, cause);
                            break;
                        }
                    } else {
                        handleUnknownException(op, cause);
                    }
                    if (countAsException) {
                        countException(cause, streamExceptionStatLogger);
                    }
                }
            });
        } else {
            op.fail(lastException);
        }
    }

    /**
     * Handle recoverable dl exception.
     *
     * @param op
     *          stream operation executing
     * @param cause
     *          exception received when executing <i>op</i>
     */
    private void handleRecoverableDLException(StreamOp op, final Throwable cause) {
        AsyncLogWriter oldWriter = null;
        boolean statusChanged = false;
        synchronized (this) {
            if (StreamStatus.INITIALIZED == status) {
                oldWriter = setStreamStatus(StreamStatus.FAILED, StreamStatus.INITIALIZED,
                        null, null, cause);
                statusChanged = true;
            }
        }
        if (statusChanged) {
            Abortables.asyncAbort(oldWriter, false);
            logger.error("Failed to write data into stream {} : ", name, cause);
            scheduleTryAcquireOnce(0L);
        }
        op.fail(cause);
    }

    /**
     * Handle unknown exception when executing <i>op</i>.
     *
     * @param op
     *          stream operation executing
     * @param cause
     *          exception received when executing <i>op</i>
     */
    private void handleUnknownException(StreamOp op, final Throwable cause) {
        AsyncLogWriter oldWriter = null;
        boolean statusChanged = false;
        synchronized (this) {
            if (StreamStatus.INITIALIZED == status) {
                oldWriter = setStreamStatus(StreamStatus.FAILED, StreamStatus.INITIALIZED,
                        null, null, cause);
                statusChanged = true;
            }
        }
        if (statusChanged) {
            Abortables.asyncAbort(oldWriter, false);
            logger.error("Failed to write data into stream {} : ", name, cause);
            scheduleTryAcquireOnce(0L);
        }
        op.fail(cause);
    }

    /**
     * Handle losing ownership during executing <i>op</i>.
     *
     * @param op
     *          stream operation executing
     * @param oafe
     *          the ownership exception received when executing <i>op</i>
     */
    private void handleOwnershipAcquireFailedException(StreamOp op, final OwnershipAcquireFailedException oafe) {
        logger.warn("Failed to write data into stream {} because stream is acquired by {} : {}",
                new Object[]{name, oafe.getCurrentOwner(), oafe.getMessage()});
        AsyncLogWriter oldWriter = null;
        boolean statusChanged = false;
        synchronized (this) {
            if (StreamStatus.INITIALIZED == status) {
                oldWriter =
                    setStreamStatus(StreamStatus.BACKOFF, StreamStatus.INITIALIZED,
                            null, oafe.getCurrentOwner(), oafe);
                statusChanged = true;
            }
        }
        if (statusChanged) {
            Abortables.asyncAbort(oldWriter, false);
            scheduleTryAcquireOnce(nextAcquireWaitTimeMs);
        }
        op.fail(oafe);
    }

    /**
     * Handling already closed exception.
     */
    private void handleAlreadyClosedException(AlreadyClosedException ace) {
        unexpectedExceptions.inc();
        logger.error("Encountered unexpected exception when writing data into stream {} : ", name, ace);
        fatalErrorHandler.notifyFatalError();
    }

    void countException(Throwable t, StatsLogger streamExceptionLogger) {
        String exceptionName = null == t ? "null" : t.getClass().getName();
        Counter counter = exceptionCounters.get(exceptionName);
        if (null == counter) {
            counter = exceptionStatLogger.getCounter(exceptionName);
            Counter oldCounter = exceptionCounters.putIfAbsent(exceptionName, counter);
            if (null != oldCounter) {
                counter = oldCounter;
            }
        }
        counter.inc();
        streamExceptionLogger.getCounter(exceptionName).inc();
    }

    Future<Boolean> acquireStream() {
        // Reset this flag so the acquire thread knows whether re-acquire is needed.
        writeSinceLastAcquire = false;

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Promise<Boolean> acquirePromise = new Promise<Boolean>();
        manager.openAsyncLogWriter().addEventListener(FutureUtils.OrderedFutureEventListener.of(new FutureEventListener<AsyncLogWriter>() {

            @Override
            public void onSuccess(AsyncLogWriter w) {
                synchronized (txnLock) {
                    sequencer.setLastId(w.getLastTxId());
                }
                AsyncLogWriter oldWriter;
                Queue<StreamOp> oldPendingOps;
                boolean success;
                synchronized (StreamImpl.this) {
                    oldWriter = setStreamStatus(StreamStatus.INITIALIZED,
                            StreamStatus.INITIALIZING, w, null, null);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = true;
                }
                // check if the stream is allowed to be acquired
                if (!streamManager.allowAcquire(StreamImpl.this)) {
                    if (null != oldWriter) {
                        Abortables.asyncAbort(oldWriter, true);
                    }
                    int maxAcquiredPartitions = dynConf.getMaxAcquiredPartitionsPerProxy();
                    StreamUnavailableException sue = new StreamUnavailableException("Stream " + partition.getStream()
                            + " is not allowed to acquire more than " + maxAcquiredPartitions + " partitions");
                    countException(sue, exceptionStatLogger);
                    logger.error("Failed to acquire stream {} because it is unavailable : {}",
                            name, sue.getMessage());
                    synchronized (this) {
                        oldWriter = setStreamStatus(StreamStatus.ERROR,
                                StreamStatus.INITIALIZED, null, null, sue);
                        // we don't switch the pending ops since they are already switched
                        // when setting the status to initialized
                        success = false;
                    }
                }
                processPendingRequestsAfterOpen(success, oldWriter, oldPendingOps);
            }

            @Override
            public void onFailure(Throwable cause) {
                AsyncLogWriter oldWriter;
                Queue<StreamOp> oldPendingOps;
                boolean success;
                if (cause instanceof AlreadyClosedException) {
                    countException(cause, streamExceptionStatLogger);
                    handleAlreadyClosedException((AlreadyClosedException) cause);
                    return;
                } else if (cause instanceof OwnershipAcquireFailedException) {
                    OwnershipAcquireFailedException oafe = (OwnershipAcquireFailedException) cause;
                    logger.warn("Failed to acquire stream ownership for {}, current owner is {} : {}",
                            new Object[]{name, oafe.getCurrentOwner(), oafe.getMessage()});
                    synchronized (StreamImpl.this) {
                        oldWriter = setStreamStatus(StreamStatus.BACKOFF,
                                StreamStatus.INITIALIZING, null, oafe.getCurrentOwner(), oafe);
                        oldPendingOps = pendingOps;
                        pendingOps = new ArrayDeque<StreamOp>();
                        success = false;
                    }
                } else if (cause instanceof InvalidStreamNameException) {
                    InvalidStreamNameException isne = (InvalidStreamNameException) cause;
                    countException(isne, streamExceptionStatLogger);
                    logger.error("Failed to acquire stream {} due to its name is invalid", name);
                    synchronized (StreamImpl.this) {
                        oldWriter = setStreamStatus(StreamStatus.ERROR,
                                StreamStatus.INITIALIZING, null, null, isne);
                        oldPendingOps = pendingOps;
                        pendingOps = new ArrayDeque<StreamOp>();
                        success = false;
                    }
                } else {
                    countException(cause, streamExceptionStatLogger);
                    logger.error("Failed to initialize stream {} : ", name, cause);
                    synchronized (StreamImpl.this) {
                        oldWriter = setStreamStatus(StreamStatus.FAILED,
                                StreamStatus.INITIALIZING, null, null, cause);
                        oldPendingOps = pendingOps;
                        pendingOps = new ArrayDeque<StreamOp>();
                        success = false;
                    }
                }
                processPendingRequestsAfterOpen(success, oldWriter, oldPendingOps);
            }

            void processPendingRequestsAfterOpen(boolean success,
                                                 AsyncLogWriter oldWriter,
                                                 Queue<StreamOp> oldPendingOps) {
                if (success) {
                    streamAcquireStat.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                } else {
                    streamAcquireStat.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                }
                for (StreamOp op : oldPendingOps) {
                    executeOp(op, success);
                    pendingOpsCounter.dec();
                }
                Abortables.asyncAbort(oldWriter, true);
                FutureUtils.setValue(acquirePromise, success);
            }
        }, scheduler, getStreamName()));
        return acquirePromise;
    }

    synchronized void setStreamInErrorStatus() {
        if (StreamStatus.CLOSING == status || StreamStatus.CLOSED == status) {
            return;
        }
        this.status = StreamStatus.ERROR;
    }

    /**
     * Update the stream status. The changes are only applied when there isn't status changed.
     *
     * @param newStatus
     *          new status
     * @param oldStatus
     *          old status
     * @param writer
     *          new log writer
     * @param owner
     *          new owner
     * @param t
     *          new exception
     * @return old writer if it exists
     */
    synchronized AsyncLogWriter setStreamStatus(StreamStatus newStatus,
                                                StreamStatus oldStatus,
                                                AsyncLogWriter writer,
                                                String owner,
                                                Throwable t) {
        if (oldStatus != this.status) {
            logger.info("Stream {} status already changed from {} -> {} when trying to change it to {}",
                    new Object[] { name, oldStatus, this.status, newStatus });
            return null;
        }

        AsyncLogWriter oldWriter = this.writer;
        this.writer = writer;
        if (null != owner && owner.equals(clientId)) {
            unexpectedExceptions.inc();
            logger.error("I am waiting myself {} to release lock on stream {}, so have to shut myself down :",
                         new Object[] { owner, name, t });
            // I lost the ownership but left a lock over zookeeper
            // I should not ask client to redirect to myself again as I can't handle it :(
            // shutdown myself
            fatalErrorHandler.notifyFatalError();
            this.owner = null;
        } else {
            this.owner = owner;
        }
        this.lastException = t;
        this.status = newStatus;
        if (StreamStatus.BACKOFF == newStatus && null != owner) {
            // start failure watch
            this.lastAcquireFailureWatch.reset().start();
        }
        if (StreamStatus.INITIALIZED == newStatus) {
            streamManager.notifyAcquired(this);
            logger.info("Inserted acquired stream {} -> writer {}", name, this);
        } else {
            streamManager.notifyReleased(this);
            logger.info("Removed acquired stream {} -> writer {}", name, this);
        }
        return oldWriter;
    }

    void close(DistributedLogManager dlm) {
        if (null != dlm) {
            try {
                dlm.close();
            } catch (IOException ioe) {
                logger.warn("Failed to close dlm for {} : ", ioe);
            }
        }
    }

    @Override
    public Future<Void> requestClose(String reason) {
        return requestClose(reason, true);
    }

    Future<Void> requestClose(String reason, boolean uncache) {
        final boolean abort;
        closeLock.writeLock().lock();
        try {
            if (StreamStatus.CLOSING == status ||
                StreamStatus.CLOSED == status) {
                return closePromise;
            }
            logger.info("Request to close stream {} : {}", getStreamName(), reason);
            // if the stream isn't closed from INITIALIZED state, we abort the stream instead of closing it.
            abort = StreamStatus.INITIALIZED != status;
            status = StreamStatus.CLOSING;
            streamManager.notifyReleased(this);
        } finally {
            closeLock.writeLock().unlock();
        }
        // we will fail the requests that are coming in between closing and closed only
        // after the async writer is closed. so we could clear up the lock before redirect
        // them.
        close(abort);
        if (uncache) {
            closePromise.onSuccess(new AbstractFunction1<Void, BoxedUnit>() {
                @Override
                public BoxedUnit apply(Void result) {
                    if (streamManager.notifyRemoved(StreamImpl.this)) {
                        logger.info("Removed cached stream {} after closed.", name);
                    }
                    return BoxedUnit.UNIT;
                }
            });
        }
        return closePromise;
    }

    @Override
    public void delete() throws IOException {
        if (null != writer) {
            Utils.close(writer);
            synchronized (this) {
                writer = null;
                lastException = new StreamUnavailableException("Stream was deleted");
            }
        }
        if (null == manager) {
            throw new UnexpectedException("No stream " + name + " to delete");
        }
        manager.delete();
    }

    /**
     * Shouldn't call close directly. The callers should call #requestClose instead
     *
     * @param shouldAbort shall we abort the stream instead of closing
     */
    private Future<Void> close(boolean shouldAbort) {
        boolean abort;
        closeLock.writeLock().lock();
        try {
            if (StreamStatus.CLOSED == status) {
                return closePromise;
            }
            abort = shouldAbort || (StreamStatus.INITIALIZED != status && StreamStatus.CLOSING != status);
            status = StreamStatus.CLOSED;
            streamManager.notifyReleased(this);
        } finally {
            closeLock.writeLock().unlock();
        }
        logger.info("Closing stream {} ...", name);
        running = false;
        // stop any outstanding ownership acquire actions first
        synchronized (this) {
            if (null != tryAcquireScheduledFuture) {
                tryAcquireScheduledFuture.cancel(true);
            }
        }
        logger.info("Stopped threads of stream {}.", name);
        // Close the writers to release the locks before failing the requests
        Future<Void> closeWriterFuture;
        if (abort) {
            closeWriterFuture = Abortables.asyncAbort(writer, true);
        } else {
            closeWriterFuture = Utils.asyncClose(writer, true);
        }
        // close the manager and error out pending requests after close writer
        closeWriterFuture.addEventListener(FutureUtils.OrderedFutureEventListener.of(
                new FutureEventListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        closeManagerAndErrorOutPendingRequests();
                        FutureUtils.setValue(closePromise, null);
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        closeManagerAndErrorOutPendingRequests();
                        FutureUtils.setValue(closePromise, null);
                    }
                }, scheduler, name));
        return closePromise;
    }

    private void closeManagerAndErrorOutPendingRequests() {
        close(manager);
        // Failed the pending requests.
        Queue<StreamOp> oldPendingOps;
        synchronized (this) {
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<StreamOp>();
        }
        StreamUnavailableException closingException =
                new StreamUnavailableException("Stream " + name + " is closed.");
        for (StreamOp op : oldPendingOps) {
            op.fail(closingException);
            pendingOpsCounter.dec();
        }
        limiter.close();
        logger.info("Closed stream {}.", name);
    }

    // Test-only apis

    @VisibleForTesting
    public StreamImpl suspendAcquiring() {
        suspended = true;
        return this;
    }

    @VisibleForTesting
    public StreamImpl resumeAcquiring() {
        suspended = false;
        scheduleTryAcquireOnce(0L);
        return this;
    }

    @VisibleForTesting
    public int numPendingOps() {
        Queue<StreamOp> queue = pendingOps;
        return null == queue ? 0 : queue.size();
    }

    @VisibleForTesting
    public StreamStatus getStatus() {
        return status;
    }

    @VisibleForTesting
    public void setStatus(StreamStatus status) {
        this.status = status;
    }

    @VisibleForTesting
    public AsyncLogWriter getWriter() {
        return writer;
    }

    @VisibleForTesting
    public DistributedLogManager getManager() {
        return manager;
    }

    @VisibleForTesting
    public Throwable getLastException() {
        return lastException;
    }

    @VisibleForTesting
    public Future<Void> getCloseFuture() {
        return closePromise;
    }
}
