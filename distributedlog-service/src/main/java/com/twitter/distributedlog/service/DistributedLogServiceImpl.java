package com.twitter.distributedlog.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.RegionUnavailableException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.exceptions.StreamNotReadyException;
import com.twitter.distributedlog.exceptions.StreamUnavailableException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.feature.AbstractFeatureProvider;
import com.twitter.distributedlog.limiter.ChainedRequestLimiter;
import com.twitter.distributedlog.limiter.RequestLimiter;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.stream.BulkWriteOp;
import com.twitter.distributedlog.service.stream.DeleteOp;
import com.twitter.distributedlog.service.stream.HeartbeatOp;
import com.twitter.distributedlog.service.stream.ReleaseOp;
import com.twitter.distributedlog.service.stream.StreamManager;
import com.twitter.distributedlog.service.stream.StreamOpStats;
import com.twitter.distributedlog.service.stream.StreamOp;
import com.twitter.distributedlog.service.stream.TruncateOp;
import com.twitter.distributedlog.service.stream.WriteOp;
import com.twitter.distributedlog.service.stream.limiter.BpsHardLimiter;
import com.twitter.distributedlog.service.stream.limiter.BpsSoftLimiter;
import com.twitter.distributedlog.service.stream.limiter.DynamicRequestLimiter;
import com.twitter.distributedlog.service.stream.limiter.RpsHardLimiter;
import com.twitter.distributedlog.service.stream.limiter.RpsSoftLimiter;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.ClientInfo;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.HeartbeatOptions;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.ServerStatus;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.distributedlog.util.TimeSequencer;
import com.twitter.util.Await;
import com.twitter.util.Duration;
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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class DistributedLogServiceImpl implements DistributedLogService.ServiceIface, StreamManager {
    static final Logger logger = LoggerFactory.getLogger(DistributedLogServiceImpl.class);

    static enum StreamStatus {
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

    public static enum ServerMode {
        DURABLE,
        MEM
    }

    public Future<Void> doDeleteAndRemoveAsync(final String stream) {
        Stream s = streams.get(stream);
        if (null == s) {
            logger.warn("No stream {} to delete.", stream);
            return Future.exception(new UnexpectedException("No stream " + stream + " to delete."));
        } else {
            Future<Void> result;
            logger.info("Deleting stream {}, {}", stream, s);
            try {
                s.delete();
                result = s.requestClose("Stream Deleted");
            } catch (IOException e) {
                logger.error("Failed on removing stream {} : ", stream, e);
                result = Future.exception(e);
            }
            return result;
        }
    }

    /**
     * Must be enqueued to an executor to avoid deadlocks (close and execute-op both
     * try to acquire the same read-write lock).
     */
    @Override
    public Future<Void> deleteAndRemoveAsync(final String stream) {
        final Promise<Void> result = new Promise<Void>();
        java.util.concurrent.Future<?> scheduleFuture = schedule(new Runnable() {
            @Override
            public void run() {
                result.become(doDeleteAndRemoveAsync(stream));
            }
        }, 0);
        if (null == scheduleFuture) {
            return Future.exception(
                    new ServiceUnavailableException("Couldn't schedule a delete task."));
        }
        return result;
    }

    public Future<Void> doCloseAndRemoveAsync(final String stream) {
        Stream s = streams.get(stream);
        if (null == s) {
            logger.info("No stream {} to release.", stream);
            return Future.value(null);
        } else {
            return s.requestClose("release ownership");
        }
    }

    /**
     * Must be enqueued to an executor to avoid deadlocks (close and execute-op both
     * try to acquire the same read-write lock).
     */
    @Override
    public Future<Void> closeAndRemoveAsync(final String stream) {
        final Promise<Void> releasePromise = new Promise<Void>();
        java.util.concurrent.Future<?> scheduleFuture = schedule(new Runnable() {
            @Override
            public void run() {
                releasePromise.become(doCloseAndRemoveAsync(stream));
            }
        }, 0);
        if (null == scheduleFuture) {
            return Future.exception(
                    new ServiceUnavailableException("Couldn't schedule a release task."));
        }
        return releasePromise;
    }

    WriteOp newWriteOp(String stream, ByteBuffer data) {
        return new WriteOp(stream, data, statsLogger, perStreamStatsLogger, serverConfig, dlsnVersion);
    }

    protected class Stream extends Thread {
        final String name;

        // lock
        final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

        DistributedLogManager manager;

        volatile AsyncLogWriter writer;
        volatile StreamStatus status;
        volatile String owner;
        volatile Throwable lastException;
        volatile boolean running = true;
        boolean suspended = false;
        private volatile Queue<StreamOp> pendingOps = new ArrayDeque<StreamOp>();
        private final Promise<Void> closePromise = new Promise<Void>();

        // A write has been attempted since the last stream acquire.
        private volatile boolean writeSinceLastAcquire = false;

        final Object streamLock = new Object();
        final Object txnLock = new Object();
        final TimeSequencer sequencer = new TimeSequencer();
        // last acquire time
        final Stopwatch lastAcquireWatch = Stopwatch.createUnstarted();
        // last acquire failure time
        final Stopwatch lastAcquireFailureWatch = Stopwatch.createUnstarted();
        final long nextAcquireWaitTimeMs;
        final DynamicRequestLimiter limiter;
        final DynamicDistributedLogConfiguration dynConf;

        // Stats
        final StatsLogger streamLogger;
        final StatsLogger exceptionStatLogger;

        // Since we may create and discard streams at initialization if there's a race,
        // must not do any expensive intialization here (particularly any locking or
        // significant resource allocation etc.).
        Stream(final String name) {
            super("Stream-" + name);
            this.name = name;
            this.status = StreamStatus.UNINITIALIZED;
            this.streamLogger = streamOpStats.streamRequestStatsLogger(name);
            this.exceptionStatLogger = streamLogger.scope("exceptions");
            this.lastException = new IOException("Fail to write record to stream " + name);
            this.nextAcquireWaitTimeMs = dlConfig.getZKSessionTimeoutMilliseconds() * 3 / 5;
            this.dynConf = getDynamicConfiguration();
            this.limiter = new DynamicRequestLimiter<StreamOp>(dynConf, limiterStatLogger) {
                @Override
                public RequestLimiter<StreamOp> build() {
                    ChainedRequestLimiter.Builder<StreamOp> builder = new ChainedRequestLimiter.Builder<StreamOp>();
                    builder.addLimiter(new RpsSoftLimiter(dynConf.getRpsSoftWriteLimit(), streamLogger));
                    builder.addLimiter(new RpsHardLimiter(dynConf.getRpsHardWriteLimit(), streamLogger, name));
                    builder.addLimiter(new BpsSoftLimiter(dynConf.getBpsSoftWriteLimit(), streamLogger));
                    builder.addLimiter(new BpsHardLimiter(dynConf.getBpsHardWriteLimit(), streamLogger, name));
                    builder.statsLogger(limiterStatLogger);
                    return builder.build();
                }
            };
            setDaemon(true);
        }

        private DynamicDistributedLogConfiguration getDynamicConfiguration() {
            Optional<DynamicDistributedLogConfiguration> dynDlConf =
                    streamConfigProvider.getDynamicStreamConfig(name);
            if (dynDlConf.isPresent()) {
                return dynDlConf.get();
            } else {
                return ConfUtils.getConstDynConf(dlConfig);
            }
        }

        private DistributedLogManager openLog(String name) throws IOException {
            Optional<DistributedLogConfiguration> dlConf = Optional.<DistributedLogConfiguration>absent();
            Optional<DynamicDistributedLogConfiguration> dynDlConf = Optional.of(dynConf);
            return dlNamespace.openLog(name, dlConf, dynDlConf);
        }

        // Expensive initialization, only called once per stream.
        public Stream initialize() throws IOException {
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
            return this;
        }

        @VisibleForTesting
        Stream suspendAcquiring() {
            synchronized (streamLock) {
                suspended = true;
            }
            return this;
        }

        @VisibleForTesting
        Stream resumeAcquiring() {
            synchronized (streamLock) {
                suspended = false;
                streamLock.notifyAll();
            }
            return this;
        }

        int numPendingOps() {
            Queue<StreamOp> queue = pendingOps;
            return null == queue ? 0 : queue.size();
        }

        StreamStatus getStatus() {
            return status;
        }

        @Override
        public String toString() {
            return String.format("Stream:%s, %s, %s Status:%s", name, manager, writer, status);
        }

        @Override
        public void run() {
            try {
                boolean shouldClose = false;
                String closeReason = "";
                while (running) {
                    synchronized (streamLock) {
                        while (suspended && running) {
                            try {
                                streamLock.wait();
                            } catch (InterruptedException ie) {
                                // interrupted on waiting
                            }
                        }
                    }

                    if (!running) {
                        break;
                    }

                    boolean needAcquire = false;
                    synchronized (this) {
                        switch (this.status) {
                        case INITIALIZING:
                            acquiredStreams.remove(name, this);
                            needAcquire = true;
                            break;
                        case FAILED:
                            this.status = StreamStatus.INITIALIZING;
                            acquiredStreams.remove(name, this);
                            needAcquire = true;
                            break;
                        case BACKOFF:
                            // We may end up here after timeout on streamLock. To avoid acquire on every timeout
                            // we should only try again if a write has been attempted since the last acquire
                            // attempt. If we end up here because the request handler woke us up, the flag will
                            // be set and we will try to acquire as intended.
                            if (writeSinceLastAcquire) {
                                this.status = StreamStatus.INITIALIZING;
                                acquiredStreams.remove(name, this);
                                needAcquire = true;
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    if (needAcquire) {
                        lastAcquireWatch.reset().start();
                        acquireStream();
                    } else if (StreamStatus.isUnavailable(status)) {
                        // if the stream is unavailable, stop the thread and close the stream
                        shouldClose = true;
                        closeReason = "Stream is unavailable anymore";
                        break;
                    } else if (StreamStatus.INITIALIZED != status && lastAcquireWatch.elapsed(TimeUnit.HOURS) > 2) {
                        // if the stream isn't in initialized state and no writes coming in, then close the stream
                        shouldClose = true;
                        closeReason = "Stream not used anymore";
                        break;
                    }
                    try {
                        synchronized (streamLock) {
                            streamLock.wait(nextAcquireWaitTimeMs);
                        }
                    } catch (InterruptedException e) {
                        // interrupted
                    }
                }
                if (shouldClose) {
                    requestClose(closeReason);
                }
            } catch (Exception ex) {
                logger.error("Stream thread {} threw unhandled exception : ", name, ex);
                setStreamInErrorStatus();
                requestClose("Unhandled exception");
            }
        }

        void scheduleTimeout(final StreamOp op) {
            final Timeout timeout = dlTimer.newTimeout(new TimerTask() {
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

        void scheduleRemoval(final String streamName, long delayMs) {
            logger.info("Scheduling removal of stream {} from cache after {} sec.", name, delayMs);
            final Timeout timeout = dlTimer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    if (streams.remove(streamName, Stream.this)) {
                        logger.info("Removed cached stream {} after probation.", name);
                    } else {
                        logger.info("Cached stream {} already removed.", name);
                    }
                }
            }, delayMs, TimeUnit.MILLISECONDS);
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
                    scheduleRemoval(name, streamProbationTimeoutMs);
                    return BoxedUnit.UNIT;
                }
            });
        }

        /**
         * Wait for stream being re-acquired if needed. Otherwise, execute the <i>op</i> immediately.
         *
         * @param op
         *          stream operation to execute.
         */
        void waitIfNeededAndWrite(StreamOp op) {
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

            op.responseHeader().addEventListener(new FutureEventListener<ResponseHeader>() {
                @Override
                public void onSuccess(ResponseHeader header) {
                    if (header.getLocation() != null || header.getCode() == StatusCode.FOUND) {
                        redirects.inc();
                    }
                    countStatusCode(header.getCode());
                }
                @Override
                public void onFailure(Throwable cause) {
                }
            });

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
                        pendingOpsCounter.incrementAndGet();
                        if (1 == pendingOps.size()) {
                            if (op instanceof HeartbeatOp) {
                                ((HeartbeatOp) op).setWriteControlRecord(true);
                            }
                        }
                    }
                }
            }
            if (notifyAcquireThread) {
                notifyStreamLockWaiters();
            }
            if (completeOpNow) {
                executeOp(op, success);
            }
        }

        void notifyStreamLockWaiters() {
            synchronized (streamLock) {
                streamLock.notifyAll();
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
                            countException(cause, exceptionStatLogger);
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
                abort(oldWriter);
                logger.error("Failed to write data into stream {} : ", name, cause);
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
                abort(oldWriter);
                logger.error("Failed to write data into stream {} : ", name, cause);
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
                        new Object[] { name, oafe.getCurrentOwner(), oafe.getMessage() });
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
                abort(oldWriter);
            }
            op.fail(oafe);
        }

        /**
         * Handling already closed exception.
         */
        private void handleAlreadyClosedException(AlreadyClosedException ace) {
            unexpectedExceptions.inc();
            logger.error("Encountered unexpected exception when writing data into stream {} : ", name, ace);
            triggerShutdown();
        }

        void acquireStream() {
            // Reset this flag so the acquire thread knows whether re-acquire is needed.
            writeSinceLastAcquire = false;

            Queue<StreamOp> oldPendingOps;
            boolean success;
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                AsyncLogWriter w = manager.startAsyncLogSegmentNonPartitioned();
                synchronized (txnLock) {
                    sequencer.setLastId(w.getLastTxId());
                }
                synchronized (this) {
                    setStreamStatus(StreamStatus.INITIALIZED,
                            StreamStatus.INITIALIZING, w, null, null);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = true;
                }
            } catch (final AlreadyClosedException ace) {
                countException(ace, exceptionStatLogger);
                handleAlreadyClosedException(ace);
                return;
            } catch (final OwnershipAcquireFailedException oafe) {
                logger.warn("Failed to acquire stream ownership for {}, current owner is {} : {}",
                            new Object[] { name, oafe.getCurrentOwner(), oafe.getMessage() });
                synchronized (this) {
                    setStreamStatus(StreamStatus.BACKOFF,
                            StreamStatus.INITIALIZING, null, oafe.getCurrentOwner(), oafe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            } catch (final IOException ioe) {
                countException(ioe, exceptionStatLogger);
                logger.error("Failed to initialize stream {} : ", name, ioe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED,
                            StreamStatus.INITIALIZING, null, null, ioe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            }
            if (success) {
                streamAcquireStat.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            } else {
                streamAcquireStat.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            }
            for (StreamOp op : oldPendingOps) {
                executeOp(op, success);
                pendingOpsCounter.decrementAndGet();
            }
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
                triggerShutdown();
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
                acquiredStreams.put(name, this);
                logger.info("Inserted acquired stream {} -> writer {}", name, this);
            } else {
                acquiredStreams.remove(name, this);
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

        void close(AsyncLogWriter writer) {
            if (null != writer) {
                try {
                    writer.close();
                } catch (IOException ioe) {
                    logger.warn("Failed to close async log writer for {} : ", ioe);
                }
            }
        }

        void abort(AsyncLogWriter writer) {
            if (null != writer) {
                try {
                    writer.abort();
                } catch (IOException e) {
                    logger.warn("Failed to abort async log writer for {} : ", e);
                }
            }
        }

        Future<Void> requestClose(String reason) {
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
                logger.info("Request to close stream {} : {}", getName(), reason);
                // if the stream isn't closed from INITIALIZED state, we abort the stream instead of closing it.
                abort = StreamStatus.INITIALIZED != status;
                status = StreamStatus.CLOSING;
                acquiredStreams.remove(name, this);
            } finally {
                closeLock.writeLock().unlock();
            }
            // we will fail the requests that are coming in between closing and closed only
            // after the async writer is closed. so we could clear up the lock before redirect
            // them.
            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        close(abort);
                        closePromise.setValue(null);
                    }
                });
            } catch (RejectedExecutionException ree) {
                logger.warn("Failed to schedule closing stream {}, close it directly : ", name, ree);
                close(abort);
                closePromise.setValue(null);
            }
            if (uncache) {
                closePromise.onSuccess(new AbstractFunction1<Void, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(Void result) {
                        if (streams.remove(name, Stream.this)) {
                            logger.info("Removed cached stream {} after closed.", name);
                        }
                        return BoxedUnit.UNIT;
                    }
                });
            }
            return closePromise;
        }

        private void delete() throws IOException {
            if (null != writer) {
                writer.close();
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
        private void close(boolean shouldAbort) {
            boolean abort;
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSED == status) {
                    return;
                }
                abort = shouldAbort || (StreamStatus.INITIALIZED != status && StreamStatus.CLOSING != status);
                status = StreamStatus.CLOSED;
                acquiredStreams.remove(name, this);
            } finally {
                closeLock.writeLock().unlock();
            }
            logger.info("Closing stream {} ...", name);
            running = false;
            // stop any outstanding ownership acquire actions first
            interrupt();
            try {
                join();
            } catch (InterruptedException e) {
                logger.error("Interrupted on closing stream {} : ", name, e);
            }
            logger.info("Stopped threads of stream {}.", name);
            // Close the writers to release the locks before failing the requests
            if (abort) {
                abort(writer);
            } else {
                close(writer);
            }
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
                pendingOpsCounter.decrementAndGet();
            }
            limiter.close();
            logger.info("Closed stream {}.", name);
        }
    }

    private final ServerConfiguration serverConfig;
    private final DistributedLogConfiguration dlConfig;
    private final DistributedLogNamespace dlNamespace;
    private final ConcurrentHashMap<String, Stream> streams =
            new ConcurrentHashMap<String, Stream>();
    private final ConcurrentHashMap<String, Stream> acquiredStreams =
            new ConcurrentHashMap<String, Stream>();

    private final int serverRegionId;
    private ServerStatus serverStatus = ServerStatus.WRITE_AND_ACCEPT;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final CountDownLatch keepAliveLatch;
    private final AtomicInteger pendingOpsCounter;

    // Features
    private final FeatureProvider featureProvider;
    private final Feature featureRegionStopAcceptNewStream;

    // Stats
    private final StatsLogger statsLogger;
    private final StatsLogger perStreamStatsLogger;
    private final StreamOpStats streamOpStats;

    // operation stats
    private final Counter bulkWritePendingStat;
    private final Counter writePendingStat;
    private final Counter redirects;
    private final Counter unexpectedExceptions;
    private final Counter serviceTimeout;

    // denied operations counters
    private final Counter deniedBulkWriteCounter;
    private final Counter deniedWriteCounter;
    private final Counter deniedHeartbeatCounter;
    private final Counter deniedTruncateCounter;
    private final Counter deniedDeleteCounter;
    private final Counter deniedReleaseCounter;

    private final Counter receivedRecordCounter;

    // exception stats
    private final StatsLogger exceptionStatLogger;
    private final ConcurrentHashMap<String, Counter> exceptionCounters =
            new ConcurrentHashMap<String, Counter>();
    private final StatsLogger statusCodeStatLogger;
    private final ConcurrentHashMap<StatusCode, Counter> statusCodeCounters =
            new ConcurrentHashMap<StatusCode, Counter>();
    // streams stats
    private final OpStatsLogger streamAcquireStat;
    // per streams stats
    private final StatsLogger limiterStatLogger;

    private final byte dlsnVersion;
    private final String clientId;
    private final ScheduledExecutorService executorService;
    private final AccessControlManager accessControlManager;
    private final boolean failFastOnStreamNotReady;
    private final HashedWheelTimer dlTimer;
    private long serviceTimeoutMs;
    private final long streamProbationTimeoutMs;
    private final StreamConfigProvider streamConfigProvider;

    DistributedLogServiceImpl(ServerConfiguration serverConf,
                              DistributedLogConfiguration dlConf,
                              StreamConfigProvider streamConfigProvider,
                              URI uri,
                              StatsLogger statsLogger,
                              StatsLogger perStreamStatsLogger,
                              CountDownLatch keepAliveLatch)
            throws IOException {
        // Configuration.
        this.serverConfig = serverConf;
        this.dlConfig = dlConf;
        this.perStreamStatsLogger = perStreamStatsLogger;
        this.dlsnVersion = serverConf.getDlsnVersion();
        this.serverRegionId = serverConf.getRegionId();
        int serverPort = serverConf.getServerPort();
        int shard = serverConf.getServerShardId();
        int numThreads = serverConf.getServerThreads();
        this.clientId = DLSocketAddress.toLockId(DLSocketAddress.getSocketAddress(serverPort), shard);
        String allocatorPoolName = String.format("allocator_%04d_%010d", serverRegionId, shard);
        dlConf.setLedgerAllocatorPoolName(allocatorPoolName);
        this.featureProvider = AbstractFeatureProvider.getFeatureProvider("", dlConf, statsLogger.scope("features"));

        // Build the namespace
        this.dlNamespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(dlConf)
                .uri(uri)
                .statsLogger(statsLogger)
                .featureProvider(this.featureProvider)
                .clientId(clientId)
                .regionId(serverRegionId)
                .build();
        this.accessControlManager = this.dlNamespace.createAccessControlManager();

        this.keepAliveLatch = keepAliveLatch;
        this.failFastOnStreamNotReady = dlConf.getFailFastOnStreamNotReady();
        this.dlTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("DLService-timer-%d").build(),
                dlConf.getTimeoutTimerTickDurationMs(),
                TimeUnit.MILLISECONDS);
        this.serviceTimeoutMs = dlConf.getServiceTimeoutMs();
        this.streamProbationTimeoutMs = dlConf.getStreamProbationTimeoutMs();
        this.streamConfigProvider = streamConfigProvider;

        // Executor Service.
        this.executorService = Executors.newScheduledThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Executor-%d").build());

        // service features
        this.featureRegionStopAcceptNewStream = this.featureProvider.getFeature(
                ServerFeatureKeys.REGION_STOP_ACCEPT_NEW_STREAM.name().toLowerCase());

        // Stats
        this.statsLogger = statsLogger;

        // Stats on server
        // Gauge for server status/health
        statsLogger.registerGauge("proxy_status", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return ServerStatus.DOWN == serverStatus ? -1 : (featureRegionStopAcceptNewStream.isAvailable() ?
                        3 : (ServerStatus.WRITE_AND_ACCEPT == serverStatus ? 1 : 2));
            }
        });
        this.unexpectedExceptions = statsLogger.getCounter("unexpected_exceptions");
        this.pendingOpsCounter = new AtomicInteger(0);
        statsLogger.registerGauge("pending_ops", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return pendingOpsCounter.get();
            }
        });
        this.serviceTimeout = statsLogger.getCounter("serviceTimeout");
        this.limiterStatLogger = statsLogger.scope("request_limiter");

        // Stats pertaining to stream op execution
        this.streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);

        // Stats on requests
        this.bulkWritePendingStat = streamOpStats.requestPendingCounter("bulkWritePending");
        this.writePendingStat = streamOpStats.requestPendingCounter("writePending");
        this.redirects = streamOpStats.requestCounter("redirect");
        this.exceptionStatLogger = streamOpStats.requestScope("exceptions");
        this.statusCodeStatLogger = streamOpStats.requestScope("statuscode");
        this.receivedRecordCounter = streamOpStats.recordsCounter("received");

        // Stats on denied requests
        this.deniedBulkWriteCounter = streamOpStats.requestDeniedCounter("bulkWrite");
        this.deniedWriteCounter = streamOpStats.requestDeniedCounter("write");
        this.deniedHeartbeatCounter = streamOpStats.requestDeniedCounter("heartbeat");
        this.deniedTruncateCounter = streamOpStats.requestDeniedCounter("truncate");
        this.deniedDeleteCounter = streamOpStats.requestDeniedCounter("delete");
        this.deniedReleaseCounter = streamOpStats.requestDeniedCounter("release");

        // Stats on streams
        StatsLogger streamsStatsLogger = statsLogger.scope("streams");
        streamsStatsLogger.registerGauge("acquired", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return acquiredStreams.size();
            }
        });
        streamsStatsLogger.registerGauge("cached", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return streams.size();
            }
        });
        this.streamAcquireStat = streamsStatsLogger.getOpStatsLogger("acquire");

        // Setup complete
        logger.info("Running distributedlog server : client id {}, allocator pool {}, perstream stat {}, dlsn version {}.",
                new Object[] { clientId, allocatorPoolName, serverConf.isPerStreamStatEnabled(), dlsnVersion });
    }

    Stream newStream(String name) {
        return new Stream(name);
    }

    @VisibleForTesting
    public void setServiceTimeoutMs(int serviceTimeoutMs) {
        this.serviceTimeoutMs = serviceTimeoutMs;
    }

    @VisibleForTesting
    public DistributedLogNamespace getDistributedLogNamespace() {
        return dlNamespace;
    }

    @VisibleForTesting
    ConcurrentMap<String, Stream> getCachedStreams() {
        return this.streams;
    }

    @VisibleForTesting
    ConcurrentMap<String, Stream> getAcquiredStreams() {
        return this.acquiredStreams;
    }

    java.util.concurrent.Future<?> schedule(Runnable r, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (ServerStatus.DOWN == serverStatus) {
                return null;
            }
            if (delayMs > 0) {
                return executorService.schedule(r, delayMs, TimeUnit.MILLISECONDS);
            } else {
                return executorService.submit(r);
            }
        } catch (RejectedExecutionException ree) {
            logger.error("Failed to schedule task {} in {} ms : ",
                    new Object[] { r, delayMs, ree });
            return null;
        } finally {
            closeLock.readLock().unlock();
        }
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

    void countStatusCode(StatusCode code) {
        Counter counter = statusCodeCounters.get(code);
        if (null == counter) {
            counter = statusCodeStatLogger.getCounter(code.name());
            Counter oldCounter = statusCodeCounters.putIfAbsent(code, counter);
            if (null != oldCounter) {
                counter = oldCounter;
            }
        }
        counter.inc();
    }

    @Override
    public Future<ServerInfo> handshake() {
        return handshakeWithClientInfo(new ClientInfo());
    }

    @Override
    public Future<ServerInfo> handshakeWithClientInfo(ClientInfo clientInfo) {
        ServerInfo serverInfo = new ServerInfo();
        closeLock.readLock().lock();
        try {
            serverInfo.setServerStatus(serverStatus);
        } finally {
            closeLock.readLock().unlock();
        }

        if (clientInfo.isSetGetOwnerships() && !clientInfo.isGetOwnerships()) {
            return Future.value(serverInfo);
        }

        Map<String, String> ownerships = new HashMap<String, String>();
        for (String name : acquiredStreams.keySet()) {
            if (clientInfo.isSetStreamNameRegex() && !name.matches(clientInfo.getStreamNameRegex())) {
                continue;
            }
            Stream stream = acquiredStreams.get(name);
            if (null == stream) {
                continue;
            }
            String owner = stream.owner;
            if (null == owner) {
                ownerships.put(name, clientId);
            }
        }
        serverInfo.setOwnerships(ownerships);
        return Future.value(serverInfo);
    }

    Stream getLogWriter(String stream) throws IOException {
        Stream writer = streams.get(stream);
        if (null == writer) {
            closeLock.readLock().lock();
            try {
                if (featureRegionStopAcceptNewStream.isAvailable()) {
                    // accept new stream is disabled in current dc
                    throw new RegionUnavailableException("Region is unavailable right now.");
                } else if (!(ServerStatus.WRITE_AND_ACCEPT == serverStatus)) {
                    // if it is closed, we would not acquire stream again.
                    return null;
                }
                writer = newStream(stream);
                Stream oldWriter = streams.putIfAbsent(stream, writer);
                if (null != oldWriter) {
                    writer = oldWriter;
                } else {
                    logger.info("Inserted mapping stream {} -> writer {}", stream, writer);
                    writer.initialize().start();
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return writer;
    }

    // Service interface methods

    @Override
    public Future<WriteResponse> write(final String stream, ByteBuffer data) {
        if (!accessControlManager.allowWrite(stream)) {
            deniedWriteCounter.inc();
            return Future.value(ResponseUtils.writeDenied());
        }
        receivedRecordCounter.inc();
        return doWrite(stream, data);
    }

    @Override
    public Future<BulkWriteResponse> writeBulkWithContext(final String stream, List<ByteBuffer> data, WriteContext ctx) {
        if (!accessControlManager.allowWrite(stream)) {
            deniedBulkWriteCounter.inc();
            return Future.value(ResponseUtils.bulkWriteDenied());
        }
        bulkWritePendingStat.inc();
        receivedRecordCounter.add(data.size());
        BulkWriteOp op = new BulkWriteOp(stream, data, statsLogger, perStreamStatsLogger);
        doExecuteStreamOp(op);
        return op.result().ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                bulkWritePendingStat.dec();
                return null;
            }
        });
    }

    @Override
    public Future<WriteResponse> writeWithContext(final String stream, ByteBuffer data, WriteContext ctx) {
        if (!accessControlManager.allowWrite(stream)) {
            deniedWriteCounter.inc();
            return Future.value(ResponseUtils.writeDenied());
        }
        return doWrite(stream, data);
    }

    @Override
    public Future<WriteResponse> heartbeat(String stream, WriteContext ctx) {
        if (!accessControlManager.allowAcquire(stream)) {
            deniedHeartbeatCounter.inc();
            return Future.value(ResponseUtils.writeDenied());
        }
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, dlsnVersion);
        doExecuteStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> heartbeatWithOptions(String stream, WriteContext ctx, HeartbeatOptions options) {
        if (!accessControlManager.allowAcquire(stream)) {
            deniedHeartbeatCounter.inc();
            return Future.value(ResponseUtils.writeDenied());
        }
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, dlsnVersion);
        if (options.isSendHeartBeatToReader()) {
            op.setWriteControlRecord(true);
        }
        doExecuteStreamOp(op);
        return op.result();
    }


    @Override
    public Future<WriteResponse> truncate(String stream, String dlsn, WriteContext ctx) {
        if (!accessControlManager.allowTruncate(stream)) {
            deniedTruncateCounter.inc();
            return Future.value(ResponseUtils.writeDenied());
        }
        TruncateOp op = new TruncateOp(stream, DLSN.deserialize(dlsn), statsLogger);
        doExecuteStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> delete(String stream, WriteContext ctx) {
        if (!accessControlManager.allowTruncate(stream)) {
            deniedDeleteCounter.inc();
            return Future.value(ResponseUtils.writeDenied());
        }
        DeleteOp op = new DeleteOp(stream, statsLogger, this /* stream manager */);
        doExecuteStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> release(String stream, WriteContext ctx) {
        if (!accessControlManager.allowRelease(stream)) {
            deniedReleaseCounter.inc();
            return Future.value(ResponseUtils.writeDenied());
        }
        ReleaseOp op = new ReleaseOp(stream, statsLogger, this /* stream manager */);
        doExecuteStreamOp(op);
        return op.result();
    }

    @Override
    public Future<Void> setAcceptNewStream(boolean enabled) {
        closeLock.writeLock().lock();
        try {
            logger.info("Set AcceptNewStream = {}", enabled);
            if (ServerStatus.DOWN != serverStatus) {
                if (enabled) {
                    serverStatus = ServerStatus.WRITE_AND_ACCEPT;
                } else {
                    serverStatus = ServerStatus.WRITE_ONLY;
                }
            }
        } finally {
            closeLock.writeLock().unlock();
        }
        return Future.Void();
    }

    private Future<WriteResponse> doWrite(final String name, ByteBuffer data) {
        writePendingStat.inc();
        receivedRecordCounter.inc();
        WriteOp op = newWriteOp(name, data);
        doExecuteStreamOp(op);
        return op.result().ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                writePendingStat.dec();
                return null;
            }
        });
    }

    private void doExecuteStreamOp(final StreamOp op) {
        op.preExecute();

        Stream stream;
        try {
            stream = getLogWriter(op.streamName());
        } catch (RegionUnavailableException rue) {
            // redirect the requests to other region
            op.fail(new RegionUnavailableException("Region " + serverRegionId + " is unavailable."));
            return;
        } catch (IOException e) {
            op.fail(e);
            return;
        }
        if (null == stream) {
            // redirect the requests when stream is unavailable.
            op.fail(new ServiceUnavailableException("Server " + clientId + " is closed."));
            return;
        }
        stream.waitIfNeededAndWrite(op);
    }

    Future<List<Void>> closeStreams() {
        int numAcquired = acquiredStreams.size();
        int numCached = streams.size();
        logger.info("Closing all acquired streams : acquired = {}, cached = {}.",
                    numAcquired, numCached);
        Set<Stream> streamsToClose = new HashSet<Stream>();
        streamsToClose.addAll(streams.values());
        return closeStreams(streamsToClose, Optional.<RateLimiter>absent());
    }

    private Future<List<Void>> closeStreams(Set<Stream> streamsToClose, Optional<RateLimiter> rateLimiter) {
        if (streamsToClose.isEmpty()) {
            logger.info("No streams to close.");
            List<Void> emptyList = new ArrayList<Void>();
            return Future.value(emptyList);
        }
        List<Future<Void>> futures = new ArrayList<Future<Void>>(streamsToClose.size());
        for (Stream stream : streamsToClose) {
            if (rateLimiter.isPresent()) {
                rateLimiter.get().acquire();
            }
            futures.add(stream.requestClose("Close Streams"));
        }
        return Future.collect(futures);
    }

    void shutdown() {
        try {
            closeLock.writeLock().lock();
            try {
                if (ServerStatus.DOWN == serverStatus) {
                    return;
                }
                serverStatus = ServerStatus.DOWN;
            } finally {
                closeLock.writeLock().unlock();
            }

            Stopwatch closeStreamsStopwatch = Stopwatch.createStarted();

            Future<List<Void>> closeResult = closeStreams();
            logger.info("Waiting for closing all streams ...");
            try {
                Await.result(closeResult, Duration.fromTimeUnit(5, TimeUnit.MINUTES));
                logger.info("Closed all streams in {} millis.",
                        closeStreamsStopwatch.elapsed(TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                logger.warn("Interrupted on waiting for closing all streams : ", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("Sorry, we didn't close all streams gracefully in 5 minutes : ", e);
            }

            // shutdown the dl namespace
            logger.info("Closing distributedlog namespace ...");
            dlNamespace.close();
            logger.info("Closed distributedlog namespace .");

            // shutdown the executor after requesting closing streams.
            SchedulerUtils.shutdownScheduler(executorService, 60, TimeUnit.SECONDS);

            // Shutdown timeout timer
            dlTimer.stop();
        } catch (Exception ex) {
            logger.info("Exception while shutting down distributedlog service.");
        } finally {
            // release the keepAliveLatch in case shutdown is called from a shutdown hook.
            keepAliveLatch.countDown();
            logger.info("Finished shutting down distributedlog service.");
        }
    }

    void triggerShutdown() {
        // release the keepAliveLatch to let the main thread shutdown the whole service.
        logger.info("Releasing KeepAlive Latch to trigger shutdown ...");
        keepAliveLatch.countDown();
        logger.info("Released KeepAlive Latch. Main thread will shut the service down.");
    }
}
