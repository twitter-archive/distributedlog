package com.twitter.distributedlog.service;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class DistributedLogServiceImpl implements DistributedLogService.ServiceIface {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServiceImpl.class);

    static final int MAX_RETRIES = 3;

    static enum StreamStatus {
        UNINITIALIZED,
        INITIALIZING,
        INITIALIZED,
        BACKOFF,
        FAILED,
        CLOSING,
        CLOSED
    }

    static enum ServerMode {
        DURABLE,
        MEM
    }

    static ResponseHeader ownerToResponseHeader(String owner) {
        return new ResponseHeader(StatusCode.FOUND).setLocation(owner);
    }

    static ResponseHeader successResponseHeader() {
        return new ResponseHeader(StatusCode.SUCCESS);
    }

    static ResponseHeader responseHeader(StatusCode code, String errMsg) {
        return new ResponseHeader(code);
    }

    static ResponseHeader exceptionToResponseHeader(Throwable t) {
        ResponseHeader response = new ResponseHeader();
        if (t instanceof DLException) {
            DLException dle = (DLException) t;
            if (dle instanceof OwnershipAcquireFailedException) {
                response.setLocation(((OwnershipAcquireFailedException) dle).getCurrentOwner());
            }
            response.setCode(dle.getCode());
            response.setErrMsg(dle.getMessage());
        } else {
            response.setCode(StatusCode.INTERNAL_SERVER_ERROR);
            response.setErrMsg("Internal server error : " + t.getMessage());
        }
        return response;
    }

    static WriteResponse writeResponse(ResponseHeader responseHeader) {
        return new WriteResponse(responseHeader);
    }

    static ConcurrentMap<String, String> ownerToHosts = new ConcurrentHashMap<String, String>();

    static String getHostByOwner(String owner) {
        String host = ownerToHosts.get(owner);
        if (null == host) {
            try {
                host = DLSocketAddress.deserialize(owner).getSocketAddress().toString();
            } catch (IOException e) {
                host = owner;
            }
            String oldHost = ownerToHosts.putIfAbsent(owner, host);
            if (null != oldHost) {
                host = oldHost;
            }
        }
        return host;
    }

    abstract class StreamOp implements FutureEventListener<WriteResponse> {
        final String stream;
        final AtomicInteger retries = new AtomicInteger(0);
        final Promise<WriteResponse> result = new Promise<WriteResponse>();
        final Stopwatch stopwatch = new Stopwatch();
        final WriteContext context;
        final OpStatsLogger opStatsLogger;

        StreamOp(String stream, WriteContext context, OpStatsLogger statsLogger) {
            this.stream = stream;
            this.context = context;
            this.opStatsLogger = statsLogger;
            // start here in case the operation is failed before executing.
            stopwatch.reset().start();
        }

        boolean hasTried(String owner) {
            return context.isSetTriedHosts() && context.getTriedHosts().contains(getHostByOwner(owner));
        }

        Future<WriteResponse> execute(AsyncLogWriter writer) {
            stopwatch.reset().start();
            return executeOp(writer);
        }

        /**
         * Execute the operation and return its corresponding response.
         *
         * @param writer
         *          writer to execute the operation.
         * @return future representing the operation.
         */
        abstract Future<WriteResponse> executeOp(AsyncLogWriter writer);

        /**
         * Fail with current <i>owner</i> and its reason <i>t</i>
         *
         * @param owner
         *          current owner
         * @param t
         *          failure reason
         */
        void fail(String owner, Throwable t) {
            opStatsLogger.registerFailedEvent(stopwatch.stop().elapsedMillis());
            if (null != owner) {
                redirects.inc();
                result.setValue(writeResponse(ownerToResponseHeader(owner)));
            } else {
                result.setValue(writeResponse(exceptionToResponseHeader(t)));
            }
        }

        @Override
        public void onSuccess(WriteResponse response) {
            opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
            // complete the response, so it would be sent back to client.
            result.setValue(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            // nop, as failure will be handled in {@link Stream.doExecuteOp}.
        }
    }

    class HeartbeatOp extends StreamOp {

        HeartbeatOp(String stream, WriteContext context) {
            super(stream, context, heartbeatStat);
        }

        @Override
        Future<WriteResponse> executeOp(AsyncLogWriter writer) {
            return Future.value(writeResponse(successResponseHeader()))
                    .addEventListener(this);
        }
    }

    class TruncateOp extends StreamOp {

        final DLSN dlsn;

        TruncateOp(String stream, DLSN dlsn, WriteContext context) {
            super(stream, context, truncateStat);
            this.dlsn = dlsn;
        }

        @Override
        Future<WriteResponse> executeOp(AsyncLogWriter writer) {
            return writer.truncate(dlsn).map(new AbstractFunction1<Boolean, WriteResponse>() {
                @Override
                public WriteResponse apply(Boolean v1) {
                    return writeResponse(successResponseHeader());
                }
            }).addEventListener(this);
        }
    }

    class DeleteOp extends StreamOp implements Runnable {

        final Promise<WriteResponse> deletePromise =
                new Promise<WriteResponse>();

        DeleteOp(String stream, WriteContext context) {
            super(stream, context, deleteStat);
        }

        @Override
        Future<WriteResponse> executeOp(AsyncLogWriter writer) {
            if (null == schedule(this, 0)) {
                return Future.exception(new ServiceUnavailableException("Couldn't schedule a delete task."));
            }
            return deletePromise.addEventListener(this);
        }

        @Override
        public void run() {
            Stream s = streams.get(stream);
            if (null == s) {
                logger.warn("No stream {} to delete.", stream);
                deletePromise.setException(new UnexpectedException("No stream " + stream + " to delete."));
            } else {
                logger.info("Removing stream {}.", stream);
                try {
                    s.delete();
                    logger.info("Removed stream {}.", stream);
                    acquiredStreams.remove(stream, s);
                    if (streams.remove(stream, s)) {
                        s.close();
                    }
                    deletePromise.setValue(writeResponse(successResponseHeader()));
                } catch (IOException e) {
                    logger.error("Failed on removing stream {} : ", stream, e);
                    deletePromise.setException(e);
                }
            }
        }
    }

    class ReleaseOp extends StreamOp implements Runnable {

        final Promise<WriteResponse> releasePromise =
                new Promise<WriteResponse>();

        ReleaseOp(String stream, WriteContext context) {
            super(stream, context, releaseStat);
        }

        @Override
        Future<WriteResponse> executeOp(AsyncLogWriter writer) {
            if (null == schedule(this, 0)) {
                return Future.exception(new ServiceUnavailableException("Couldn't schedule a release task."));
            }
            return releasePromise.addEventListener(this);
        }

        @Override
        public void run() {
            Stream s = streams.remove(stream);
            if (null == s) {
                logger.warn("No stream {} to release.", stream);
            } else {
                logger.info("Removing stream {}.", stream);
                acquiredStreams.remove(stream, s);
                s.close();
            }
            releasePromise.setValue(writeResponse(successResponseHeader()));
        }
    }

    class WriteOp extends StreamOp {
        final byte[] payload;

        WriteOp(String stream, ByteBuffer data, WriteContext context) {
            super(stream, context, writeStat);
            payload = new byte[data.remaining()];
            data.get(payload);
        }

        @Override
        Future<WriteResponse> executeOp(AsyncLogWriter writer) {
            long txnId;
            Future<DLSN> writeResult;
            synchronized (txnLock) {
                txnId = System.currentTimeMillis();
                writeResult = writer.write(new LogRecord(txnId, payload));
            }
            if (serverMode.equals(ServerMode.DURABLE)) {
                return writeResult.map(new AbstractFunction1<DLSN, WriteResponse>() {
                    @Override
                    public WriteResponse apply(DLSN value) {
                        return writeResponse(successResponseHeader()).setDlsn(value.serialize());
                    }
                }).addEventListener(this);
            } else {
                if (delayMs > 0) {
                    final long txnIdToReturn = txnId;
                    try {
                        executorService.schedule(new Runnable() {
                            @Override
                            public void run() {
                                opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                                result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                            }
                        }, delayMs, TimeUnit.MILLISECONDS);
                    } catch (RejectedExecutionException ree) {
                        opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                        result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                    }
                } else {
                    opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                    result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnId)));
                }
                return result;
            }
        }

    }

    class Stream extends Thread {
        final String name;

        // lock
        final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

        final DistributedLogManager manager;

        volatile AsyncLogWriter writer;
        volatile StreamStatus status;
        volatile String owner;
        volatile Throwable lastException;
        volatile boolean running = true;
        private volatile Queue<StreamOp> pendingOps = new ArrayDeque<StreamOp>();

        final Object streamLock = new Object();
        // last acquire time
        final Stopwatch lastAcquireWatch = new Stopwatch();
        // last acquire failure time
        final Stopwatch lastAcquireFailureWatch = new Stopwatch();
        final long nextAcquireWaitTimeMs;

        Stream(String name) throws IOException {
            super("Stream-" + name);
            this.name = name;
            this.manager = dlFactory.createDistributedLogManagerWithSharedClients(name);
            status = StreamStatus.UNINITIALIZED;
            lastException = new IOException("Fail to write record to stream " + name);
            nextAcquireWaitTimeMs = dlConfig.getZKSessionTimeoutMilliseconds() * 3 / 5;
        }

        @Override
        public void run() {
            boolean shouldClose = false;
            while (running) {
                boolean needAcquire = false;
                synchronized (this) {
                    switch (this.status) {
                    case UNINITIALIZED:
                        this.status = StreamStatus.INITIALIZING;
                        needAcquire = true;
                        break;
                    case BACKOFF:
                        this.status = StreamStatus.INITIALIZING;
                        needAcquire = true;
                        break;
                    case FAILED:
                        // there are pending ops, try re-acquiring the stream again
                        if (pendingOps.size() > 0) {
                            this.status = StreamStatus.INITIALIZING;
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
                } else if (StreamStatus.INITIALIZED != status && lastAcquireWatch.elapsedTime(TimeUnit.HOURS) > 2) {
                    if (streams.remove(name, this)) {
                        shouldClose = true;
                        break;
                    }
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
                requestClose();
            }
        }

        /**
         * Wait for stream being re-acquired if needed. Otherwise, execute the <i>op</i> immediately.
         *
         * @param op
         *          stream operation to execute.
         */
        void waitIfNeededAndWrite(StreamOp op) {
            op.retries.incrementAndGet();
            boolean completeOpNow = false;
            boolean success = true;
            if (StreamStatus.CLOSED == status) {
                // Stream is closed, fail the op immediately
                op.result.setValue(writeResponse(responseHeader(
                        StatusCode.SERVICE_UNAVAILABLE, "Stream " + name + " is closed.")));
                return;
            } if (StreamStatus.INITIALIZED == status && writer != null) {
                completeOpNow = true;
                success = true;
            } else {
                synchronized (this) {
                    if (StreamStatus.CLOSED == status) {
                        // complete the write op as {@link #executeOp(op, success)} will handle closed case.
                        completeOpNow = true;
                        success = true;
                    } if (StreamStatus.INITIALIZED == status) {
                        completeOpNow = true;
                        success = true;
                    } else if (StreamStatus.FAILED == status &&
                            lastAcquireFailureWatch.elapsedMillis() < nextAcquireWaitTimeMs) {
                        completeOpNow = true;
                        success = false;
                    } else { // closing & initializing
                        if (pendingOps.isEmpty()) {
                            // when any op is added in pending queue, notify stream to acquire the stream.
                            synchronized (streamLock) {
                                streamLock.notifyAll();
                            }
                        }
                        pendingOps.add(op);
                    }
                }
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
                if (StreamStatus.CLOSED == status) {
                    op.result.setValue(writeResponse(responseHeader(
                            StatusCode.SERVICE_UNAVAILABLE, "Stream " + name + " is closed.")));
                    return;
                }
                doExecuteOp(op, success);
            } finally {
                closeLock.readLock().unlock();
            }
        }

        private void doExecuteOp(final StreamOp op, boolean success) {
            AsyncLogWriter w = writer;
            if (null != w && success) {
                op.execute(w).addEventListener(new FutureEventListener<WriteResponse>() {
                    @Override
                    public void onSuccess(WriteResponse value) {
                        // nop
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        if (cause instanceof OwnershipAcquireFailedException) {
                            handleOwnershipAcquireFailedException(op, (OwnershipAcquireFailedException) cause);
                        } else if (cause instanceof AlreadyClosedException) {
                            handleAlreadyClosedException((AlreadyClosedException) cause);
                        } else {
                            handleGenericException(op, cause);
                        }
                    }
                });
            } else {
                failOp(op, owner, lastException);
            }
        }

        private void failOp(StreamOp op, String owner, Throwable t) {
            op.fail(owner, t);
        }

        /**
         * Handle generic exception when executing <i>op</i>.
         *
         * @param op
         *          stream operation executing
         * @param cause
         *          exception received when executing <i>op</i>
         */
        private void handleGenericException(StreamOp op, final Throwable cause) {
            countException(cause);
            AsyncLogWriter oldWriter = null;
            boolean reinitialize = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    oldWriter = setStreamStatus(StreamStatus.BACKOFF, null, null, cause);
                    reinitialize = true;
                }
            }
            if (reinitialize) {
                close(oldWriter);
                logger.error("Failed to write data into stream {} : ", name, cause);
            }
            if (op.retries.get() >= MAX_RETRIES) {
                op.fail(owner, lastException);
            } else {
                waitIfNeededAndWrite(op);
            }
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
            countException(oafe);
            logger.error("Failed to write data into stream {} : ", name, oafe);
            AsyncLogWriter oldWriter = null;
            boolean statusChanged = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    oldWriter =
                        setStreamStatus(StreamStatus.FAILED, null, oafe.getCurrentOwner(), oafe);
                    statusChanged = true;
                }
            }
            if (statusChanged) {
                close(oldWriter);
            }
            failOp(op, oafe.getCurrentOwner(), oafe);
        }

        /**
         * Handling already closed exception.
         */
        private void handleAlreadyClosedException(AlreadyClosedException ace) {
            // TODO: change to more specific exception: e.g. bkzk session expired?
            logger.error("DL Manager is closed. Quiting : ", ace);
            Runtime.getRuntime().exit(-1);
        }

        void acquireStream() {
            Queue<StreamOp> oldPendingOps;
            boolean success;
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                AsyncLogWriter w = manager.startAsyncLogSegmentNonPartitioned();
                synchronized (this) {
                    setStreamStatus(StreamStatus.INITIALIZED, w, null, null);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = true;
                }
            } catch (final AlreadyClosedException ace) {
                handleAlreadyClosedException(ace);
                return;
            } catch (final OwnershipAcquireFailedException oafe) {
                countException(oafe);
                logger.error("Failed to initialize stream {} : ", name, oafe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED, null, oafe.getCurrentOwner(), oafe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            } catch (final IOException ioe) {
                countException(ioe);
                logger.error("Failed to initialize stream {} : ", name, ioe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED, null, null, ioe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            }
            stopwatch.stop();
            if (success) {
                streamAcquireStat.registerSuccessfulEvent(stopwatch.elapsedMillis());
            } else {
                streamAcquireStat.registerFailedEvent(stopwatch.elapsedMillis());
            }
            for (StreamOp op : oldPendingOps) {
                executeOp(op, success);
            }
        }

        synchronized AsyncLogWriter setStreamStatus(StreamStatus status, AsyncLogWriter writer,
                                                    String owner, Throwable t) {
            AsyncLogWriter oldWriter = this.writer;
            this.writer = writer;
            if (null != owner && owner.equals(clientId)) {
                // I lost the ownership but left a lock over zookeeper
                // I should not ask client to redirect to myself again as I can't handle it :(
                // shutdown myself
                triggerShutdown();
                this.owner = null;
            } else {
                this.owner = owner;
            }
            this.lastException = t;
            this.status = status;
            if (StreamStatus.FAILED == status && null != owner) {
                // start failure watch
                this.lastAcquireFailureWatch.reset().start();
            }
            if (StreamStatus.INITIALIZED == status) {
                acquiredStreams.put(name, this);
            } else {
                acquiredStreams.remove(name, this);
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

        void requestClose() {
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSING == status) {
                    return;
                }
                status = StreamStatus.CLOSING;
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
                        close();
                    }
                });
            } catch (RejectedExecutionException ree) {
                logger.warn("Failed to schedule closing stream {}, close it directly : ", name, ree);
                close();
            }
        }

        void delete() throws IOException {
            if (null != writer) {
                writer.close();
                writer = null;
            }
            if (null == manager) {
                throw new UnexpectedException("No stream " + name + " to delete");
            }
            try {
                manager.delete();
            } finally {
                close();
            }
        }

        void close() {
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSED == status) {
                    return;
                }
                status = StreamStatus.CLOSED;
            } finally {
                closeLock.writeLock().unlock();
            }
            logger.info("Closing stream {} ...", name);
            running = false;
            Queue<StreamOp> oldPendingOps;
            synchronized (this) {
                oldPendingOps = pendingOps;
                pendingOps = new ArrayDeque<StreamOp>();
            }
            for (StreamOp op : oldPendingOps) {
                op.result.setValue(writeResponse(responseHeader(
                        StatusCode.SERVICE_UNAVAILABLE, "Stream " + name + " is closed.")));
            }
            close(writer);
            close(manager);
            interrupt();
            try {
                join();
            } catch (InterruptedException e) {
                logger.error("Interrupted on closing stream {} : ", name, e);
            }
            logger.info("Closed stream {}.", name);
        }
    }

    private final DistributedLogConfiguration dlConfig;
    private final DistributedLogManagerFactory dlFactory;
    private final ConcurrentHashMap<String, Stream> streams =
            new ConcurrentHashMap<String, Stream>();
    private final ConcurrentHashMap<String, Stream> acquiredStreams =
            new ConcurrentHashMap<String, Stream>();

    private boolean closed = false;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final CountDownLatch keepAliveLatch;
    private final Object txnLock = new Object();

    // Stats
    // operation stats
    private final OpStatsLogger writeStat;
    private final OpStatsLogger heartbeatStat;
    private final OpStatsLogger truncateStat;
    private final OpStatsLogger deleteStat;
    private final OpStatsLogger releaseStat;
    private final Counter redirects;
    // exception stats
    private final StatsLogger exceptionStatLogger;
    private final ConcurrentHashMap<String, Counter> exceptionCounters =
            new ConcurrentHashMap<String, Counter>();
    // streams stats
    private final OpStatsLogger streamAcquireStat;

    private final String clientId;
    private final ServerMode serverMode;
    private final ScheduledExecutorService executorService;
    private final long delayMs;

    DistributedLogServiceImpl(DistributedLogConfiguration dlConf, URI uri, StatsLogger statsLogger,
                              CountDownLatch keepAliveLatch)
            throws IOException {
        // Configuration.
        this.dlConfig = dlConf;
        this.serverMode = ServerMode.valueOf(dlConf.getString("server_mode", ServerMode.DURABLE.toString()));
        int serverRegionId = dlConf.getInt("server_region_id", DistributedLogConstants.LOCAL_REGION_ID);
        int serverPort = dlConf.getInt("server_port", 0);
        int shard = dlConf.getInt("server_shard", -1);
        int numThreads = dlConf.getInt("server_threads", Runtime.getRuntime().availableProcessors());
        this.clientId = DLSocketAddress.toLockId(DLSocketAddress.getSocketAddress(serverPort), shard);
        String allocatorPoolName = String.format("allocator_%04d_%010d", serverRegionId, shard);
        dlConf.setLedgerAllocatorPoolName(allocatorPoolName);
        this.dlFactory = new DistributedLogManagerFactory(dlConf, uri, statsLogger, clientId, serverRegionId);
        this.keepAliveLatch = keepAliveLatch;

        // Executor Service.
        this.executorService = Executors.newScheduledThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Executor-%d").build());
        if (ServerMode.MEM.equals(serverMode)) {
            this.delayMs = dlConf.getLong("latency_delay", 0);
        } else {
            this.delayMs = 0;
        }

        // Stats
        StatsLogger requestsStatsLogger = statsLogger.scope("request");
        this.writeStat = requestsStatsLogger.getOpStatsLogger("write");
        this.heartbeatStat = requestsStatsLogger.getOpStatsLogger("heartbeat");
        this.truncateStat = requestsStatsLogger.getOpStatsLogger("truncate");
        this.deleteStat = requestsStatsLogger.getOpStatsLogger("delete");
        this.releaseStat = requestsStatsLogger.getOpStatsLogger("release");
        this.redirects = requestsStatsLogger.getCounter("redirect");
        this.exceptionStatLogger = requestsStatsLogger.scope("exceptions");
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
        streamAcquireStat = streamsStatsLogger.getOpStatsLogger("acquire");

        logger.info("Running distributedlog server in {} mode, delay ms {}, client id {}, allocator pool {}.",
                new Object[] { serverMode, delayMs, clientId, allocatorPoolName });
    }

    private java.util.concurrent.Future<?> schedule(Runnable r, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
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

    void countException(Throwable t) {
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
    }

    @Override
    public Future<ServerInfo> handshake() {
        return Future.value(new ServerInfo());
    }

    Stream getLogWriter(String stream) throws IOException {
        Stream writer = streams.get(stream);
        if (null == writer) {
            closeLock.readLock().lock();
            try {
                // if it is closed, we would not acquire stream again.
                if (closed) {
                    return null;
                }
                writer = new Stream(stream);
                Stream oldWriter = streams.putIfAbsent(stream, writer);
                if (null != oldWriter) {
                    writer.close();
                    writer = oldWriter;
                } else {
                    writer.start();
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return writer;
    }

    @Override
    public Future<WriteResponse> write(final String stream, ByteBuffer data) {
        return doWrite(stream, data, new WriteContext());
    }

    @Override
    public Future<WriteResponse> writeWithContext(final String stream, ByteBuffer data, WriteContext ctx) {
        return doWrite(stream, data, ctx);
    }

    private Future<WriteResponse> doWrite(final String name, ByteBuffer data, WriteContext ctx) {
        WriteOp op = new WriteOp(name, data, ctx);
        return doExecuteStreamOp(op);
    }

    @Override
    public Future<WriteResponse> heartbeat(String stream, WriteContext ctx) {
        HeartbeatOp op = new HeartbeatOp(stream, ctx);
        return doExecuteStreamOp(op);
    }

    @Override
    public Future<WriteResponse> truncate(String stream, String dlsn, WriteContext ctx) {
        TruncateOp op = new TruncateOp(stream, DLSN.deserialize(dlsn), ctx);
        return doExecuteStreamOp(op);
    }

    @Override
    public Future<WriteResponse> delete(String stream, WriteContext ctx) {
        DeleteOp op = new DeleteOp(stream, ctx);
        return doExecuteStreamOp(op);
    }

    @Override
    public Future<WriteResponse> release(String stream, WriteContext ctx) {
        ReleaseOp op = new ReleaseOp(stream, ctx);
        return doExecuteStreamOp(op);
    }

    private Future<WriteResponse> doExecuteStreamOp(StreamOp op) {
        Stream stream;
        try {
            stream = getLogWriter(op.stream);
        } catch (IOException e) {
            return Future.exception(e);
        }
        if (null == stream) {
            // redirect the requests when stream is unavailable.
            return Future.value(writeResponse(responseHeader(
                    StatusCode.SERVICE_UNAVAILABLE, "Server " + clientId + " is closed.")));
        }
        stream.waitIfNeededAndWrite(op);
        return op.result;
    }

    void shutdown() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
        for (Stream stream: streams.values()) {
            stream.requestClose();
        }
        // shutdown the executor after requesting closing streams.
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.warn("Executor service doesn't shutdown in 60 seconds, force quiting...");
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            logger.warn("Interrupted on waiting shutting down executor service : ", ie);
        }
        keepAliveLatch.countDown();
    }

    void triggerShutdown() {
        // avoid spawning too many threads.
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        Thread shutdownThread = new Thread("ShutdownThread") {
            @Override
            public void run() {
                shutdown();
            }
        };
        shutdownThread.start();
    }
}
