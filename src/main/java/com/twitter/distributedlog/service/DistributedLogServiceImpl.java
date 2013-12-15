package com.twitter.distributedlog.service;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.Pair;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
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
    static final int MAX_BACKOFFS = 2;
    static final Random rand = new Random(System.currentTimeMillis());

    static enum StreamStatus {
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

    class WriteOp {
        final String stream;
        final byte[] payload;
        final Promise<WriteResponse> result = new Promise<WriteResponse>();
        final AtomicInteger retries = new AtomicInteger(0);
        final Stopwatch stopwatch = new Stopwatch();
        final WriteContext context;

        WriteOp(String stream, ByteBuffer data, WriteContext context) {
            this.stream = stream;
            this.context = context;
            payload = new byte[data.remaining()];
            data.get(payload);
            stopwatch.start();
        }

        boolean hasTried(String owner) {
            return context.isSetTriedHosts() && context.getTriedHosts().contains(getHostByOwner(owner));
        }

        Future<DLSN> execute(AsyncLogWriter writer) {
            long txnId;
            Future<DLSN> writeResult;
            synchronized (txnLock) {
                txnId = System.currentTimeMillis();
                writeResult = writer.write(new LogRecord(txnId, payload));
            }
            if (serverMode.equals(ServerMode.DURABLE)) {
                return writeResult.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                        result.setValue(writeResponse(successResponseHeader()).setDlsn(value.serialize()));
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        // nop
                    }
                });
            } else {
                if (delayMs > 0) {
                    final long txnIdToReturn = txnId;
                    try {
                        executorService.schedule(new Runnable() {
                            @Override
                            public void run() {
                                requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                                result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                            }
                        }, delayMs, TimeUnit.MILLISECONDS);
                    } catch (RejectedExecutionException ree) {
                        requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                        result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                    }
                } else {
                    requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                    result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnId)));
                }
                return writeResult;
            }
        }

        void fail(String owner, Throwable t) {
            requestStat.registerFailedEvent(stopwatch.stop().elapsedMillis());
            if (null != owner) {
                redirects.inc();
                result.setValue(writeResponse(ownerToResponseHeader(owner)));
            } else {
                countException(t);
                result.setValue(writeResponse(exceptionToResponseHeader(t)));
            }
        }

    }

    class Stream implements Runnable {
        final String name;

        // lock
        final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

        volatile DistributedLogManager manager;
        volatile AsyncLogWriter writer;
        volatile StreamStatus status;
        volatile String owner;
        volatile Throwable lastException;
        volatile java.util.concurrent.Future<?> removeMyselfTask = null;

        private volatile Queue<WriteOp> pendingOps = new ArrayDeque<WriteOp>();

        Stream(String name) {
            this.name = name;
            status = StreamStatus.INITIALIZING;
            lastException = new IOException("Fail to write record to stream " + name);
        }

        void initialize(long delayInMillis) {
            schedule(this, delayInMillis);
        }

        void waitIfNeededAndWrite(WriteOp op) {
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
                    } else if (StreamStatus.FAILED == status) {
                        completeOpNow = true;
                        success = false;
                    } else { // closing & initializing
                        pendingOps.add(op);
                    }
                }
            }
            if (completeOpNow) {
                executeOp(op, success);
            }
        }

        void executeOp(WriteOp op, boolean success) {
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

        private void doExecuteOp(final WriteOp op, boolean success) {
            AsyncLogWriter w = writer;
            if (null != w && success) {
                op.execute(w).addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
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

        private void failOp(WriteOp op, String owner, Throwable t) {
            if (owner != null && op.hasTried(owner) && op.retries.get() < MAX_BACKOFFS) {
                boolean backoff = false;
                Pair<DistributedLogManager, AsyncLogWriter> pairToClose = null;
                synchronized (this) {
                    if (StreamStatus.BACKOFF == status) {
                        pairToClose =
                                setStreamStatus(StreamStatus.INITIALIZING, null, null, null, t);
                        backoff = true;
                    }
                }
                close(pairToClose);
                if (backoff) {
                    java.util.concurrent.Future<?> task = removeMyselfTask;
                    if (null != task) {
                        task.cancel(true);
                    }
                    initialize(100 + (op.retries.get() > 1 ? rand.nextInt(100) : 0));
                }
                waitIfNeededAndWrite(op);
            } else {
                op.fail(owner, t);
            }
        }

        private void handleGenericException(WriteOp op, final Throwable cause) {
            logger.error("Failed to write data into stream {} : ", name, cause);
            Pair<DistributedLogManager, AsyncLogWriter> pairToClose = null;
            boolean reinitialized = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    pairToClose =
                            setStreamStatus(StreamStatus.INITIALIZING, null, null, null, cause);
                    reinitialized = true;
                }
            }
            close(pairToClose);
            if (reinitialized) {
                // if we encountered a locking exception, kick reinitialize only after a zookeeper
                // session expired, otherwise we end up locking myself.
                if (cause instanceof LockingException) {
                    initialize(dlConfig.getZKSessionTimeoutMilliseconds());
                } else {
                    // other issue, kick it immediately.
                    initialize(0);
                }
            }
            if (op.retries.get() >= MAX_RETRIES) {
                op.fail(owner, lastException);
            } else {
                waitIfNeededAndWrite(op);
            }
        }

        private void handleOwnershipAcquireFailedException(WriteOp op, final OwnershipAcquireFailedException oafe) {
            logger.error("Failed to write data into stream {} : ", name, oafe);
            Pair<DistributedLogManager, AsyncLogWriter> pairToClose = null;
            boolean removeCache = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    pairToClose =
                        setStreamStatus(StreamStatus.BACKOFF, null, null, oafe.getCurrentOwner(), oafe);
                    removeCache = true;
                }
            }
            close(pairToClose);
            // cache the ownership for a while
            if (removeCache) {
                removeMyselfAfterAPeriod(oafe);
            }
            failOp(op, oafe.getCurrentOwner(), oafe);
        }

        private void handleAlreadyClosedException(AlreadyClosedException ace) {
            // TODO: change to more specific exception: e.g. bkzk session expired?
            logger.error("DL Manager is closed. Quiting : ", ace);
            Runtime.getRuntime().exit(-1);
        }

        private void removeMyselfAfterAPeriod(final Throwable t) {
            removeMyselfTask = schedule(new Runnable() {
                @Override
                public void run() {
                    removeMySelf(t);
                }
            }, dlConfig.getZKSessionTimeoutMilliseconds() / 2);
        }

        @Override
        public void run() {
            Queue<WriteOp> oldPendingOps;
            boolean success;
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                DistributedLogManager dlManager = dlFactory.createDistributedLogManagerWithSharedClients(name);
                AsyncLogWriter w = dlManager.startAsyncLogSegmentNonPartitioned();
                synchronized (this) {
                    setStreamStatus(StreamStatus.INITIALIZED, dlManager, w, null, null);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<WriteOp>();
                    success = true;
                }
            } catch (final AlreadyClosedException ace) {
                handleAlreadyClosedException(ace);
                return;
            } catch (final OwnershipAcquireFailedException oafe) {
                logger.error("Failed to initialize stream {} : ", name, oafe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.BACKOFF, null, null, oafe.getCurrentOwner(), oafe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<WriteOp>();
                    success = false;
                }
                // cache the ownership for a while
                removeMyselfAfterAPeriod(oafe);
            } catch (final IOException ioe) {
                logger.error("Failed to initialize stream {} : ", name, ioe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED, null, null, null, ioe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<WriteOp>();
                    success = false;
                }
                if (ioe instanceof LockingException) {
                    // don't remove myself until a zookeeper session expired,
                    // so it would fail the request during this period immediately
                    // and it would avoid ending up locking a lot of time.
                    removeMyselfAfterAPeriod(ioe);
                } else {
                    // other exceptions: fail immediately.
                    removeMySelf(ioe);
                }
            }
            stopwatch.stop();
            if (success) {
                streamAcquireStat.registerSuccessfulEvent(stopwatch.elapsedMillis());
            } else {
                streamAcquireStat.registerFailedEvent(stopwatch.elapsedMillis());
            }
            for (WriteOp op : oldPendingOps) {
                executeOp(op, success);
            }
        }

        /**
         * Remove myself from streams
         */
        private void removeMySelf(Throwable t) {
            if (streams.remove(name, this)) {
                logger.info("Remove myself {} for stream {} due to exception : ",
                        new Object[] { this, name, t });
                close();
            }
        }

        synchronized Pair<DistributedLogManager, AsyncLogWriter> setStreamStatus(
                StreamStatus status, DistributedLogManager manager, AsyncLogWriter writer,
                String owner, Throwable t) {
            DistributedLogManager oldManager = this.manager;
            this.manager = manager;
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
            return Pair.of(oldManager, oldWriter);
        }

        void close(Pair<DistributedLogManager, AsyncLogWriter> pair) {
            if (null != pair) {
                if (null != pair.getLast()) {
                    try {
                        pair.getLast().close();
                    } catch (IOException ioe) {
                        logger.warn("Failed to close async log writer : ", ioe);
                    }
                }
                if (null != pair.getFirst()) {
                    try {
                        pair.getFirst().close();
                    } catch (IOException e) {
                        logger.warn("Failed to close manager : ", e);
                    }
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

        void close() {
            logger.info("Closing stream {} ...", name);
            close(Pair.of(manager, writer));
            logger.info("Closed stream {}.", name);
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSED == status) {
                    return;
                }
                status = StreamStatus.CLOSED;
            } finally {
                closeLock.writeLock().unlock();
            }
            Queue<WriteOp> oldPendingOps;
            synchronized (this) {
                oldPendingOps = pendingOps;
                pendingOps = new ArrayDeque<WriteOp>();
            }
            for (WriteOp op : oldPendingOps) {
                op.result.setValue(writeResponse(responseHeader(
                        StatusCode.SERVICE_UNAVAILABLE, "Stream " + name + " is closed.")));
            }
        }
    }

    private final DistributedLogConfiguration dlConfig;
    private final DistributedLogManagerFactory dlFactory;
    private final ConcurrentHashMap<String, Stream> streams =
            new ConcurrentHashMap<String, Stream>();

    private boolean closed = false;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final CountDownLatch keepAliveLatch;
    private final Object txnLock = new Object();

    // Stats
    private final OpStatsLogger requestStat;
    private final Counter redirects;
    private final StatsLogger exceptionStatLogger;
    // exception stats
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
        this.serverMode = ServerMode.valueOf(dlConf.getString("server_mode", ServerMode.MEM.toString()));
        int serverPort = dlConf.getInt("server_port", 0);
        int shard = dlConf.getInt("server_shard", -1);
        int numThreads = dlConf.getInt("server_threads", Runtime.getRuntime().availableProcessors());
        this.clientId = DLSocketAddress.toLockId(DLSocketAddress.getSocketAddress(serverPort), shard);
        this.dlFactory = new DistributedLogManagerFactory(dlConf, uri, statsLogger, clientId);
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
        StatsLogger requestsStatsLogger = statsLogger.scope("write");
        this.requestStat = requestsStatsLogger.getOpStatsLogger("request");
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
                return streams.size();
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

        logger.info("Running distributedlog server in {} mode, delay ms {}, client id {}.",
                new Object[] { serverMode, delayMs, clientId });
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

    Stream getLogWriter(String stream) {
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
                    writer.initialize(0);
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
        Stream stream = getLogWriter(name);
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
