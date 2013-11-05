package com.twitter.distributedlog.service;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class DistributedLogServiceImpl implements DistributedLogService.ServiceIface {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServiceImpl.class);

    static final int MAX_RETRIES = 3;

    static enum StreamStatus {
        INITIALIZING,
        INITIALIZED,
        FAILED
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

    static ResponseHeader exceptionToResponseHeader(Throwable t) {
        ResponseHeader response = new ResponseHeader();
        if (t instanceof DLException) {
            DLException dle = (DLException) t;
            if (dle instanceof OwnershipAcquireFailedException) {
                response.setLocation(((OwnershipAcquireFailedException) dle).getCurrentOwner());
            }
            response.setCode(dle.getCode());
        } else if (t instanceof IOException) {
            response.setCode(StatusCode.INTERNAL_SERVER_ERROR);
        } else {
            response.setCode(StatusCode.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    static WriteResponse writeResponse(ResponseHeader responseHeader) {
        return new WriteResponse(responseHeader);
    }

    class WriteOp {
        final String stream;
        final byte[] payload;
        final Promise<WriteResponse> result = new Promise<WriteResponse>();
        final AtomicInteger retries = new AtomicInteger(0);
        final Stopwatch stopwatch = new Stopwatch();

        WriteOp(String stream, ByteBuffer data) {
            this.stream = stream;
            payload = new byte[data.remaining()];
            data.get(payload);
            stopwatch.start();
        }

        void execute(AsyncLogWriter writer) throws IOException {
            long txnId;
            Future<DLSN> writeResult;
            synchronized (txnLock) {
                txnId = System.currentTimeMillis();
                writeResult = writer.write(new LogRecord(txnId, payload));
            }
            if (serverMode.equals(ServerMode.DURABLE)) {
                writeResult.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                        result.setValue(writeResponse(successResponseHeader()).setDlsn(value.serialize()));
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        requestStat.registerFailedEvent(stopwatch.stop().elapsedMillis());
                        logger.error("Failed to write data into stream {} : ", stream, cause);
                        result.setValue(writeResponse(exceptionToResponseHeader(cause)));
                    }
                });
            } else {
                if (delayMs > 0) {
                    final long txnIdToReturn = txnId;
                    executorService.schedule(new Runnable() {
                        @Override
                        public void run() {
                            requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                            result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                        }
                    }, delayMs, TimeUnit.MILLISECONDS);
                } else {
                    requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                    result.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnId)));
                }
            }
        }

        void fail(String owner, Throwable t) {
            requestStat.registerFailedEvent(stopwatch.stop().elapsedMillis());
            if (null != owner) {
                logger.info("Redirect write to stream {} to {}.", stream, owner);
                result.setValue(writeResponse(ownerToResponseHeader(owner)));
            } else {
                logger.error("Fail to write stream {} : ", stream, t);
                result.setValue(writeResponse(exceptionToResponseHeader(t)));
            }
        }

    }

    class Stream implements Runnable {
        final String name;

        volatile DistributedLogManager manager;
        volatile AsyncLogWriter writer;
        volatile StreamStatus status;
        volatile String owner;
        volatile Throwable lastException;

        private volatile Queue<WriteOp> pendingOps = new ArrayDeque<WriteOp>();

        Stream(String name) {
            this.name = name;
            status = StreamStatus.INITIALIZING;
            lastException = new IOException("Fail to write record to stream " + name);
        }

        void initialize() {
            executorService.submit(this);
        }

        void waitIfNeededAndWrite(WriteOp op) {
            boolean completeOpNow = false;
            boolean success = true;
            if (StreamStatus.INITIALIZED == status && writer != null) {
                completeOpNow = true;
                success = true;
            } else {
                synchronized (this) {
                    if (StreamStatus.INITIALIZED == status) {
                        completeOpNow = true;
                        success = true;
                    } else if (StreamStatus.FAILED == status) {
                        completeOpNow = true;
                        success = false;
                    } else {
                        pendingOps.add(op);
                    }
                }
            }
            if (completeOpNow) {
                executeOp(op, success);
            }
        }

        void executeOp(WriteOp op, boolean success) {
            try{
                AsyncLogWriter w = writer;
                if (null != w && success) {
                    op.execute(w);
                } else {
                    logger.error("Failed op : writer {}, success {}.", w, success);
                    op.fail(owner, lastException);
                }
            } catch (final OwnershipAcquireFailedException oafe) {
                logger.error("Failed to write data into stream {} : ", name, oafe);
                DistributedLogManager managerToClose = null;
                synchronized (this) {
                    if (StreamStatus.INITIALIZED == status) {
                        managerToClose =
                                setStreamStatus(StreamStatus.FAILED, null, null, oafe.getCurrentOwner(), oafe);
                    }
                }
                op.fail(owner, lastException);
                close(managerToClose);
                // cache the ownership for a while
                executorService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        removeMySelf(oafe);
                    }
                }, 60, TimeUnit.SECONDS);
            } catch (IOException e) {
                logger.error("Failed to write data into stream {} : ", name, e);
                DistributedLogManager managerToClose = null;
                boolean reinitialized = false;
                synchronized (this) {
                    if (StreamStatus.INITIALIZED == status) {
                        managerToClose =
                                setStreamStatus(StreamStatus.INITIALIZING, null, null, null, e);
                        reinitialized = true;
                    }
                }
                close(managerToClose);
                if (reinitialized) {
                    initialize();
                }
                if (op.retries.incrementAndGet() >= MAX_RETRIES) {
                    op.fail(owner, lastException);
                } else {
                    waitIfNeededAndWrite(op);
                }
            }
        }

        @Override
        public void run() {
            Queue<WriteOp> oldPendingOps;
            boolean success;
            try {
                DistributedLogManager dlManager = dlFactory.createDistributedLogManagerWithSharedClients(name);
                dlManager.recover();
                AsyncLogWriter w = dlManager.asyncStartLogSegmentNonPartitioned();
                synchronized (this) {
                    setStreamStatus(StreamStatus.INITIALIZED, dlManager, w, null, null);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<WriteOp>();
                    success = true;
                }
            } catch (final OwnershipAcquireFailedException oafe) {
                logger.error("Failed to initialize stream {} : ", name, oafe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED, null, null, oafe.getCurrentOwner(), oafe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<WriteOp>();
                    success = false;
                }
                // cache the ownership for a while
                executorService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        removeMySelf(oafe);
                    }
                }, 60, TimeUnit.SECONDS);
            } catch (IOException ioe) {
                logger.error("Failed to initialize stream {} : ", name, ioe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED, null, null, null, ioe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<WriteOp>();
                    success = false;
                }
                removeMySelf(ioe);
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
            }
        }

        synchronized DistributedLogManager setStreamStatus(
                StreamStatus status, DistributedLogManager manager, AsyncLogWriter writer,
                String owner, Throwable t) {
            DistributedLogManager oldManager = this.manager;
            this.manager = manager;
            this.writer = writer;
            this.owner = owner;
            this.lastException = t;
            this.status = status;
            return oldManager;
        }

        void close(DistributedLogManager manager) {
            if (null != manager) {
                try {
                    manager.close();
                } catch (IOException e) {
                    logger.warn("Failed to close manager : ", e);
                }
            }
        }

        void close() {
            close(manager);
        }
    }

    private final DistributedLogManagerFactory dlFactory;
    private final ConcurrentHashMap<String, Stream> streams =
            new ConcurrentHashMap<String, Stream>();

    private boolean closed = false;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final Object txnLock = new Object();

    // Stats
    private final OpStatsLogger requestStat;

    private final ServerMode serverMode;
    private final ScheduledExecutorService executorService;
    private final long delayMs;

    DistributedLogServiceImpl(DistributedLogConfiguration dlConf, URI uri, StatsLogger statsLogger)
            throws IOException {
        this.dlFactory = new DistributedLogManagerFactory(dlConf, uri, statsLogger);
        StatsLogger requestsStatsLogger = statsLogger.scope("write");
        this.requestStat = requestsStatsLogger.getOpStatsLogger("request");
        this.serverMode = ServerMode.valueOf(dlConf.getString("server_mode", ServerMode.MEM.toString()));
        this.executorService = Executors.newScheduledThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Executor-%d").build());
        if (ServerMode.MEM.equals(serverMode)) {
            this.delayMs = dlConf.getLong("latency_delay", 0);
        } else {
            this.delayMs = 0;
        }
        logger.info("Running distributedlog server in {} mode, delay ms {}.", serverMode, delayMs);
    }

    Stream getLogWriter(String stream) {
        Stream writer = streams.get(stream);
        if (null == writer) {
            writer = new Stream(stream);
            Stream oldWriter = streams.putIfAbsent(stream, writer);
            if (null != oldWriter) {
                writer.close();
                writer = oldWriter;
            } else {
                writer.initialize();
            }
        }
        return writer;
    }

    @Override
    public Future<WriteResponse> write(final String stream, ByteBuffer data) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return Future.exception(new ServiceUnavailableException("Server is closing."));
            } else {
                return doWrite(stream, data);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private Future<WriteResponse> doWrite(final String stream, ByteBuffer data) {
        WriteOp op = new WriteOp(stream, data);
        getLogWriter(stream).waitIfNeededAndWrite(op);
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
            stream.close();
        }
    }
}
