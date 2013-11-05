package com.twitter.distributedlog.service;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class DistributedLogServiceImpl implements DistributedLogService.ServiceIface {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServiceImpl.class);

    static final int MAX_RETRIES = 3;

    static class ManagerWithLogWriter {
        final DistributedLogManager manager;
        final AsyncLogWriter writer;

        ManagerWithLogWriter(DistributedLogManager manager, AsyncLogWriter writer) {
            this.manager = manager;
            this.writer = writer;
        }

        void close() {
            try {
                manager.close();
            } catch (IOException e) {
                logger.warn("Failed to close manager : ", e);
            }
        }
    }

    static enum ServerMode {
        DURABLE,
        MEM
    }

    private final DistributedLogManagerFactory dlFactory;
    private final ConcurrentHashMap<String, ManagerWithLogWriter> streams =
            new ConcurrentHashMap<String, ManagerWithLogWriter>();

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
        if (ServerMode.MEM.equals(serverMode)) {
            this.executorService = Executors.newScheduledThreadPool(
                    Runtime.getRuntime().availableProcessors(),
                    new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Executor-%d").build());
            this.delayMs = dlConf.getLong("latency_delay", 0);
        } else {
            this.executorService = null;
            this.delayMs = 0;
        }
        logger.info("Running distributedlog server in {} mode, delay ms {}.", serverMode, delayMs);
    }

    ManagerWithLogWriter getLogWriter(String stream) throws IOException {
        ManagerWithLogWriter writer = streams.get(stream);
        if (null == writer) {
            // TODO: from the load test, it takes up to 2 second to initialize. need to optimize it or move
            //       it to background threads.
            DistributedLogManager dlManager = dlFactory.createDistributedLogManagerWithSharedClients(stream);
            dlManager.recover();
            AsyncLogWriter w = dlManager.asyncStartLogSegmentNonPartitioned();
            writer = new ManagerWithLogWriter(dlManager, w);
            ManagerWithLogWriter oldWriter = streams.putIfAbsent(stream, writer);
            if (null != oldWriter) {
                writer.close();
                writer = oldWriter;
            }
        }
        return writer;
    }

    void removeLogWriter(String stream, ManagerWithLogWriter writer) {
        if (streams.remove(stream, writer)) {
            logger.info("Remove log writer {} of stream {}.", writer, stream);
        }
    }

    @Override
    public Future<String> write(final String stream, ByteBuffer data) {
        final Promise<String> result = new Promise<String>();
        closeLock.readLock().lock();
        try {
            if (closed) {
                // TODO: better exception
                result.setException(new IOException("Server is closing."));
            } else {
                doWrite(stream, data, result, new AtomicInteger(0), new Stopwatch().start());
            }
        } finally {
            closeLock.readLock().unlock();
        }
        return result;
    }

    private void doWrite(final String stream, ByteBuffer data,
                         final Promise<String> result,
                         final AtomicInteger retries,
                         final Stopwatch stopwatch) {
        ManagerWithLogWriter writer;
        try {
            writer = getLogWriter(stream);
        } catch (IOException e) {
            logger.error("Failed to get log writer of stream {} : ", stream, e);
            requestStat.registerFailedEvent(stopwatch.stop().elapsedMillis());
            result.setException(e);
            return;
        }

        byte[] payload = new byte[data.remaining()];
        data.get(payload);
        try {
            long txnId;
            Future<DLSN> writeResult;
            synchronized (txnLock) {
                txnId = System.currentTimeMillis();
                writeResult = writer.writer.write(new LogRecord(txnId, payload));
            }
            if (serverMode.equals(ServerMode.DURABLE)) {
                writeResult.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                        result.setValue(value.serialize());
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        requestStat.registerFailedEvent(stopwatch.stop().elapsedMillis());
                        // TODO: handle redirection.
                        logger.error("Failed to write data into stream {} : ", stream, cause);
                        result.setException(cause);
                    }
                });
            } else {
                if (null != executorService && delayMs > 0) {
                    final long txnIdToReturn = txnId;
                    executorService.schedule(new Runnable() {
                        @Override
                        public void run() {
                            requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                            result.setValue(Long.toString(txnIdToReturn));
                        }
                    }, delayMs, TimeUnit.MILLISECONDS);
                } else {
                    requestStat.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                    result.setValue(Long.toString(txnId));
                }
            }
        } catch (IOException e) {
            logger.error("Failed to write data into stream {} : ", stream, e);
            removeLogWriter(stream, writer);
            if (retries.incrementAndGet() >= MAX_RETRIES) {
                requestStat.registerFailedEvent(stopwatch.stop().elapsedMillis());
                result.setException(e);
            } else {
                // zookeeper session expired
                doWrite(stream, data, result, retries, stopwatch);
            }
        }
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
        for (ManagerWithLogWriter writer : streams.values()) {
            writer.close();
        }
    }
}
