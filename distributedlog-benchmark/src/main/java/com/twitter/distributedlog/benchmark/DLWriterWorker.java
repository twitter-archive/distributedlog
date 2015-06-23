package com.twitter.distributedlog.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.util.FutureEventListener;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DLWriterWorker implements Worker {

    static final Logger LOG = LoggerFactory.getLogger(DLWriterWorker.class);

    static final int BACKOFF_MS = 200;

    final String streamPrefix;
    final int startStreamId;
    final int endStreamId;
    final int writeRate;
    final int writeConcurrency;
    final int messageSizeBytes;
    final ExecutorService executorService;
    final ScheduledExecutorService rescueService;
    final RateLimiter rateLimiter;
    final Random random;
    final DistributedLogNamespace namespace;
    final List<DistributedLogManager> dlms;
    final List<AsyncLogWriter> streamWriters;
    final int numStreams;

    volatile boolean running = true;

    final StatsLogger statsLogger;
    final OpStatsLogger requestStat;

    public DLWriterWorker(DistributedLogConfiguration conf,
                          URI uri,
                          String streamPrefix,
                          int startStreamId,
                          int endStreamId,
                          int writeRate,
                          int writeConcurrency,
                          int messageSizeBytes,
                          StatsLogger statsLogger) throws IOException {
        Preconditions.checkArgument(startStreamId <= endStreamId);
        this.streamPrefix = streamPrefix;
        this.startStreamId = startStreamId;
        this.endStreamId = endStreamId;
        this.writeRate = writeRate;
        this.writeConcurrency = writeConcurrency;
        this.messageSizeBytes = messageSizeBytes;
        this.statsLogger = statsLogger;
        this.requestStat = this.statsLogger.getOpStatsLogger("requests");
        this.executorService = Executors.newCachedThreadPool();
        this.rescueService = Executors.newSingleThreadScheduledExecutor();
        this.rateLimiter = RateLimiter.create(writeRate);
        this.random = new Random(System.currentTimeMillis());

        this.namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .statsLogger(statsLogger.scope("dl"))
                .build();
        this.numStreams = endStreamId - startStreamId;
        dlms = new ArrayList<DistributedLogManager>(numStreams);
        streamWriters = new ArrayList<AsyncLogWriter>(numStreams);
        final ConcurrentMap<String, AsyncLogWriter> writers = new ConcurrentHashMap<String, AsyncLogWriter>();
        final CountDownLatch latch = new CountDownLatch(this.numStreams);
        for (int i = startStreamId; i < endStreamId; i++) {
            final String streamName = String.format("%s_%d", streamPrefix, i);
            final DistributedLogManager dlm = namespace.openLog(streamName);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
                        if (null != writers.putIfAbsent(streamName, writer)) {
                            writer.close();
                        }
                        latch.countDown();
                    } catch (IOException e) {
                        LOG.error("Failed to intialize writer for stream : {}", streamName, e);
                    }

                }
            });
            dlms.add(dlm);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new IOException("Interrupted on initializing writers for streams.", e);
        }
        for (int i = startStreamId; i < endStreamId; i++) {
            final String streamName = String.format("%s_%d", streamPrefix, i);
            AsyncLogWriter writer = writers.get(streamName);
            if (null == writer) {
                throw new IOException("Writer for " + streamName + " never initialized.");
            }
            streamWriters.add(writer);
        }
        LOG.info("Writing to {} streams.", numStreams);
    }

    void rescueWriter(int idx, AsyncLogWriter writer) {
        if (streamWriters.get(idx) == writer) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Failed to close writer for stream {}.", idx);
            }
            AsyncLogWriter newWriter = null;
            try {
                newWriter = dlms.get(idx).startAsyncLogSegmentNonPartitioned();
            } catch (IOException e) {
                LOG.error("Failed to create new writer for stream {}, backoff for {} ms.",
                          idx, BACKOFF_MS);
                scheduleRescue(idx, writer, BACKOFF_MS);
            }
            streamWriters.set(idx, newWriter);
        } else {
            LOG.warn("AsyncLogWriter for stream {} was already rescued.", idx);
        }
    }

    void scheduleRescue(final int idx, final AsyncLogWriter writer, int delayMs) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                rescueWriter(idx, writer);
            }
        };
        if (delayMs > 0) {
            rescueService.schedule(r, delayMs, TimeUnit.MILLISECONDS);
        } else {
            rescueService.submit(r);
        }
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.rescueService, 2, TimeUnit.MINUTES);
        for (AsyncLogWriter writer : streamWriters) {
            writer.close();
        }
        for (DistributedLogManager dlm : dlms) {
            dlm.close();
        }
        namespace.close();
    }

    @Override
    public void run() {
        LOG.info("Starting dlwriter (rate = {}, concurrency = {}, prefix = {}, numStreams = {})",
                 new Object[] { writeRate, writeConcurrency, streamPrefix, numStreams });
        for (int i = 0; i < writeConcurrency; i++) {
            executorService.submit(new Writer(i));
        }
    }

    class Writer implements Runnable {

        final int idx;

        Writer(int idx) {
            this.idx = idx;
        }

        @Override
        public void run() {
            LOG.info("Started writer {}.", idx);
            while (running) {
                final int streamIdx = random.nextInt(numStreams);
                final AsyncLogWriter writer = streamWriters.get(streamIdx);
                rateLimiter.acquire();
                final long requestMillis = System.currentTimeMillis();
                final byte[] data;
                try {
                    data = Utils.generateMessage(requestMillis, messageSizeBytes);
                } catch (TException e) {
                    LOG.error("Error on generating message : ", e);
                    break;
                }
                writer.write(new LogRecord(requestMillis, data)).addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        requestStat.registerSuccessfulEvent(System.currentTimeMillis() - requestMillis);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        requestStat.registerFailedEvent(System.currentTimeMillis() - requestMillis);
                        LOG.error("Failed to publish, rescue it : ", cause);
                        scheduleRescue(streamIdx, writer, 0);
                    }
                });
            }
        }
    }

}
