package com.twitter.distributedlog.v2.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogWriter;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.distributedlog.v2.DistributedLogConfiguration;
import com.twitter.distributedlog.v2.DistributedLogManagerFactory;
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
    final DistributedLogManagerFactory factory;
    final List<DistributedLogManager> dlms;
    final List<LogWriter> streamWriters;
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

        this.factory = new DistributedLogManagerFactory(conf, uri, statsLogger.scope("dl"));
        this.numStreams = endStreamId - startStreamId;
        dlms = new ArrayList<DistributedLogManager>(numStreams);
        streamWriters = new ArrayList<LogWriter>(numStreams);
        final ConcurrentMap<String, LogWriter> writers = new ConcurrentHashMap<String, LogWriter>();
        final CountDownLatch latch = new CountDownLatch(this.numStreams);
        for (int i = startStreamId; i < endStreamId; i++) {
            final String streamName = String.format("%s_%d", streamPrefix, i);
            final DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        LogWriter writer = dlm.startLogSegmentNonPartitioned();
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
            LogWriter writer = writers.get(streamName);
            if (null == writer) {
                throw new IOException("Writer for " + streamName + " never initialized.");
            }
            streamWriters.add(writer);
        }
        LOG.info("Writing to {} streams.", numStreams);
    }

    void rescueWriter(int idx, LogWriter writer) {
        if (streamWriters.get(idx) == writer) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Failed to close writer for stream {}.", idx);
            }
            LogWriter newWriter = null;
            try {
                newWriter = dlms.get(idx).startLogSegmentNonPartitioned();
            } catch (IOException e) {
                LOG.error("Failed to create new writer for stream {}, backoff for {} ms.",
                          idx, BACKOFF_MS);
                try {
                    Thread.sleep(BACKOFF_MS);
                } catch (InterruptedException ie) {
                    LOG.warn("Interrupted on rescuing stream {}", idx, ie);
                }
            }
            streamWriters.set(idx, newWriter);
        } else {
            LOG.warn("LogWriter for stream {} was already rescued.", idx);
        }
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.rescueService, 2, TimeUnit.MINUTES);
        for (LogWriter writer : streamWriters) {
            writer.close();
        }
        for (DistributedLogManager dlm : dlms) {
            dlm.close();
        }
        factory.close();
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
                final LogWriter writer = streamWriters.get(streamIdx);
                rateLimiter.acquire();
                final long requestMillis = System.currentTimeMillis();
                final byte[] data;
                try {
                    data = Utils.generateMessage(requestMillis, messageSizeBytes);
                } catch (TException e) {
                    LOG.error("Error on generating message : ", e);
                    break;
                }
                try {
                    writer.write(new LogRecord(requestMillis, data));
                    requestStat.registerSuccessfulEvent(System.currentTimeMillis() - requestMillis);
                } catch (IOException e) {
                    requestStat.registerFailedEvent(System.currentTimeMillis() - requestMillis);
                    rescueWriter(streamIdx, writer);
                }
            }
        }
    }

}
