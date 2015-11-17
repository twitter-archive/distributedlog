package com.twitter.distributedlog.v2.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.v2.DistributedLogConfiguration;
import com.twitter.distributedlog.v2.DistributedLogManager;
import com.twitter.distributedlog.v2.DistributedLogManagerFactory;
import com.twitter.distributedlog.v2.LogReader;
import com.twitter.distributedlog.v2.LogRecord;
import com.twitter.distributedlog.v2.benchmark.thrift.Message;
import com.twitter.distributedlog.v2.util.SchedulerUtils;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReaderWorker implements Worker {

    static final Logger LOG = LoggerFactory.getLogger(ReaderWorker.class);

    static final int BACKOFF_MS = 200;

    final String streamPrefix;
    final int startStreamId;
    final int endStreamId;
    final ScheduledExecutorService executorService;
    final DistributedLogManagerFactory factory;
    final DistributedLogManager[] dlms;
    final LogReader[] logReaders;
    final StreamReader[] streamReaders;
    final int numStreams;

    volatile boolean running = true;

    final StatsLogger statsLogger;
    final OpStatsLogger e2eStat;
    final OpStatsLogger deliveryStat;
    final OpStatsLogger negativeE2EStat;
    final OpStatsLogger negativeDeliveryStat;
    final OpStatsLogger truncationStat;
    final Counter invalidRecordsCounter;
    final Counter outOfOrderSequenceIdCounter;
    final Counter emptyResponseCounter;

    class StreamReader implements Runnable {

        final int streamIdx;
        final String streamName;

        StreamReader(int idx) {
            this.streamIdx = idx;
            int streamId = startStreamId + streamIdx;
            streamName = String.format("%s_%d", streamPrefix, streamId);
        }

        private void onSuccess(final LogRecord record) {
            Message msg;
            try {
                msg = Utils.parseMessage(record.getPayload());
            } catch (TException e) {
                invalidRecordsCounter.inc();
                LOG.warn("Failed to parse record {} for stream {} : size = {} , ",
                         new Object[] { record, streamIdx, record.getPayload().length, e });
                return;
            }
            long curTimeMillis = System.currentTimeMillis();
            long e2eLatency = curTimeMillis - msg.getPublishTime();
            long deliveryLatency = curTimeMillis - record.getTransactionId();
            if (e2eLatency >= 0) {
                e2eStat.registerSuccessfulEvent(e2eLatency);
            } else {
                negativeE2EStat.registerSuccessfulEvent(-e2eLatency);
            }
            if (deliveryLatency >= 0) {
                deliveryStat.registerSuccessfulEvent(deliveryLatency);
            } else {
                negativeDeliveryStat.registerSuccessfulEvent(-deliveryLatency);
            }
        }

        @Override
        public void run() {
            reinitStream(streamIdx);
            while (running) {
                readNext();
            }
        }

        private void readNext() {
            LogRecord record;
            try {
                record = logReaders[streamIdx].readNext(true);
                if (null == record) {
                    emptyResponseCounter.inc();
                } else {
                    onSuccess(record);
                }
            } catch (IOException e) {
                reinitStream(streamIdx);
            }
        }
    }

    public ReaderWorker(DistributedLogConfiguration conf,
                        URI uri,
                        String streamPrefix,
                        int startStreamId,
                        int endStreamId,
                        int readThreadPoolSize,
                        StatsLogger statsLogger) throws IOException {
        Preconditions.checkArgument(startStreamId <= endStreamId);
        this.streamPrefix = streamPrefix;
        this.startStreamId = startStreamId;
        this.endStreamId = endStreamId;
        this.statsLogger = statsLogger;
        this.e2eStat = this.statsLogger.getOpStatsLogger("e2e");
        this.negativeE2EStat = this.statsLogger.getOpStatsLogger("e2eNegative");
        this.deliveryStat = this.statsLogger.getOpStatsLogger("delivery");
        this.negativeDeliveryStat = this.statsLogger.getOpStatsLogger("deliveryNegative");
        this.truncationStat = this.statsLogger.getOpStatsLogger("truncation");
        this.emptyResponseCounter = this.statsLogger.getCounter("empty_resonse");
        this.invalidRecordsCounter = this.statsLogger.getCounter("invalid_records");
        this.outOfOrderSequenceIdCounter = this.statsLogger.getCounter("out_of_order_seq_id");
        this.numStreams = endStreamId - startStreamId;
        this.executorService = Executors.newScheduledThreadPool(
            numStreams, new ThreadFactoryBuilder().setNameFormat("benchmark.reader-%d").build());

        // construct the factory
        this.factory = new DistributedLogManagerFactory(conf, uri, statsLogger.scope("dl"));
        this.dlms = new DistributedLogManager[numStreams];
        this.logReaders = new LogReader[numStreams];
        this.streamReaders = new StreamReader[numStreams];
        for (int i = 0; i < numStreams; i++) {
            streamReaders[i] = new StreamReader(i);
            executorService.submit(streamReaders[i]);
        }
        LOG.info("Initialized benchmark reader on {} streams {} : [{} - {})",
                 new Object[] { numStreams, streamPrefix, startStreamId, endStreamId });
    }

    private void reinitStream(int idx) {
        int streamId = startStreamId + idx;
        String streamName = String.format("%s_%d", streamPrefix, streamId);

        if (logReaders[idx] != null) {
            try {
                logReaders[idx].close();
            } catch (IOException e) {
                LOG.warn("Failed on closing stream reader {} : ", streamName, e);
            }
            logReaders[idx] = null;
        }
        if (dlms[idx] != null) {
            try {
                dlms[idx].close();
            } catch (IOException e) {
                LOG.warn("Failed on closing dlm {} : ", streamName, e);
            }
            dlms[idx] = null;
        }

        try {
            dlms[idx] = factory.createDistributedLogManager(streamName,
                    DistributedLogManagerFactory.ClientSharingOption.SharedClients);
        } catch (IOException ioe) {
            LOG.error("Failed on creating dlm {} : ", streamName, ioe);
            backoffAndReinitStream(idx);
            return;
        }
        long txid;
        try {
            txid = dlms[idx].getLastTxId();
        } catch (IOException ioe) {
            LOG.error("Failed on getting last dlsn from stream {} : ", streamName, ioe);
            backoffAndReinitStream(idx);
            return;
        }
        try {
            logReaders[idx] = dlms[idx].getInputStream(txid);
        } catch (IOException ioe) {
            LOG.error("Failed on opening reader for stream {} starting from {} : ",
                      new Object[] { streamName, txid, ioe });
            backoffAndReinitStream(idx);
            return;
        }
        LOG.info("Opened reader for stream {}, catching from {}.", streamName, txid);
        // catching up
        LogRecord record;
        try {
            record = logReaders[idx].readNext(true);
            txid = dlms[idx].getLastTxId();
            LOG.info("Reader catching up on reading stream {}, starting tx id = {}, last tx id = {}",
                    new Object[] { idx, record.getTransactionId(), txid });
            int count = 0;
            while ((record = logReaders[idx].readNext(true)) != null && record.getTransactionId() < txid) {
                ++count;
                if (count % 1000 == 0) {
                    LOG.info("Catching up {} records for stream {}", count, idx);
                }
            }
            LOG.info("Reader caught up on reading stream {} : tx id = {}", idx, txid);
        } catch (IOException ioe) {
            LOG.error("Failed on getting last dlsn from stream {} : ", streamName, ioe);
            backoffAndReinitStream(idx);
            return;
        }
    }

    void backoffAndReinitStream(final int idx) {
        LOG.info("Backoff and reinitializing stream {}.", idx);
        try {
            Thread.sleep(BACKOFF_MS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted on re-initializing stream {}", idx, e);
        }
        reinitStream(idx);
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        for (LogReader reader : logReaders) {
            if (null != reader) {
                reader.close();
            }
        }
        for (DistributedLogManager dlm : dlms) {
            if (null != dlm) {
                dlm.close();
            }
        }
        factory.close();
        SchedulerUtils.shutdownScheduler(executorService, 2, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        // nope
    }
}
