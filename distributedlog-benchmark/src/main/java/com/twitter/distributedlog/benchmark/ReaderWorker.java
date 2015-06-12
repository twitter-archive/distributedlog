package com.twitter.distributedlog.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.benchmark.thrift.Message;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Duration$;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
    final AsyncLogReader[] logReaders;
    final StreamReader[] streamReaders;
    final int numStreams;
    final boolean readFromHead;

    final int truncationIntervalInSeconds;
    // DL Client Related Variables
    final ZooKeeperClient[] zkClients;
    final ServerSet[] serverSets;
    final DistributedLogClient dlc;

    volatile boolean running = true;

    final StatsReceiver statsReceiver;
    final StatsLogger statsLogger;
    final OpStatsLogger e2eStat;
    final OpStatsLogger deliveryStat;
    final OpStatsLogger negativeE2EStat;
    final OpStatsLogger negativeDeliveryStat;
    final OpStatsLogger truncationStat;
    final Counter invalidRecordsCounter;
    final Counter outOfOrderSequenceIdCounter;

    class StreamReader implements FutureEventListener<LogRecordWithDLSN>, Runnable, Gauge<Number> {

        final int streamIdx;
        final String streamName;
        DLSN prevDLSN = null;
        long prevSequenceId = Long.MIN_VALUE;

        StreamReader(int idx, StatsLogger statsLogger) {
            this.streamIdx = idx;
            int streamId = startStreamId + streamIdx;
            streamName = String.format("%s_%d", streamPrefix, streamId);
            statsLogger.scope(streamName).registerGauge("sequence_id", this);
        }

        @Override
        public void onSuccess(final LogRecordWithDLSN record) {
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
            synchronized (this) {
                if (record.getSequenceId() <= prevSequenceId
                        || (prevSequenceId >= 0L && record.getSequenceId() != prevSequenceId + 1)) {
                    outOfOrderSequenceIdCounter.inc();
                    LOG.warn("Encountered decreasing sequence id for stream {} : previous = {}, current = {}",
                            new Object[]{streamIdx, prevSequenceId, record.getSequenceId()});
                }
                prevSequenceId = record.getSequenceId();
            }
            prevDLSN = record.getDlsn();
            readLoop();
        }

        @Override
        public void onFailure(Throwable cause) {
            scheduleReinitStream(streamIdx).map(new Function<Void, Void>() {
                @Override
                public Void apply(Void value) {
                    prevDLSN = null;
                    prevSequenceId = Long.MIN_VALUE;
                    readLoop();
                    return null;
                }
            });
        }

        void readLoop() {
            if (!running) {
                return;
            }
            logReaders[streamIdx].readNext().addEventListener(this);
        }

        @Override
        public void run() {
            final DLSN dlsnToTruncate = prevDLSN;
            if (null == dlsnToTruncate) {
                return;
            }
            final Stopwatch stopwatch = Stopwatch.createStarted();
            dlc.truncate(streamName, dlsnToTruncate).addEventListener(
                    new FutureEventListener<Boolean>() {
                        @Override
                        public void onSuccess(Boolean value) {
                            truncationStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
                        }

                        @Override
                        public void onFailure(Throwable cause) {
                            truncationStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
                            LOG.error("Failed to truncate stream {} to {} : ",
                                    new Object[]{streamName, dlsnToTruncate, cause});
                        }
                    });
        }

        @Override
        public Number getDefaultValue() {
            return Long.MIN_VALUE;
        }

        @Override
        public synchronized Number getSample() {
            return prevSequenceId;
        }
    }

    public ReaderWorker(DistributedLogConfiguration conf,
                        URI uri,
                        String streamPrefix,
                        int startStreamId,
                        int endStreamId,
                        int readThreadPoolSize,
                        List<String> serverSetPaths,
                        int truncationIntervalInSeconds,
                        boolean readFromHead, /* read from the earliest data of log */
                        StatsReceiver statsReceiver,
                        StatsLogger statsLogger) throws IOException {
        Preconditions.checkArgument(startStreamId <= endStreamId);
        this.streamPrefix = streamPrefix;
        this.startStreamId = startStreamId;
        this.endStreamId = endStreamId;
        this.truncationIntervalInSeconds = truncationIntervalInSeconds;
        this.readFromHead = readFromHead;
        this.statsReceiver = statsReceiver;
        this.statsLogger = statsLogger;
        this.e2eStat = this.statsLogger.getOpStatsLogger("e2e");
        this.negativeE2EStat = this.statsLogger.getOpStatsLogger("e2eNegative");
        this.deliveryStat = this.statsLogger.getOpStatsLogger("delivery");
        this.negativeDeliveryStat = this.statsLogger.getOpStatsLogger("deliveryNegative");
        this.truncationStat = this.statsLogger.getOpStatsLogger("truncation");
        this.invalidRecordsCounter = this.statsLogger.getCounter("invalid_records");
        this.outOfOrderSequenceIdCounter = this.statsLogger.getCounter("out_of_order_seq_id");
        this.executorService = Executors.newScheduledThreadPool(
            readThreadPoolSize, new ThreadFactoryBuilder().setNameFormat("benchmark.reader-%d").build());

        if (truncationIntervalInSeconds > 0 && !serverSetPaths.isEmpty()) {
            zkClients = new ZooKeeperClient[serverSetPaths.size()];
            serverSets = new ServerSet[serverSetPaths.size()];

            for (int i = 0; i < serverSets.length; i++) {
                String serverSetPath = serverSetPaths.get(0);
                Pair<ZooKeeperClient, ServerSet> ssPair = Utils.parseServerSet(serverSetPath);
                this.zkClients[i] = ssPair.getLeft();
                this.serverSets[i] = ssPair.getRight();
            }
            ServerSet local = this.serverSets[0];
            ServerSet[] remotes = new ServerSet[this.serverSets.length - 1];
            System.arraycopy(this.serverSets, 1, remotes, 0, remotes.length);
            dlc = DistributedLogClientBuilder.newBuilder()
                    .clientId(ClientId$.MODULE$.apply("dlog_loadtest_reader"))
                    .clientBuilder(ClientBuilder.get()
                        .hostConnectionLimit(10)
                        .hostConnectionCoresize(10)
                        .tcpConnectTimeout(Duration$.MODULE$.fromSeconds(1))
                        .requestTimeout(Duration$.MODULE$.fromSeconds(2)))
                    .redirectBackoffStartMs(100)
                    .redirectBackoffMaxMs(500)
                    .requestTimeoutMs(2000)
                    .statsReceiver(statsReceiver)
                    .serverSets(local, remotes)
                    .name("reader")
                    .build();
            LOG.info("Initialized distributedlog client for truncation @ {}.", serverSetPaths);
        } else {
            zkClients = new ZooKeeperClient[0];
            serverSets = new ServerSet[0];
            dlc = null;
        }

        // construct the factory
        this.factory = new DistributedLogManagerFactory(conf, uri, statsLogger.scope("dl"));
        this.numStreams = endStreamId - startStreamId;
        this.dlms = new DistributedLogManager[numStreams];
        this.logReaders = new AsyncLogReader[numStreams];
        final CountDownLatch latch = new CountDownLatch(numStreams);
        for (int i = 0; i < numStreams; i++) {
            final int idx = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    reinitStream(idx).map(new Function<Void, Void>() {
                        @Override
                        public Void apply(Void value) {
                            LOG.info("Initialized stream reader {}.", idx);
                            latch.countDown();
                            return null;
                        }
                    });
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Failed to intialize benchmark readers : ", e);
        }
        this.streamReaders = new StreamReader[numStreams];
        for (int i = 0; i < numStreams; i++) {
            streamReaders[i] = new StreamReader(i, statsLogger.scope("perstream"));
            if (truncationIntervalInSeconds > 0) {
                executorService.scheduleWithFixedDelay(streamReaders[i],
                        truncationIntervalInSeconds, truncationIntervalInSeconds, TimeUnit.SECONDS);
            }
        }
        LOG.info("Initialized benchmark reader on {} streams {} : [{} - {})",
                 new Object[] { numStreams, streamPrefix, startStreamId, endStreamId });
    }

    private Future<Void> reinitStream(int idx) {
        Promise<Void> promise = new Promise<Void>();
        reinitStream(idx, promise);
        return promise;
    }

    private void reinitStream(int idx, Promise<Void> promise) {
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
            dlms[idx] = factory.createDistributedLogManagerWithSharedClients(streamName);
        } catch (IOException ioe) {
            LOG.error("Failed on creating dlm {} : ", streamName, ioe);
            scheduleReinitStream(idx, promise);
            return;
        }
        DLSN lastDLSN;
        if (readFromHead) {
            lastDLSN = DLSN.InitialDLSN;
        } else {
            try {
                lastDLSN = dlms[idx].getLastDLSN();
            } catch (IOException ioe) {
                LOG.error("Failed on getting last dlsn from stream {} : ", streamName, ioe);
                scheduleReinitStream(idx, promise);
                return;
            }
        }
        try {
            logReaders[idx] = dlms[idx].getAsyncLogReader(lastDLSN);
        } catch (IOException ioe) {
            LOG.error("Failed on opening reader for stream {} starting from {} : ",
                      new Object[] { streamName, lastDLSN, ioe });
            scheduleReinitStream(idx, promise);
            return;
        }
        LOG.info("Opened reader for stream {}, starting from {}.", streamName, lastDLSN);
        promise.setValue(null);
    }

    Future<Void> scheduleReinitStream(int idx) {
        Promise<Void> promise = new Promise<Void>();
        scheduleReinitStream(idx, promise);
        return promise;
    }

    void scheduleReinitStream(final int idx, final Promise<Void> promise) {
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                reinitStream(idx, promise);
            }
        }, BACKOFF_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        for (AsyncLogReader reader : logReaders) {
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
        if (this.dlc != null) {
            this.dlc.close();
        }
        for (ZooKeeperClient zkClient : zkClients) {
            zkClient.close();
        }
    }

    @Override
    public void run() {
        LOG.info("Starting reader (prefix = {}, numStreams = {}).",
                 streamPrefix, numStreams);
        for (StreamReader sr : streamReaders) {
            sr.readLoop();
        }
    }
}
