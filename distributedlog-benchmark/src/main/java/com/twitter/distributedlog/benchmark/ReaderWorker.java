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
package com.twitter.distributedlog.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordSet;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.benchmark.thrift.Message;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.util.FutureUtils;
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
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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
    final ExecutorService callbackExecutor;
    final DistributedLogNamespace namespace;
    final DistributedLogManager[] dlms;
    final AsyncLogReader[] logReaders;
    final StreamReader[] streamReaders;
    final int numStreams;
    final boolean readFromHead;

    final int truncationIntervalInSeconds;
    // DL Client Related Variables
    final DLZkServerSet[] serverSets;
    final List<String> finagleNames;
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

    class StreamReader implements FutureEventListener<List<LogRecordWithDLSN>>, Runnable, Gauge<Number> {

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
        public void onSuccess(final List<LogRecordWithDLSN> records) {
            for (final LogRecordWithDLSN record : records) {
                if (record.isRecordSet()) {
                    try {
                        processRecordSet(record);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                } else {
                    processRecord(record);
                }
            }
            readLoop();
        }

        public void processRecordSet(final LogRecordWithDLSN record) throws IOException {
            LogRecordSet.Reader reader = LogRecordSet.of(record);
            LogRecordWithDLSN nextRecord = reader.nextRecord();
            while (null != nextRecord) {
                processRecord(nextRecord);
                nextRecord = reader.nextRecord();
            }
        }

        public void processRecord(final LogRecordWithDLSN record) {
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

            prevDLSN = record.getDlsn();
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
            logReaders[streamIdx].readBulk(10).addEventListener(this);
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
                        List<String> finagleNames,
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
        this.callbackExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("benchmark.reader-callback-%d").build());
        this.finagleNames = finagleNames;
        this.serverSets = createServerSets(serverSetPaths);

        conf.setDeserializeRecordSetOnReads(false);

        if (truncationIntervalInSeconds > 0 && (!finagleNames.isEmpty() || !serverSetPaths.isEmpty())) {
            // Construct client for truncation
            DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder()
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
                    .name("reader");

            if (serverSetPaths.isEmpty()) {
                // Prepare finagle names
                String local = finagleNames.get(0);
                String[] remotes = new String[finagleNames.size() - 1];
                finagleNames.subList(1, finagleNames.size()).toArray(remotes);

                builder = builder.finagleNameStrs(local, remotes);
                LOG.info("Initialized distributedlog client for truncation @ {}.", finagleNames);
            } else if (serverSets.length != 0){
                ServerSet local = this.serverSets[0].getServerSet();
                ServerSet[] remotes = new ServerSet[this.serverSets.length - 1];
                for (int i = 1; i < serverSets.length; i++) {
                    remotes[i-1] = serverSets[i].getServerSet();
                }

                builder = builder.serverSets(local, remotes);
                LOG.info("Initialized distributedlog client for truncation @ {}.", serverSetPaths);
            } else {
                builder = builder.uri(uri);
                LOG.info("Initialized distributedlog client for namespace {}", uri);
            }
            dlc = builder.build();
        } else {
            dlc = null;
        }

        // construct the factory
        this.namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .statsLogger(statsLogger.scope("dl"))
                .build();
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

    protected DLZkServerSet[] createServerSets(List<String> serverSetPaths) {
        DLZkServerSet[] serverSets = new DLZkServerSet[serverSetPaths.size()];
        for (int i = 0; i < serverSets.length; i++) {
            String serverSetPath = serverSetPaths.get(i);
            serverSets[i] = DLZkServerSet.of(URI.create(serverSetPath), 60000);
        }
        return serverSets;
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
                FutureUtils.result(logReaders[idx].asyncClose());
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
            dlms[idx] = namespace.openLog(streamName);
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
                FutureUtils.result(reader.asyncClose());
            }
        }
        for (DistributedLogManager dlm : dlms) {
            if (null != dlm) {
                dlm.close();
            }
        }
        namespace.close();
        SchedulerUtils.shutdownScheduler(executorService, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(callbackExecutor, 2, TimeUnit.MINUTES);
        if (this.dlc != null) {
            this.dlc.close();
        }
        for (DLZkServerSet serverSet: serverSets) {
            serverSet.close();
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
