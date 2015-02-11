package com.twitter.distributedlog.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.Duration$;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WriterWorker implements Worker {

    static final Logger LOG = LoggerFactory.getLogger(WriterWorker.class);

    final String streamPrefix;
    final int startStreamId;
    final int endStreamId;
    final int writeRate;
    final int writeConcurrency;
    final int messageSizeBytes;
    final int hostConnectionCoreSize;
    final int hostConnectionLimit;
    final ExecutorService executorService;
    final RateLimiter rateLimiter;
    final ZooKeeperClient[] zkClients;
    final ServerSet[] serverSets;
    final Random random;
    final List<String> streamNames;
    final int numStreams;
    final int batchSize;
    final boolean thriftmux;
    final boolean handshakeWithClientInfo;

    volatile boolean running = true;

    final StatsReceiver statsReceiver;
    final StatsLogger statsLogger;
    final OpStatsLogger requestStat;
    final StatsLogger exceptionsLogger;
    final StatsLogger dlErrorCodeLogger;

    public WriterWorker(String streamPrefix,
                        int startStreamId,
                        int endStreamId,
                        int writeRate,
                        int writeConcurrency,
                        int messageSizeBytes,
                        int batchSize,
                        int hostConnectionCoreSize,
                        int hostConnectionLimit,
                        List<String> serverSetPaths,
                        StatsReceiver statsReceiver,
                        StatsLogger statsLogger,
                        boolean thriftmux,
                        boolean handshakeWithClientInfo) {
        Preconditions.checkArgument(startStreamId <= endStreamId);
        Preconditions.checkArgument(serverSetPaths.size() > 0);
        this.streamPrefix = streamPrefix;
        this.startStreamId = startStreamId;
        this.endStreamId = endStreamId;
        this.writeRate = writeRate;
        this.writeConcurrency = writeConcurrency;
        this.messageSizeBytes = messageSizeBytes;
        this.statsReceiver = statsReceiver;
        this.statsLogger = statsLogger;
        this.requestStat = this.statsLogger.getOpStatsLogger("requests");
        this.exceptionsLogger = statsLogger.scope("exceptions");
        this.dlErrorCodeLogger = statsLogger.scope("dl_error_code");
        this.executorService = Executors.newCachedThreadPool();
        this.rateLimiter = RateLimiter.create(writeRate);
        this.random = new Random(System.currentTimeMillis());
        this.batchSize = batchSize;
        this.hostConnectionCoreSize = hostConnectionCoreSize;
        this.hostConnectionLimit = hostConnectionLimit;
        this.thriftmux = thriftmux;
        this.handshakeWithClientInfo = handshakeWithClientInfo;

        // ServerSet
        this.serverSets = new ServerSet[serverSetPaths.size()];
        this.zkClients = new ZooKeeperClient[serverSetPaths.size()];

        for (int i = 0; i < serverSets.length; i++) {
            String serverSetPath = serverSetPaths.get(i);
            Pair<ZooKeeperClient, ServerSet> ssPair = Utils.parseServerSet(serverSetPath);
            this.zkClients[i] = ssPair.getLeft();
            this.serverSets[i] = ssPair.getRight();
        }

        // Streams
        streamNames = new ArrayList<String>(endStreamId - startStreamId);
        for (int i = startStreamId; i < endStreamId; i++) {
            streamNames.add(String.format("%s_%d", streamPrefix, i));
        }
        numStreams = streamNames.size();
        LOG.info("Writing to {} streams : {}", numStreams, streamNames);
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        for (ZooKeeperClient zkClient : zkClients) {
            zkClient.close();
        }
    }

    private DistributedLogClient buildDlogClient() {
        ClientBuilder clientBuilder = ClientBuilder.get()
            .hostConnectionLimit(hostConnectionLimit)
            .hostConnectionCoresize(hostConnectionCoreSize)
            .tcpConnectTimeout(Duration$.MODULE$.fromSeconds(1))
            .requestTimeout(Duration$.MODULE$.fromSeconds(2));

        ClientId clientId = ClientId$.MODULE$.apply("dlog_loadtest_writer");

        ServerSet local = serverSets[0];
        ServerSet[] remotes = new ServerSet[serverSets.length - 1];
        System.arraycopy(serverSets, 1, remotes, 0, remotes.length);

        return DistributedLogClientBuilder.newBuilder()
            .clientId(clientId)
            .clientBuilder(clientBuilder)
            .thriftmux(thriftmux)
            .redirectBackoffStartMs(100)
            .redirectBackoffMaxMs(500)
            .requestTimeoutMs(2000)
            .statsReceiver(statsReceiver)
            .serverSets(local, remotes)
            .streamNameRegex("^" + streamPrefix + "_[0-9]+$")
            .handshakeWithClientInfo(handshakeWithClientInfo)
            .name("writer")
            .build();
    }

    ByteBuffer buildBuffer(long requestMillis, int messageSizeBytes) {
        ByteBuffer data;
        try {
            data = ByteBuffer.wrap(Utils.generateMessage(requestMillis, messageSizeBytes));
            return data;
        } catch (TException e) {
            LOG.error("Error generating message : ", e);
            return null;
        }
    }

    List<ByteBuffer> buildBufferList(int batchSize, long requestMillis, int messageSizeBytes) {
        ArrayList<ByteBuffer> bufferList = new ArrayList<ByteBuffer>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            ByteBuffer buf = buildBuffer(requestMillis, messageSizeBytes);
            if (null == buf) {
                return null;
            }
            bufferList.add(buf);
        }
        return bufferList;
    }

    class TimedRequestHandler implements FutureEventListener<DLSN> {
        final long requestMillis;
        TimedRequestHandler(long requestMillis) {
            this.requestMillis = requestMillis;
        }
        @Override
        public void onSuccess(DLSN value) {
            requestStat.registerSuccessfulEvent(System.currentTimeMillis() - requestMillis);
        }
        @Override
        public void onFailure(Throwable cause) {
            LOG.error("Failed to publish : ", cause);
            requestStat.registerFailedEvent(System.currentTimeMillis() - requestMillis);
            exceptionsLogger.getCounter(cause.getClass().getName()).inc();
            if (cause instanceof DLException) {
                DLException dle = (DLException) cause;
                dlErrorCodeLogger.getCounter(dle.getCode().toString()).inc();
            }
        }
    }

    class Writer implements Runnable {

        final int idx;
        final DistributedLogClient dlc;

        Writer(int idx) {
            this.idx = idx;
            this.dlc = buildDlogClient();
        }

        @Override
        public void run() {
            LOG.info("Started writer {}.", idx);
            while (running) {
                rateLimiter.acquire();
                final String streamName = streamNames.get(random.nextInt(numStreams));
                final long requestMillis = System.currentTimeMillis();
                final ByteBuffer data = buildBuffer(requestMillis, messageSizeBytes);
                if (null == data) {
                    break;
                }
                dlc.write(streamName, data).addEventListener(new TimedRequestHandler(requestMillis));
            }
            dlc.close();
        }
    }

    class BulkWriter implements Runnable {

        final int idx;
        final DistributedLogClient dlc;

        BulkWriter(int idx) {
            this.idx = idx;
            this.dlc = buildDlogClient();
        }

        @Override
        public void run() {
            LOG.info("Started writer {}.", idx);
            while (running) {
                rateLimiter.acquire(batchSize);
                String streamName = streamNames.get(random.nextInt(numStreams));
                final long requestMillis = System.currentTimeMillis();
                final List<ByteBuffer> data = buildBufferList(batchSize, requestMillis, messageSizeBytes);
                if (null == data) {
                    break;
                }
                List<Future<DLSN>> results = dlc.writeBulk(streamName, data);
                for (Future<DLSN> result : results) {
                    result.addEventListener(new TimedRequestHandler(requestMillis));
                }
            }
            dlc.close();
        }
    }

    @Override
    public void run() {
        LOG.info("Starting writer (rate = {}, concurrency = {}, prefix = {}, batchSize = {})",
                 new Object[] { writeRate, writeConcurrency, streamPrefix, batchSize });
        for (int i = 0; i < writeConcurrency; i++) {
            Runnable writer = null;
            if (batchSize > 0) {
                writer = new BulkWriter(i);
            } else {
                writer = new Writer(i);
            }
            executorService.submit(writer);
        }
    }
}
