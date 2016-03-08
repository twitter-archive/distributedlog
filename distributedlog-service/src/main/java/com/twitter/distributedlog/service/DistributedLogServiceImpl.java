package com.twitter.distributedlog.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.RegionUnavailableException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.exceptions.StreamUnavailableException;
import com.twitter.distributedlog.exceptions.TooManyStreamsException;
import com.twitter.distributedlog.feature.AbstractFeatureProvider;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.stream.BulkWriteOp;
import com.twitter.distributedlog.service.stream.CreateOp;
import com.twitter.distributedlog.service.stream.DeleteOp;
import com.twitter.distributedlog.service.stream.HeartbeatOp;
import com.twitter.distributedlog.service.stream.ReleaseOp;
import com.twitter.distributedlog.service.stream.Stream;
import com.twitter.distributedlog.service.stream.StreamFactory;
import com.twitter.distributedlog.service.stream.StreamFactoryImpl;
import com.twitter.distributedlog.service.stream.StreamManager;
import com.twitter.distributedlog.service.stream.StreamManagerImpl;
import com.twitter.distributedlog.service.stream.StreamOp;
import com.twitter.distributedlog.service.stream.StreamOpStats;
import com.twitter.distributedlog.service.stream.TruncateOp;
import com.twitter.distributedlog.service.stream.WriteOpWithPayload;
import com.twitter.distributedlog.service.stream.WriteOp;
import com.twitter.distributedlog.service.stream.limiter.ServiceRequestLimiter;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.ClientInfo;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.HeartbeatOptions;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.ServerStatus;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.rate.MovingAverageRateFactory;
import com.twitter.distributedlog.rate.MovingAverageRate;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Timer;
import com.twitter.util.ScheduledThreadPoolTimer;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DistributedLogServiceImpl implements DistributedLogService.ServiceIface,
                                                  FatalErrorHandler {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServiceImpl.class);

    private final int MOVING_AVERAGE_WINDOW_SECS = 60;

    private final ServerConfiguration serverConfig;
    private final DistributedLogConfiguration dlConfig;
    private final DistributedLogNamespace dlNamespace;
    private final int serverRegionId;
    private ServerStatus serverStatus = ServerStatus.WRITE_AND_ACCEPT;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final CountDownLatch keepAliveLatch;
    private final byte dlsnVersion;
    private final String clientId;
    private final ScheduledExecutorService executorService;
    private final AccessControlManager accessControlManager;
    private final StreamConfigProvider streamConfigProvider;
    private final StreamManager streamManager;
    private final StreamFactory streamFactory;
    private final MovingAverageRateFactory movingAvgFactory;
    private final MovingAverageRate windowedRps;
    private final MovingAverageRate windowedBps;
    private final ServiceRequestLimiter limiter;
    private final Timer timer;
    private final HashedWheelTimer requestTimer;

    // Features
    private final FeatureProvider featureProvider;
    private final Feature featureRegionStopAcceptNewStream;
    private final Feature featureChecksumDisabled;
    private final Feature limiterDisabledFeature;

    // Stats
    private final StatsLogger statsLogger;
    private final StatsLogger perStreamStatsLogger;
    private final StreamOpStats streamOpStats;
    private final Counter bulkWritePendingStat;
    private final Counter writePendingStat;
    private final Counter redirects;
    private final Counter receivedRecordCounter;
    private final StatsLogger statusCodeStatLogger;
    private final ConcurrentHashMap<StatusCode, Counter> statusCodeCounters =
            new ConcurrentHashMap<StatusCode, Counter>();
    private final Counter statusCodeTotal;

    DistributedLogServiceImpl(ServerConfiguration serverConf,
                              DistributedLogConfiguration dlConf,
                              DynamicDistributedLogConfiguration dynDlConf,
                              StreamConfigProvider streamConfigProvider,
                              URI uri,
                              StreamPartitionConverter converter,
                              StatsLogger statsLogger,
                              StatsLogger perStreamStatsLogger,
                              CountDownLatch keepAliveLatch)
            throws IOException {
        // Configuration.
        this.serverConfig = serverConf;
        this.dlConfig = dlConf;
        this.perStreamStatsLogger = perStreamStatsLogger;
        this.dlsnVersion = serverConf.getDlsnVersion();
        this.serverRegionId = serverConf.getRegionId();
        int serverPort = serverConf.getServerPort();
        int shard = serverConf.getServerShardId();
        int numThreads = serverConf.getServerThreads();
        this.clientId = DLSocketAddress.toLockId(DLSocketAddress.getSocketAddress(serverPort), shard);
        String allocatorPoolName = String.format("allocator_%04d_%010d", serverRegionId, shard);
        dlConf.setLedgerAllocatorPoolName(allocatorPoolName);
        this.featureProvider = AbstractFeatureProvider.getFeatureProvider("", dlConf, statsLogger.scope("features"));

        // Build the namespace
        this.dlNamespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(dlConf)
                .uri(uri)
                .statsLogger(statsLogger)
                .featureProvider(this.featureProvider)
                .clientId(clientId)
                .regionId(serverRegionId)
                .build();
        this.accessControlManager = this.dlNamespace.createAccessControlManager();
        this.keepAliveLatch = keepAliveLatch;
        this.streamConfigProvider = streamConfigProvider;

        // Stats pertaining to stream op execution
        this.streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);

        // Executor Service.
        this.executorService = Executors.newScheduledThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Executor-%d").build());

        // Timer, kept separate to ensure reliability of timeouts.
        this.requestTimer = new HashedWheelTimer(
            new ThreadFactoryBuilder().setNameFormat("DLServiceTimer-%d").build(),
            dlConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
            dlConf.getTimeoutTimerNumTicks());

        // Creating and managing Streams
        this.streamFactory = new StreamFactoryImpl(clientId,
                streamOpStats,
                serverConf,
                dlConf,
                featureProvider,
                streamConfigProvider,
                converter,
                dlNamespace,
                executorService,
                this,
                requestTimer);
        this.streamManager = new StreamManagerImpl(
                clientId,
                dlConf,
                executorService,
                streamFactory,
                converter,
                streamConfigProvider,
                dlNamespace);

        // Service features
        this.featureRegionStopAcceptNewStream = this.featureProvider.getFeature(
                ServerFeatureKeys.REGION_STOP_ACCEPT_NEW_STREAM.name().toLowerCase());
        this.featureChecksumDisabled = this.featureProvider.getFeature(
                ServerFeatureKeys.SERVICE_CHECKSUM_DISABLED.name().toLowerCase());
        this.limiterDisabledFeature = this.featureProvider.getFeature(
                ServerFeatureKeys.SERVICE_GLOBAL_LIMITER_DISABLED.name().toLowerCase());

        // Resource limiting
        this.timer = new ScheduledThreadPoolTimer(1, "timer", true);
        this.movingAvgFactory = new MovingAverageRateFactory(timer);
        this.windowedRps = movingAvgFactory.create(MOVING_AVERAGE_WINDOW_SECS);
        this.windowedBps = movingAvgFactory.create(MOVING_AVERAGE_WINDOW_SECS);
        this.limiter = new ServiceRequestLimiter(
                dynDlConf,
                streamOpStats.baseScope("service_limiter"),
                windowedRps,
                windowedBps,
                streamManager,
                limiterDisabledFeature);

        // Stats
        this.statsLogger = statsLogger;

        // Stats on server
        // Gauge for server status/health
        statsLogger.registerGauge("proxy_status", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return ServerStatus.DOWN == serverStatus ? -1 : (featureRegionStopAcceptNewStream.isAvailable() ?
                        3 : (ServerStatus.WRITE_AND_ACCEPT == serverStatus ? 1 : 2));
            }
        });
        // Global moving average rps
        statsLogger.registerGauge("moving_avg_rps", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return windowedRps.get();
            }
        });
        // Global moving average bps
        statsLogger.registerGauge("moving_avg_bps", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return windowedBps.get();
            }
        });

        // Stats on requests
        this.bulkWritePendingStat = streamOpStats.requestPendingCounter("bulkWritePending");
        this.writePendingStat = streamOpStats.requestPendingCounter("writePending");
        this.redirects = streamOpStats.requestCounter("redirect");
        this.statusCodeStatLogger = streamOpStats.requestScope("statuscode");
        this.statusCodeTotal = streamOpStats.requestCounter("statuscode_count");
        this.receivedRecordCounter = streamOpStats.recordsCounter("received");

        // Stats on streams
        StatsLogger streamsStatsLogger = statsLogger.scope("streams");
        streamsStatsLogger.registerGauge("acquired", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return streamManager.numAcquired();
            }
        });
        streamsStatsLogger.registerGauge("cached", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return streamManager.numCached();
            }
        });

        // Setup complete
        logger.info("Running distributedlog server : client id {}, allocator pool {}, perstream stat {}, dlsn version {}.",
                new Object[] { clientId, allocatorPoolName, serverConf.isPerStreamStatEnabled(), dlsnVersion });
    }

    private void countStatusCode(StatusCode code) {
        Counter counter = statusCodeCounters.get(code);
        if (null == counter) {
            counter = statusCodeStatLogger.getCounter(code.name());
            Counter oldCounter = statusCodeCounters.putIfAbsent(code, counter);
            if (null != oldCounter) {
                counter = oldCounter;
            }
        }
        counter.inc();
        statusCodeTotal.inc();
    }

    @Override
    public Future<ServerInfo> handshake() {
        return handshakeWithClientInfo(new ClientInfo());
    }

    @Override
    public Future<ServerInfo> handshakeWithClientInfo(ClientInfo clientInfo) {
        ServerInfo serverInfo = new ServerInfo();
        closeLock.readLock().lock();
        try {
            serverInfo.setServerStatus(serverStatus);
        } finally {
            closeLock.readLock().unlock();
        }

        if (clientInfo.isSetGetOwnerships() && !clientInfo.isGetOwnerships()) {
            return Future.value(serverInfo);
        }

        Optional<String> regex = Optional.absent();
        if (clientInfo.isSetStreamNameRegex()) {
            regex = Optional.of(clientInfo.getStreamNameRegex());
        }

        Map<String, String> ownershipMap = streamManager.getStreamOwnershipMap(regex);
        serverInfo.setOwnerships(ownershipMap);
        return Future.value(serverInfo);
    }

    @VisibleForTesting
    Stream getLogWriter(String stream) throws IOException {
        Stream writer = streamManager.getStream(stream);
        if (null == writer) {
            closeLock.readLock().lock();
            try {
                if (featureRegionStopAcceptNewStream.isAvailable()) {
                    // accept new stream is disabled in current dc
                    throw new RegionUnavailableException("Region is unavailable right now.");
                } else if (!(ServerStatus.WRITE_AND_ACCEPT == serverStatus)) {
                    // if it is closed, we would not acquire stream again.
                    return null;
                }
                writer = streamManager.getOrCreateStream(stream);
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return writer;
    }

    // Service interface methods

    @Override
    public Future<WriteResponse> write(final String stream, ByteBuffer data) {
        receivedRecordCounter.inc();
        return doWrite(stream, data, null /* checksum */);
    }

    @Override
    public Future<BulkWriteResponse> writeBulkWithContext(final String stream, List<ByteBuffer> data, WriteContext ctx) {
        bulkWritePendingStat.inc();
        receivedRecordCounter.add(data.size());
        BulkWriteOp op = new BulkWriteOp(stream, data, statsLogger, perStreamStatsLogger, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result().ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                bulkWritePendingStat.dec();
                return null;
            }
        });
    }

    @Override
    public Future<WriteResponse> writeWithContext(final String stream, ByteBuffer data, WriteContext ctx) {
        return doWrite(stream, data, getChecksum(ctx));
    }

    @Override
    public Future<WriteResponse> heartbeat(String stream, WriteContext ctx) {
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, perStreamStatsLogger, dlsnVersion, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> heartbeatWithOptions(String stream, WriteContext ctx, HeartbeatOptions options) {
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, perStreamStatsLogger, dlsnVersion, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        if (options.isSendHeartBeatToReader()) {
            op.setWriteControlRecord(true);
        }
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> truncate(String stream, String dlsn, WriteContext ctx) {
        TruncateOp op = new TruncateOp(stream, DLSN.deserialize(dlsn), statsLogger, perStreamStatsLogger, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> delete(String stream, WriteContext ctx) {
        DeleteOp op = new DeleteOp(stream, statsLogger, perStreamStatsLogger, streamManager, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> release(String stream, WriteContext ctx) {
        ReleaseOp op = new ReleaseOp(stream, statsLogger, perStreamStatsLogger, streamManager, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> create(String stream, WriteContext ctx) {
        CreateOp op = new CreateOp(stream, statsLogger, streamManager, getChecksum(ctx), featureChecksumDisabled);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<Void> setAcceptNewStream(boolean enabled) {
        closeLock.writeLock().lock();
        try {
            logger.info("Set AcceptNewStream = {}", enabled);
            if (ServerStatus.DOWN != serverStatus) {
                if (enabled) {
                    serverStatus = ServerStatus.WRITE_AND_ACCEPT;
                } else {
                    serverStatus = ServerStatus.WRITE_ONLY;
                }
            }
        } finally {
            closeLock.writeLock().unlock();
        }
        return Future.Void();
    }

    private Future<WriteResponse> doWrite(final String name, ByteBuffer data, Long checksum) {
        writePendingStat.inc();
        receivedRecordCounter.inc();
        WriteOp op = newWriteOp(name, data, checksum);
        executeStreamOp(op);
        return op.result().ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                writePendingStat.dec();
                return null;
            }
        });
    }

    private Long getChecksum(WriteContext ctx) {
        return ctx.isSetCrc32() ? ctx.getCrc32() : null;
    }

    private void executeStreamOp(final StreamOp op) {

        // Must attach this as early as possible--returning before this point will cause us to
        // lose the status code.
        op.responseHeader().addEventListener(new FutureEventListener<ResponseHeader>() {
            @Override
            public void onSuccess(ResponseHeader header) {
                if (header.getLocation() != null || header.getCode() == StatusCode.FOUND) {
                    redirects.inc();
                }
                countStatusCode(header.getCode());
            }
            @Override
            public void onFailure(Throwable cause) {
            }
        });

        try {
            // Apply the request limiter
            limiter.apply(op);

            // Execute per-op pre-exec code
            op.preExecute();

        } catch (TooManyStreamsException e) {
            // Translate to StreamUnavailableException to ensure that the client will redirect
            // to a different host. Ideally we would be able to return TooManyStreamsException,
            // but the way exception handling works right now we can't control the handling in
            // the client because client changes deploy very slowly.
            op.fail(new StreamUnavailableException(e.getMessage()));
            return;
        } catch (Exception e) {
            op.fail(e);
            return;
        }

        Stream stream;
        try {
            stream = getLogWriter(op.streamName());
        } catch (RegionUnavailableException rue) {
            // redirect the requests to other region
            op.fail(new RegionUnavailableException("Region " + serverRegionId + " is unavailable."));
            return;
        } catch (IOException e) {
            op.fail(e);
            return;
        }
        if (null == stream) {
            // redirect the requests when stream is unavailable.
            op.fail(new ServiceUnavailableException("Server " + clientId + " is closed."));
            return;
        }

        if (op instanceof WriteOpWithPayload) {
            WriteOpWithPayload writeOp = (WriteOpWithPayload) op;
            windowedBps.add(writeOp.getPayloadSize());
            windowedRps.inc();
        }

        stream.submit(op);
    }

    void shutdown() {
        try {
            closeLock.writeLock().lock();
            try {
                if (ServerStatus.DOWN == serverStatus) {
                    return;
                }
                serverStatus = ServerStatus.DOWN;
            } finally {
                closeLock.writeLock().unlock();
            }

            streamManager.close();
            movingAvgFactory.close();
            limiter.close();

            Stopwatch closeStreamsStopwatch = Stopwatch.createStarted();

            Future<List<Void>> closeResult = streamManager.closeStreams();
            logger.info("Waiting for closing all streams ...");
            try {
                Await.result(closeResult, Duration.fromTimeUnit(5, TimeUnit.MINUTES));
                logger.info("Closed all streams in {} millis.",
                        closeStreamsStopwatch.elapsed(TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                logger.warn("Interrupted on waiting for closing all streams : ", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("Sorry, we didn't close all streams gracefully in 5 minutes : ", e);
            }

            // shutdown the dl namespace
            logger.info("Closing distributedlog namespace ...");
            dlNamespace.close();
            logger.info("Closed distributedlog namespace .");

            // Stop the timer.
            timer.stop();

            // shutdown the executor after requesting closing streams.
            SchedulerUtils.shutdownScheduler(executorService, 60, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.info("Exception while shutting down distributedlog service.");
        } finally {
            // release the keepAliveLatch in case shutdown is called from a shutdown hook.
            keepAliveLatch.countDown();
            logger.info("Finished shutting down distributedlog service.");
        }
    }

    @Override
    public void notifyFatalError() {
        triggerShutdown();
    }

    private void triggerShutdown() {
        // release the keepAliveLatch to let the main thread shutdown the whole service.
        logger.info("Releasing KeepAlive Latch to trigger shutdown ...");
        keepAliveLatch.countDown();
        logger.info("Released KeepAlive Latch. Main thread will shut the service down.");
    }

    @VisibleForTesting
    java.util.concurrent.Future<?> schedule(Runnable runnable, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (serverStatus != ServerStatus.WRITE_AND_ACCEPT) {
                return null;
            } else if (delayMs > 0) {
                return executorService.schedule(runnable, delayMs, TimeUnit.MILLISECONDS);
            } else {
                return executorService.submit(runnable);
            }
        } catch (RejectedExecutionException ree) {
            logger.error("Failed to schedule task {} in {} ms : ",
                    new Object[] { runnable, delayMs, ree });
            return null;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    // Test methods.

    private DynamicDistributedLogConfiguration getDynConf(String streamName) {
        Optional<DynamicDistributedLogConfiguration> dynDlConf =
                streamConfigProvider.getDynamicStreamConfig(streamName);
        if (dynDlConf.isPresent()) {
            return dynDlConf.get();
        } else {
            return ConfUtils.getConstDynConf(dlConfig);
        }
    }

    @VisibleForTesting
    Stream newStream(String name) {
        return streamFactory.create(name, getDynConf(name), streamManager);
    }

    @VisibleForTesting
    WriteOp newWriteOp(String stream, ByteBuffer data, Long checksum) {
        return new WriteOp(stream, data, statsLogger, perStreamStatsLogger, serverConfig, dlsnVersion,
            checksum, featureChecksumDisabled, accessControlManager);
    }

    @VisibleForTesting
    Future<List<Void>> closeStreams() {
        return streamManager.closeStreams();
    }

    @VisibleForTesting
    public DistributedLogNamespace getDistributedLogNamespace() {
        return dlNamespace;
    }

    @VisibleForTesting
    StreamManager getStreamManager() {
        return streamManager;
    }
}
