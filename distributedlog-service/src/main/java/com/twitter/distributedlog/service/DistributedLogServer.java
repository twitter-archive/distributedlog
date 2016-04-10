package com.twitter.distributedlog.service;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicConfigurationFactory;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.service.announcer.Announcer;
import com.twitter.distributedlog.service.announcer.NOPAnnouncer;
import com.twitter.distributedlog.service.announcer.ServerSetAnnouncer;
import com.twitter.distributedlog.service.config.DefaultStreamConfigProvider;
import com.twitter.distributedlog.service.config.NullStreamConfigProvider;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.config.ServiceStreamConfigProvider;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.streamset.IdentityStreamPartitionConverter;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.finagle.Stack;
import com.twitter.finagle.ThriftMuxServer$;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientIdRequiredFilter;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.finagle.transport.Transport;
import com.twitter.util.Duration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DistributedLogServer {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServer.class);

    private DistributedLogServiceImpl dlService = null;
    private Server server = null;
    private StatsProvider statsProvider;
    private Announcer announcer = null;
    private ScheduledExecutorService configExecutorService;
    private long gracefulShutdownMs = 0L;

    private final StatsReceiver statsReceiver;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Optional<String> uri;
    private final Optional<String> conf;
    private final Optional<String> streamConf;
    private final Optional<Integer> port;
    private final Optional<Integer> statsPort;
    private final Optional<Integer> shardId;
    private final Optional<String> announcePath;
    private final Optional<Boolean> thriftmux;

    DistributedLogServer(Optional<String> uri,
                         Optional<String> conf,
                         Optional<String> streamConf,
                         Optional<Integer> port,
                         Optional<Integer> statsPort,
                         Optional<Integer> shardId,
                         Optional<String> announcePath,
                         Optional<Boolean> thriftmux,
                         StatsReceiver statsReceiver,
                         StatsProvider statsProvider) {
        this.uri = uri;
        this.conf = conf;
        this.streamConf = streamConf;
        this.port = port;
        this.statsPort = statsPort;
        this.shardId = shardId;
        this.announcePath = announcePath;
        this.thriftmux = thriftmux;
        this.statsReceiver = statsReceiver;
        this.statsProvider = statsProvider;
    }

    public void runServer() throws ConfigurationException, IllegalArgumentException, IOException {
        if (!uri.isPresent()) {
            throw new IllegalArgumentException("No distributedlog uri provided.");
        }
        URI dlUri = URI.create(uri.get());
        DistributedLogConfiguration dlConf = new DistributedLogConfiguration();
        if (conf.isPresent()) {
            String configFile = conf.get();
            try {
                dlConf.loadConf(new File(configFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new IllegalArgumentException("Failed to load distributedlog configuration from " + configFile + ".");
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Failed to load distributedlog configuration from malformed "
                        + configFile + ".");
            }
        }

        this.configExecutorService = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("DistributedLogService-Dyncfg-%d")
                        .setDaemon(true)
                        .build());

        // server configuration and dynamic configuration
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(dlConf);
        serverConf.validate();

        DynamicDistributedLogConfiguration dynDlConf = getServiceDynConf(dlConf);

        logger.info("Starting stats provider : {}", statsProvider.getClass());
        statsProvider.start(dlConf);

        if (announcePath.isPresent()) {
            announcer = new ServerSetAnnouncer(
                    dlUri,
                    port.or(0),
                    statsPort.or(0),
                    shardId.or(0));
        } else {
            announcer = new NOPAnnouncer();
        }

        // Build the stream partition converter
        StreamPartitionConverter converter;
        try {
            converter = ReflectionUtils.newInstance(serverConf.getStreamPartitionConverterClass());
        } catch (ConfigurationException e) {
            logger.warn("Failed to load configured stream-to-partition converter. Fallback to use {}",
                    IdentityStreamPartitionConverter.class.getName());
            converter = new IdentityStreamPartitionConverter();
        }

        StreamConfigProvider streamConfProvider =
                getStreamConfigProvider(dlConf, converter);

        // pre-run
        preRun(dlConf, serverConf);

        Pair<DistributedLogServiceImpl, Server> serverPair = runServer(
                serverConf,
                dlConf,
                dynDlConf,
                dlUri,
                converter,
                statsProvider,
                port.or(0),
                keepAliveLatch,
                statsReceiver,
                thriftmux.isPresent(),
                streamConfProvider);

        this.dlService = serverPair.getLeft();
        this.server = serverPair.getRight();

        // announce the service
        announcer.announce();
    }

    protected void preRun(DistributedLogConfiguration conf, ServerConfiguration serverConf) {
        this.gracefulShutdownMs = serverConf.getGracefulShutdownPeriodMs();
        if (!serverConf.isDurableWriteEnabled()) {
            conf.setDurableWriteEnabled(false);
        }
    }

    private DynamicDistributedLogConfiguration getServiceDynConf(DistributedLogConfiguration dlConf) throws ConfigurationException {
        Optional<DynamicDistributedLogConfiguration> dynConf = Optional.absent();
        if (conf.isPresent()) {
            DynamicConfigurationFactory configFactory = new DynamicConfigurationFactory(
                    configExecutorService, dlConf.getDynamicConfigReloadIntervalSec(), TimeUnit.SECONDS);
            dynConf = configFactory.getDynamicConfiguration(conf.get());
        }
        if (dynConf.isPresent()) {
            return dynConf.get();
        } else {
            return ConfUtils.getConstDynConf(dlConf);
        }
    }

    private StreamConfigProvider getStreamConfigProvider(DistributedLogConfiguration dlConf,
                                                         StreamPartitionConverter partitionConverter)
            throws ConfigurationException {
        StreamConfigProvider streamConfProvider = new NullStreamConfigProvider();
        if (streamConf.isPresent() && conf.isPresent()) {
            String dynConfigPath = streamConf.get();
            String defaultConfigFile = conf.get();
            streamConfProvider = new ServiceStreamConfigProvider(
                    dynConfigPath,
                    defaultConfigFile,
                    partitionConverter,
                    configExecutorService,
                    dlConf.getDynamicConfigReloadIntervalSec(),
                    TimeUnit.SECONDS);
        } else if (conf.isPresent()) {
            String configFile = conf.get();
            streamConfProvider = new DefaultStreamConfigProvider(configFile, configExecutorService,
                    dlConf.getDynamicConfigReloadIntervalSec(), TimeUnit.SECONDS);
        }
        return streamConfProvider;
    }

    static Pair<DistributedLogServiceImpl, Server> runServer(
            ServerConfiguration serverConf,
            DistributedLogConfiguration dlConf,
            URI dlUri,
            StreamPartitionConverter converter,
            StatsProvider provider,
            int port) throws IOException {

        return runServer(serverConf,
                dlConf,
                ConfUtils.getConstDynConf(dlConf),
                dlUri,
                converter,
                provider,
                port,
                new CountDownLatch(0),
                new NullStatsReceiver(),
                false,
                new NullStreamConfigProvider());
    }

    static Pair<DistributedLogServiceImpl, Server> runServer(
            ServerConfiguration serverConf,
            DistributedLogConfiguration dlConf,
            DynamicDistributedLogConfiguration dynDlConf,
            URI dlUri,
            StreamPartitionConverter partitionConverter,
            StatsProvider provider,
            int port,
            CountDownLatch keepAliveLatch,
            StatsReceiver statsReceiver,
            boolean thriftmux,
            StreamConfigProvider streamConfProvider) throws IOException {
        logger.info("Running server @ uri {}.", dlUri);

        boolean perStreamStatsEnabled = serverConf.isPerStreamStatEnabled();
        StatsLogger perStreamStatsLogger;
        if (perStreamStatsEnabled) {
            perStreamStatsLogger = provider.getStatsLogger("stream");
        } else {
            perStreamStatsLogger = NullStatsLogger.INSTANCE;
        }

        // dl service
        DistributedLogServiceImpl dlService = new DistributedLogServiceImpl(
                serverConf,
                dlConf,
                dynDlConf,
                streamConfProvider,
                dlUri,
                partitionConverter,
                provider.getStatsLogger(""),
                perStreamStatsLogger,
                keepAliveLatch);

        StatsReceiver serviceStatsReceiver = statsReceiver.scope("service");
        StatsLogger serviceStatsLogger = provider.getStatsLogger("service");

        ServerBuilder serverBuilder = ServerBuilder.get()
                .name("DistributedLogServer")
                .codec(ThriftServerFramedCodec.get())
                .reportTo(statsReceiver)
                .keepAlive(true)
                .bindTo(new InetSocketAddress(port));

        if (thriftmux) {
            logger.info("Using thriftmux.");
            Tuple2<Transport.Liveness, Stack.Param<Transport.Liveness>> livenessParam = new Transport.Liveness(
                    Duration.Top(), Duration.Top(), Option.apply((Object) Boolean.valueOf(true))).mk();
            serverBuilder = serverBuilder.stack(ThriftMuxServer$.MODULE$.configured(livenessParam._1(), livenessParam._2()));
        }

        logger.info("DistributedLogServer running with the following configuration : \n{}", dlConf.getPropsAsString());

        // starts dl server
        Server server = ServerBuilder.safeBuild(
                new ClientIdRequiredFilter<byte[], byte[]>(serviceStatsReceiver).andThen(
                    new StatsFilter<byte[], byte[]>(serviceStatsLogger).andThen(
                        new DistributedLogService.Service(dlService, new TBinaryProtocol.Factory()))),
                serverBuilder);

        logger.info("Started DistributedLog Server.");
        return Pair.of(dlService, server);
    }

    static void closeServer(Pair<DistributedLogServiceImpl, Server> pair,
                            long gracefulShutdownPeriod,
                            TimeUnit timeUnit) {
        if (null != pair.getLeft()) {
            pair.getLeft().shutdown();
            if (gracefulShutdownPeriod > 0) {
                try {
                    timeUnit.sleep(gracefulShutdownPeriod);
                } catch (InterruptedException e) {
                    logger.info("Interrupted on waiting service shutting down state propagated to all clients : ", e);
                }
            }
        }
        if (null != pair.getRight()) {
            logger.info("Closing dl thrift server.");
            pair.getRight().close();
            logger.info("Closed dl thrift server.");
        }
    }

    /**
     * Close the server.
     */
    public void close() {
        if (null != announcer) {
            try {
                announcer.unannounce();
            } catch (IOException e) {
                logger.warn("Error on unannouncing service : ", e);
            }
            announcer.close();
        }
        closeServer(Pair.of(dlService, server), gracefulShutdownMs, TimeUnit.MILLISECONDS);
        if (null != statsProvider) {
            statsProvider.stop();
        }
        SchedulerUtils.shutdownScheduler(configExecutorService, 60, TimeUnit.SECONDS);
        keepAliveLatch.countDown();
    }

    public void join() throws InterruptedException {
        keepAliveLatch.await();
    }

    /**
     * Running distributedlog server.
     *
     * @param uri distributedlog namespace
     * @param conf distributedlog configuration file location
     * @param streamConf per stream configuration dir location
     * @param port listen port
     * @param statsPort stats port
     * @param shardId shard id
     * @param announcePath server set announce path
     * @param thriftmux flag to enable thrift mux
     * @param statsReceiver receiver to receive finagle stats
     * @param statsProvider provider to receive dl stats
     * @return distributedlog server
     * @throws ConfigurationException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static DistributedLogServer runServer(
               Optional<String> uri,
               Optional<String> conf,
               Optional<String> streamConf,
               Optional<Integer> port,
               Optional<Integer> statsPort,
               Optional<Integer> shardId,
               Optional<String> announcePath,
               Optional<Boolean> thriftmux,
               StatsReceiver statsReceiver,
               StatsProvider statsProvider)
            throws ConfigurationException, IllegalArgumentException, IOException {

        final DistributedLogServer server = new DistributedLogServer(
                uri,
                conf,
                streamConf,
                port,
                statsPort,
                shardId,
                announcePath,
                thriftmux,
                statsReceiver,
                statsProvider);

        server.runServer();
        return server;
    }
}
