package com.twitter.distributedlog.v2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.MetadataAccessor;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.feature.CoreFeatureKeys;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import com.twitter.distributedlog.util.Utils;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DistributedLogManagerFactory {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogManagerFactory.class);

    static interface ZooKeeperClientHandler<T> {
        T handle(ZooKeeperClient zkc) throws IOException;
    }

    /**
     * Run given <i>handler</i> by providing an available new zookeeper client
     *
     * @param handler
     *          Handler to process with provided zookeeper client.
     * @param conf
     *          Distributedlog Configuration.
     * @param namespace
     *          Distributedlog Namespace.
     */
    private static <T> T withZooKeeperClient(ZooKeeperClientHandler<T> handler,
                                             DistributedLogConfiguration conf,
                                             URI namespace) throws IOException {
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .name(String.format("dlzk:%s:factory_static", namespace))
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .uri(namespace)
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .requestRateLimit(conf.getZKRequestRateLimit())
                .zkAclId(conf.getZkAclId())
                .build();
        try {
            return handler.handle(zkc);
        } finally {
            zkc.close();
        }
    }

    public static enum ClientSharingOption {
        PerStreamClients,
        SharedZKClientPerStreamBKClient,
        SharedClients
    }

    private final String clientId;
    private DistributedLogConfiguration conf;
    private URI namespace;
    private final StatsLogger statsLogger;
    private final ScheduledExecutorService scheduledExecutorService;
    private final OrderedSafeExecutor lockStateExecutor;
    private final ClientSocketChannelFactory channelFactory;
    private final HashedWheelTimer requestTimer;
    // zookeeper clients
    // NOTE: The actual zookeeper client is initialized lazily when it is referenced by
    //       {@link com.twitter.distributedlog.v2.ZooKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    private final ZooKeeperClientBuilder sharedWriterZKCBuilderForDL;
    private final ZooKeeperClient sharedWriterZKCForDL;
    private final ZooKeeperClientBuilder sharedReaderZKCBuilderForDL;
    private final ZooKeeperClient sharedReaderZKCForDL;
    private ZooKeeperClientBuilder sharedWriterZKCBuilderForBK = null;
    private ZooKeeperClient sharedWriterZKCForBK = null;
    private ZooKeeperClientBuilder sharedReaderZKCBuilderForBK = null;
    private ZooKeeperClient sharedReaderZKCForBK = null;
    // NOTE: The actual bookkeeper client is initialized lazily when it is referenced by
    //       {@link com.twitter.distributedlog.v2.BookKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    private final BookKeeperClientBuilder sharedWriterBKCBuilder;
    private final BookKeeperClient writerBKC;
    private final BookKeeperClientBuilder sharedReaderBKCBuilder;
    private final BookKeeperClient readerBKC;

    protected boolean closed = false;

    private final PermitLimiter writeLimiter;

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri) throws IOException, IllegalArgumentException {
        this(conf, uri, NullStatsLogger.INSTANCE);
    }

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri,
                                        StatsLogger statsLogger) throws IOException, IllegalArgumentException {
        this(conf, uri, statsLogger, DistributedLogConstants.UNKNOWN_CLIENT_ID);
    }

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri,
                                        StatsLogger statsLogger, String clientId) throws IOException, IllegalArgumentException {
        validateConfAndURI(conf, uri);
        this.conf = conf;
        this.namespace = uri;
        this.statsLogger = statsLogger;
        this.clientId = clientId;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(
                conf.getNumWorkerThreads(),
                new ThreadFactoryBuilder().setNameFormat("DLM-" + uri.getPath() + "-executor-%d").build()
        );
        this.channelFactory = new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("DL-netty-boss-%d").build()),
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("DL-netty-worker-%d").build()),
            conf.getBKClientNumberIOThreads());
        this.lockStateExecutor = OrderedSafeExecutor.newBuilder()
                .name("DLM-LockState")
                .numThreads(conf.getNumLockStateThreads())
                .statsLogger(statsLogger)
                .build();
        this.requestTimer = new HashedWheelTimer(
            new ThreadFactoryBuilder().setNameFormat("DLFactoryTimer-%d").build(),
            conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
            conf.getTimeoutTimerNumTicks());

        // Build zookeeper client for writers
        this.sharedWriterZKCBuilderForDL = createDLZKClientBuilder(
                String.format("dlzk:%s:factory_writer_shared", namespace),
                conf,
                DLUtils.getZKServersFromDLUri(uri),
                statsLogger.scope("dlzk_factory_writer_shared"));
        this.sharedWriterZKCForDL = this.sharedWriterZKCBuilderForDL.build();

        // Resolve uri to get bk dl config
        BKDLConfig bkdlConfig = resolveBKDLConfig();

        // Build zookeeper client for readers
        if (bkdlConfig.getDlZkServersForWriter().equals(bkdlConfig.getDlZkServersForReader())) {
            this.sharedReaderZKCBuilderForDL = this.sharedWriterZKCBuilderForDL;
        } else {
            this.sharedReaderZKCBuilderForDL = createDLZKClientBuilder(
                    String.format("dlzk:%s:factory_reader_shared", namespace),
                    conf,
                    bkdlConfig.getDlZkServersForReader(),
                    statsLogger.scope("dlzk_factory_reader_shared"));
        }
        this.sharedReaderZKCForDL = this.sharedReaderZKCBuilderForDL.build();

        // Build bookkeeper client for writers
        this.sharedWriterBKCBuilder = createBKCBuilder(
                String.format("bk:%s:factory_writer_shared", namespace),
                conf,
                bkdlConfig.getBkZkServersForWriter(),
                bkdlConfig.getBkLedgersPath());
        this.writerBKC = this.sharedWriterBKCBuilder.build();

        // Build bookkeeper client for readers
        if (bkdlConfig.getBkZkServersForWriter().equals(bkdlConfig.getBkZkServersForReader())) {
            this.sharedReaderBKCBuilder = this.sharedWriterBKCBuilder;
        } else {
            this.sharedReaderBKCBuilder = createBKCBuilder(
                    String.format("bk:%s:factory_reader_shared", namespace),
                    conf,
                    bkdlConfig.getBkZkServersForReader(),
                    bkdlConfig.getBkLedgersPath());
        }
        this.readerBKC = this.sharedReaderBKCBuilder.build();

        if (conf.getGlobalOutstandingWriteLimit() < 0) {
            this.writeLimiter = PermitLimiter.NULL_PERMIT_LIMITER;
        } else {
            Feature disableWriteLimitFeature = new SettableFeature(
                    CoreFeatureKeys.DISABLE_WRITE_LIMIT.name().toLowerCase(), 0);
            this.writeLimiter = new SimplePermitLimiter(
                conf.getOutstandingWriteLimitDarkmode(),
                conf.getGlobalOutstandingWriteLimit(),
                statsLogger.scope("writeLimiter"),
                true /* singleton */,
                disableWriteLimitFeature);
        }
    }

    private static ZooKeeperClientBuilder createDLZKClientBuilder(String zkcName,
                                                                DistributedLogConfiguration conf,
                                                                String zkServers,
                                                                StatsLogger statsLogger) {
        RetryPolicy retryPolicy = null;
        if (conf.getZKNumRetries() > 0) {
            retryPolicy = new BoundExponentialBackoffRetryPolicy(
                conf.getZKRetryBackoffStartMillis(),
                conf.getZKRetryBackoffMaxMillis(), conf.getZKNumRetries());
        }
        ZooKeeperClientBuilder builder = ZooKeeperClientBuilder.newBuilder()
            .name(zkcName)
            .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
            .retryThreadCount(conf.getZKClientNumberRetryThreads())
            .requestRateLimit(conf.getZKRequestRateLimit())
            .zkServers(zkServers)
            .retryPolicy(retryPolicy)
            .statsLogger(statsLogger)
            .zkAclId(conf.getZkAclId());
        LOG.info("Created shared zooKeeper client builder {}: zkServers = {}, numRetries = {}, sessionTimeout = {}, retryBackoff = {},"
                 + " maxRetryBackoff = {}, zkAclId = {}.", new Object[] { zkcName, zkServers, conf.getZKNumRetries(),
                conf.getZKSessionTimeoutMilliseconds(), conf.getZKRetryBackoffStartMillis(),
                conf.getZKRetryBackoffMaxMillis(), conf.getZkAclId() });
        return builder;
    }

    private static ZooKeeperClientBuilder createBKZKClientBuilder(String zkcName,
                                                                  DistributedLogConfiguration conf,
                                                                  String zkServers,
                                                                  StatsLogger statsLogger) {
        RetryPolicy retryPolicy = null;
        if (conf.getZKNumRetries() > 0) {
            retryPolicy = new BoundExponentialBackoffRetryPolicy(
                    conf.getBKClientZKRetryBackoffStartMillis(),
                    conf.getBKClientZKRetryBackoffMaxMillis(),
                    conf.getBKClientZKNumRetries());
        }
        ZooKeeperClientBuilder builder = ZooKeeperClientBuilder.newBuilder()
                .name(zkcName)
                .sessionTimeoutMs(conf.getBKClientZKSessionTimeoutMilliSeconds())
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .requestRateLimit(conf.getBKClientZKRequestRateLimit())
                .zkServers(zkServers)
                .retryPolicy(retryPolicy)
                .statsLogger(statsLogger)
                .zkAclId(conf.getZkAclId());
        LOG.info("Created shared zooKeeper client builder {}: zkServers = {}, numRetries = {}, sessionTimeout = {}, retryBackoff = {},"
                + " maxRetryBackoff = {}, zkAclId = {}.", new Object[] { zkcName, zkServers, conf.getBKClientZKNumRetries(),
                conf.getBKClientZKSessionTimeoutMilliSeconds(), conf.getBKClientZKRetryBackoffStartMillis(),
                conf.getBKClientZKRetryBackoffMaxMillis(), conf.getZkAclId() });
        return builder;
    }

    private BookKeeperClientBuilder createBKCBuilder(String bkcName,
                                                     DistributedLogConfiguration conf,
                                                     String zkServers,
                                                     String ledgersPath) {
        BookKeeperClientBuilder builder = BookKeeperClientBuilder.newBuilder()
                .name(bkcName)
                .dlConfig(conf)
                .zkServers(zkServers)
                .ledgersPath(ledgersPath)
                .channelFactory(channelFactory)
                .requestTimer(requestTimer)
                .statsLogger(statsLogger);
        LOG.info("Created shared client builder {} : zkServers = {}, ledgersPath = {}, numIOThreads = {}, numWorkerThreads = {}",
                 new Object[] { bkcName, zkServers, ledgersPath, conf.getBKClientNumberIOThreads(), conf.getBKClientNumberWorkerThreads() });
        return builder;
    }

    @VisibleForTesting
    public ZooKeeperClient getSharedWriterZKCForDL() {
        return sharedWriterZKCForDL;
    }

    @VisibleForTesting
    public BookKeeperClientBuilder getSharedReaderBKCBuilder() {
        return sharedReaderBKCBuilder;
    }

    public BookKeeperClient getReaderBKC() {
        return readerBKC;
    }

    /**
     * Run given <i>handler</i> by providing an available zookeeper client.
     *
     * @param handler
     *          Handler to process with provided zookeeper client.
     * @return result processed by handler.
     * @throws IOException
     */
    private <T> T withZooKeeperClient(ZooKeeperClientHandler<T> handler) throws IOException {
        return handler.handle(sharedWriterZKCForDL);
    }

    private BKDLConfig resolveBKDLConfig() throws IOException {
        return withZooKeeperClient(new ZooKeeperClientHandler<BKDLConfig>() {
            @Override
            public BKDLConfig handle(ZooKeeperClient zkc) throws IOException {
                return BKDLConfig.resolveDLConfig(zkc, namespace);
            }
        });
    }

    /**
     * Create a DistributedLogManager as <i>nameOfLogStream</i>.
     *
     * <p>
     * For zookeeper session expire handling purpose, we don't use zookeeper client & bookkeeper client builder inside factory.
     * We managed the shared executor service for all the {@link DistributedLogManager}s created by this factory, so we don't
     * spawn too much threads.
     * </p>
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @return distributedlog manager instance.
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Deprecated
    public DistributedLogManager createDistributedLogManager(String nameOfLogStream) throws IOException, IllegalArgumentException {
        return createDistributedLogManager(nameOfLogStream, ClientSharingOption.PerStreamClients);
    }

    /**
     * Create a DistributedLogManager as <i>nameOfLogStream</i>.
     *
     * <p>
     * For zookeeper session expire handling purpose, we don't use shared bookkeeper client builder inside factory. We however use
     * shared zookeeper client as the ZooKeeper client can seamlessly reconnect on a session expiration
     * We managed the shared executor service for all the {@link DistributedLogManager}s created by this factory, so we don't
     * spawn too much threads.
     * </p>
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @return distributedlog manager instance.
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Deprecated
    public DistributedLogManager createDistributedLogManagerWithSharedZK(String nameOfLogStream) throws IOException, IllegalArgumentException {
        return createDistributedLogManager(nameOfLogStream, ClientSharingOption.SharedZKClientPerStreamBKClient);
    }

    /**
     * Create a DistributedLogManager as <i>nameOfLogStream</i>, which shared the zookeeper & bookkeeper builder
     * used by the factory.
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @return distributedlog manager instance.
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Deprecated
    public DistributedLogManager createDistributedLogManagerWithSharedClients(String nameOfLogStream)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(nameOfLogStream, ClientSharingOption.SharedClients);
    }

    /**
     * Create a DistributedLogManager for <i>nameOfLogStream</i>, with specified client sharing options.
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @param clientSharingOption
     *          specifies if the ZK/BK clients are shared
     * @return distributedlog manager instance.
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public DistributedLogManager createDistributedLogManager(
        String nameOfLogStream,
        ClientSharingOption clientSharingOption)
        throws IOException, IllegalArgumentException {
        Optional<DistributedLogConfiguration> streamConfiguration = Optional.absent();
        return createDistributedLogManager(nameOfLogStream, clientSharingOption, streamConfiguration);
    }

    /**
     * Create a DistributedLogManager for <i>nameOfLogStream</i>, with specified client sharing options.
     * Override whitelisted stream-level configuration settings with settings found in
     * <i>streamConfiguration</i>.
     *
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @param clientSharingOption
     *          specifies if the ZK/BK clients are shared
     * @param streamConfiguration
     *          stream configuration overrides.
     * @return distributedlog manager instance.
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public DistributedLogManager createDistributedLogManager(
        String nameOfLogStream,
        ClientSharingOption clientSharingOption,
        Optional<DistributedLogConfiguration> streamConfiguration)
        throws IOException, IllegalArgumentException {
        // Make sure the name is well formed
        DistributedLogManagerFactory.validateName(nameOfLogStream);

        DistributedLogConfiguration mergedConfiguration = (DistributedLogConfiguration)conf.clone();
        Optional<com.twitter.distributedlog.DistributedLogConfiguration> streamConfV3;
        if (streamConfiguration.isPresent()) {
            com.twitter.distributedlog.DistributedLogConfiguration v3Conf = streamConfiguration.get();
            streamConfV3 = Optional.of(v3Conf);
        } else {
            streamConfV3 = Optional.absent();
        }
        mergedConfiguration.loadStreamConf(streamConfV3);

        ZooKeeperClientBuilder writerZKCBuilderForDL = null;
        ZooKeeperClientBuilder readerZKCBuilderForDL = null;
        ZooKeeperClient writerZKCForBK = null;
        ZooKeeperClient readerZKCForBK = null;
        BookKeeperClientBuilder writerBKCBuilder = null;
        BookKeeperClientBuilder readerBKCBuilder = null;

        switch(clientSharingOption) {
            case SharedClients:
                writerZKCBuilderForDL = sharedWriterZKCBuilderForDL;
                readerZKCBuilderForDL = sharedReaderZKCBuilderForDL;
                writerBKCBuilder = sharedWriterBKCBuilder;
                readerBKCBuilder = sharedReaderBKCBuilder;
                break;
            case SharedZKClientPerStreamBKClient:
                writerZKCBuilderForDL = sharedWriterZKCBuilderForDL;
                readerZKCBuilderForDL = sharedReaderZKCBuilderForDL;
                BKDLConfig bkdlConfig = resolveBKDLConfig();
                synchronized (this) {
                    if (null == this.sharedWriterZKCForBK) {
                        this.sharedWriterZKCBuilderForBK = createBKZKClientBuilder(
                            String.format("bkzk:%s:factory_writer_shared", namespace),
                            mergedConfiguration,
                            bkdlConfig.getBkZkServersForWriter(),
                            statsLogger.scope("bkzk_factory_writer_shared"));
                        this.sharedWriterZKCForBK = this.sharedWriterZKCBuilderForBK.build();
                    }
                    if (null == this.sharedReaderZKCForBK) {
                        if (bkdlConfig.getBkZkServersForWriter().equals(bkdlConfig.getBkZkServersForReader())) {
                            this.sharedReaderZKCBuilderForBK = this.sharedWriterZKCBuilderForBK;
                        } else {
                            this.sharedReaderZKCBuilderForBK = createBKZKClientBuilder(
                                String.format("bkzk:%s:factory_reader_shared", namespace),
                                mergedConfiguration,
                                bkdlConfig.getBkZkServersForReader(),
                                statsLogger.scope("bkzk_factory_reader_shared"));
                        }
                        this.sharedReaderZKCForBK = this.sharedReaderZKCBuilderForBK.build();
                    }
                    writerZKCForBK = this.sharedWriterZKCForBK;
                    readerZKCForBK = this.sharedReaderZKCForBK;
                }
                break;
        }

        BKDistributedLogManager distLogMgr = new BKDistributedLogManager(
            nameOfLogStream, mergedConfiguration, namespace,
            writerZKCBuilderForDL, readerZKCBuilderForDL,
            writerZKCForBK, readerZKCForBK,
            writerBKCBuilder, readerBKCBuilder, scheduledExecutorService, lockStateExecutor,
            channelFactory, requestTimer, writeLimiter, statsLogger);

        distLogMgr.setClientId(clientId);
        return distLogMgr;
    }


    public MetadataAccessor createMetadataAccessor(String nameOfMetadataNode) throws IOException, IllegalArgumentException {
        DistributedLogManagerFactory.validateName(nameOfMetadataNode);
        return new ZKMetadataAccessor(nameOfMetadataNode, conf, namespace,
                sharedWriterZKCBuilderForDL, sharedReaderZKCBuilderForDL, statsLogger);
    }

    public boolean checkIfLogExists(String nameOfLogStream)
        throws IOException, IllegalArgumentException {
        return DistributedLogManagerFactory.checkIfLogExists(conf, namespace, nameOfLogStream);
    }

    public Collection<String> enumerateAllLogsInNamespace()
        throws IOException, IllegalArgumentException {
        return withZooKeeperClient(new ZooKeeperClientHandler<Collection<String>>() {
            @Override
            public Collection<String> handle(ZooKeeperClient zkc) throws IOException {
                return DistributedLogManagerFactory.enumerateAllLogsInternal(zkc, conf, namespace);
            }
        });
    }

    public Map<String, byte[]> enumerateLogsWithMetadataInNamespace()
        throws IOException, IllegalArgumentException {
        return withZooKeeperClient(new ZooKeeperClientHandler<Map<String, byte[]>>() {
            @Override
            public Map<String, byte[]> handle(ZooKeeperClient zkc) throws IOException {
                return DistributedLogManagerFactory.enumerateLogsWithMetadataInternal(zkc, conf, namespace);
            }
        });
    }

    /**
     * This method is to initialize the metadata for a unpartitioned stream with name <i>streamName</i>.
     *
     * @param streamName
     *          stream name.
     * @throws IOException
     */
    public void createUnpartitionedStream(final String streamName) throws IOException {
        withZooKeeperClient(new ZooKeeperClientHandler<Void>() {
            @Override
            public Void handle(ZooKeeperClient zkc) throws IOException {
                BKDistributedLogManager.createUnpartitionedStream(conf, zkc, namespace, streamName);
                return null;
            }
        });
    }

    @Deprecated
    public static DistributedLogManager createDistributedLogManager(String name, URI uri) throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, new DistributedLogConfiguration(), uri);
    }

    private static void validateInput(DistributedLogConfiguration conf, URI uri, String nameOfStream)
        throws IllegalArgumentException {
        validateConfAndURI(conf, uri);
        validateName(nameOfStream);
    }

    private static void validateConfAndURI(DistributedLogConfiguration conf, URI uri)
        throws IllegalArgumentException {
        // input validation
        //
        if (null == conf) {
            throw new IllegalArgumentException("Incorrect Configuration");
        }

        if ((null == uri) || (null == uri.getAuthority()) || (null == uri.getPath())) {
            throw new IllegalArgumentException("Incorrect ZK URI");
        }
    }

    private static void validateName(String nameOfStream) {
        if (nameOfStream.contains("/")) {
            throw new IllegalArgumentException("Stream Name contains invalid characters");
        }
    }

    @Deprecated
    public static DistributedLogManager createDistributedLogManager(
            String name,
            DistributedLogConfiguration conf,
            URI uri)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, (ZooKeeperClientBuilder) null, (BookKeeperClientBuilder) null,
                NullStatsLogger.INSTANCE);
    }

    @Deprecated
    public static DistributedLogManager createDistributedLogManager(
            String name,
            DistributedLogConfiguration conf,
            URI uri,
            ZooKeeperClient zkc,
            BookKeeperClient bkc)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, ZooKeeperClientBuilder.newBuilder().zkc(zkc),
                BookKeeperClientBuilder.newBuilder().bkc(bkc), NullStatsLogger.INSTANCE);
    }

    @Deprecated
    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                                                    ZooKeeperClient zkc, BookKeeperClient bkc, StatsLogger statsLogger)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, ZooKeeperClientBuilder.newBuilder().zkc(zkc),
                BookKeeperClientBuilder.newBuilder().bkc(bkc).statsLogger(statsLogger), statsLogger);
    }

    @Deprecated
    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                ZooKeeperClientBuilder zkcBuilder, BookKeeperClientBuilder bkcBuilder, StatsLogger statsLogger)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri, name);
        return new BKDistributedLogManager(name, conf, uri, zkcBuilder, zkcBuilder, null, null, bkcBuilder, bkcBuilder,
                PermitLimiter.NULL_PERMIT_LIMITER, statsLogger);
    }

    @Deprecated
    public static MetadataAccessor createMetadataAccessor(
            String name,
            URI uri,
            DistributedLogConfiguration conf)
        throws IOException, IllegalArgumentException {
        return createMetadataAccessor(name, uri, conf, (ZooKeeperClientBuilder) null);
    }

    @Deprecated
    public static MetadataAccessor createMetadataAccessor(
            String name,
            URI uri,
            DistributedLogConfiguration conf,
            ZooKeeperClient zkc)
        throws IOException, IllegalArgumentException {
        return createMetadataAccessor(name, uri, conf, ZooKeeperClientBuilder.newBuilder().zkc(zkc));
    }

    @Deprecated
    public static MetadataAccessor createMetadataAccessor(
            String name,
            URI uri,
            DistributedLogConfiguration conf,
            ZooKeeperClientBuilder zkcBuilder)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri, name);
        return new ZKMetadataAccessor(name, conf, uri, zkcBuilder, zkcBuilder, NullStatsLogger.INSTANCE);
    }

    public static boolean checkIfLogExists(DistributedLogConfiguration conf, URI uri, String name)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri, name);
        final String logRootPath = uri.getPath() + String.format("/%s", name);
        return withZooKeeperClient(new ZooKeeperClientHandler<Boolean>() {
            @Override
            public Boolean handle(ZooKeeperClient zkc) throws IOException {
                // check existence after syncing
                try {
                    return null != Utils.sync(zkc, logRootPath).exists(logRootPath, false);
                } catch (KeeperException e) {
                    throw new ZKException("Error on checking if log " + logRootPath + " exists", e.code());
                } catch (InterruptedException e) {
                    throw new DLInterruptedException("Interrupted on checking if log " + logRootPath + " exists", e);
                }
            }
        }, conf, uri);
    }

    public static Collection<String> enumerateAllLogsInNamespace(final DistributedLogConfiguration conf, final URI uri)
        throws IOException, IllegalArgumentException {
        return withZooKeeperClient(new ZooKeeperClientHandler<Collection<String>>() {
            @Override
            public Collection<String> handle(ZooKeeperClient zkc) throws IOException {
                return enumerateAllLogsInternal(zkc, conf, uri);
            }
        }, conf, uri);
    }

    private static Collection<String> enumerateAllLogsInternal(ZooKeeperClient zkc, DistributedLogConfiguration conf, URI uri)
        throws IOException, IllegalArgumentException {
        validateConfAndURI(conf, uri);
        String namespaceRootPath = uri.getPath();
        try {
            ZooKeeper zk = Utils.sync(zkc, namespaceRootPath);
            Stat currentStat = zk.exists(namespaceRootPath, false);
            if (currentStat == null) {
                return new LinkedList<String>();
            }
            return zk.getChildren(namespaceRootPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new DLInterruptedException("Interrupted while deleting " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
            throw new IOException("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
        }
    }

    public static Map<String, byte[]> enumerateLogsWithMetadataInNamespace(final DistributedLogConfiguration conf, final URI uri)
        throws IOException, IllegalArgumentException {
        return withZooKeeperClient(new ZooKeeperClientHandler<Map<String, byte[]>>() {
            @Override
            public Map<String, byte[]> handle(ZooKeeperClient zkc) throws IOException {
                return enumerateLogsWithMetadataInternal(zkc, conf, uri);
            }
        }, conf, uri);
    }

    private static Map<String, byte[]> enumerateLogsWithMetadataInternal(ZooKeeperClient zkc,
                                                                         DistributedLogConfiguration conf, URI uri)
        throws IOException, IllegalArgumentException {
        validateConfAndURI(conf, uri);
        String namespaceRootPath = uri.getPath();
        HashMap<String, byte[]> result = new HashMap<String, byte[]>();
        try {
            ZooKeeper zk = Utils.sync(zkc, namespaceRootPath);
            Stat currentStat = zk.exists(namespaceRootPath, false);
            if (currentStat == null) {
                return result;
            }
            List<String> children = zk.getChildren(namespaceRootPath, false);
            for(String child: children) {
                String zkPath = String.format("%s/%s", namespaceRootPath, child);
                currentStat = zk.exists(zkPath, false);
                if (currentStat == null) {
                    result.put(child, new byte[0]);
                } else {
                    result.put(child, zk.getData(zkPath, false, currentStat));
                }
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while reading {}", namespaceRootPath, ie);
            throw new DLInterruptedException("Interrupted while reading " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading " + namespaceRootPath + " entry in zookeeper", ke);
            throw new IOException("Error reading " + namespaceRootPath + " entry in zookeeper", ke);
        }
        return result;
    }

    /**
     * Close the distributed log manager factory, freeing any resources it may hold.
     */
    public void close() {
        ZooKeeperClient writerZKC;
        ZooKeeperClient readerZKC;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            writerZKC = sharedWriterZKCForBK;
            readerZKC = sharedReaderZKCForBK;
        }

        SchedulerUtils.shutdownScheduler(scheduledExecutorService, 5000, TimeUnit.MILLISECONDS);
        LOG.info("Executor Service Stopped.");

        writerBKC.close();
        readerBKC.close();
        sharedWriterZKCForDL.close();
        sharedReaderZKCForDL.close();

        // Close shared zookeeper clients for bk
        if (null != writerZKC) {
            writerZKC.close();
        }
        if (null != readerZKC) {
            readerZKC.close();
        }
        channelFactory.releaseExternalResources();
        LOG.info("Release external resources used by channel factory.");
        requestTimer.stop();
        LOG.info("Stopped request timer");
        SchedulerUtils.shutdownScheduler(lockStateExecutor, 5000, TimeUnit.MILLISECONDS);
        LOG.info("Stopped lock state executor");
    }
}
