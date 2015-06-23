package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DistributedLogManagerFactory.ClientSharingOption;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.acl.DefaultAccessControlManager;
import com.twitter.distributedlog.acl.ZKAccessControlManager;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.LedgerAllocatorUtils;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.feature.CoreFeatureKeys;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.stats.ReadAheadExceptionsLogger;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.LimitedPermitManager;
import com.twitter.distributedlog.util.MonitoredScheduledThreadPoolExecutor;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DistributedLog Namespace uses zookeeper for metadata store and bookkeeper for data store.
 */
public class BKDistributedLogNamespace
        implements Watcher, AsyncCallback.Children2Callback, Runnable, DistributedLogNamespace {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogNamespace.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private DistributedLogConfiguration _conf = null;
        private URI _uri = null;
        private StatsLogger _statsLogger = NullStatsLogger.INSTANCE;
        private FeatureProvider _featureProvider = new SettableFeatureProvider("", 0);
        private String _clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
        private int _regionId = DistributedLogConstants.LOCAL_REGION_ID;

        private Builder() {}

        public Builder conf(DistributedLogConfiguration conf) {
            this._conf = conf;
            return this;
        }

        public Builder uri(URI uri) {
            this._uri = uri;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this._statsLogger = statsLogger;
            return this;
        }

        public Builder featureProvider(FeatureProvider featureProvider) {
            this._featureProvider = featureProvider;
            return this;
        }

        public Builder clientId(String clientId) {
            this._clientId = clientId;
            return this;
        }

        public Builder regionId(int regionId) {
            this._regionId = regionId;
            return this;
        }

        public BKDistributedLogNamespace build()
                throws IOException, NullPointerException, IllegalArgumentException {
            Preconditions.checkNotNull(_conf, "No DistributedLog Configuration");
            Preconditions.checkNotNull(_uri, "No DistributedLog URI");
            Preconditions.checkNotNull(_featureProvider, "No Feature Provider");
            Preconditions.checkNotNull(_statsLogger, "No Stats Logger");
            Preconditions.checkNotNull(_featureProvider, "No Feature Provider");
            Preconditions.checkNotNull(_clientId, "No Client ID");
            return new BKDistributedLogNamespace(
                    _conf,
                    _uri,
                    _featureProvider,
                    _statsLogger,
                    _clientId,
                    _regionId);
        }
    }


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

    static boolean isReservedStreamName(String name) {
        return name.startsWith(".");
    }

    private final String clientId;
    private final int regionId;
    private DistributedLogConfiguration conf;
    private URI namespace;
    private final OrderedScheduler scheduler;
    private final ScheduledExecutorService readAheadExecutor;
    private final OrderedSafeExecutor lockStateExecutor;
    private final ClientSocketChannelFactory channelFactory;
    private final HashedWheelTimer requestTimer;
    // zookeeper clients
    // NOTE: The actual zookeeper client is initialized lazily when it is referenced by
    //       {@link com.twitter.distributedlog.ZooKeeperClient#get()}. So it is safe to
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
    //       {@link com.twitter.distributedlog.BookKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    private final BookKeeperClientBuilder sharedWriterBKCBuilder;
    private final BookKeeperClient writerBKC;
    private final BookKeeperClientBuilder sharedReaderBKCBuilder;
    private final BookKeeperClient readerBKC;
    // ledger allocator
    private final LedgerAllocator allocator;
    // access control manager
    private AccessControlManager accessControlManager;
    // log segment rolling permit manager
    private final PermitManager logSegmentRollingPermitManager;
    // namespace listener
    private final AtomicBoolean namespaceWatcherSet = new AtomicBoolean(false);
    private final CopyOnWriteArraySet<NamespaceListener> namespaceListeners =
            new CopyOnWriteArraySet<NamespaceListener>();
    // feature provider
    private final FeatureProvider featureProvider;

    // Stats Loggers
    private final StatsLogger statsLogger;
    private final ReadAheadExceptionsLogger readAheadExceptionsLogger;

    protected boolean closed = false;

    private final PermitLimiter writeLimiter;

    private BKDistributedLogNamespace(
            DistributedLogConfiguration conf,
            URI uri,
            FeatureProvider featureProvider,
            StatsLogger statsLogger,
            String clientId,
            int regionId)
            throws IOException, IllegalArgumentException {
        validateConfAndURI(conf, uri);
        this.conf = conf;
        this.namespace = uri;
        this.featureProvider = featureProvider;
        this.statsLogger = statsLogger;
        this.clientId = clientId;
        this.regionId = regionId;
        this.scheduler = OrderedScheduler.newBuilder()
                .name("DLM-" + uri.getPath())
                .corePoolSize(conf.getNumWorkerThreads())
                .statsLogger(statsLogger.scope("factory").scope("thread_pool"))
                .traceTaskExecution(conf.getEnableTaskExecutionStats())
                .traceTaskExecutionWarnTimeUs(conf.getTaskExecutionWarnTimeMicros())
                .build();
        if (conf.getNumReadAheadWorkerThreads() > 0) {
            this.readAheadExecutor = new MonitoredScheduledThreadPoolExecutor(
                    conf.getNumReadAheadWorkerThreads(),
                    new ThreadFactoryBuilder().setNameFormat("DLM-" + uri.getPath() + "-readahead-executor-%d").build(),
                    statsLogger.scope("factory").scope("readahead_thread_pool"),
                    conf.getTraceReadAheadDeliveryLatency()
            );
            LOG.info("Created dedicated readahead executor : threads = {}", conf.getNumReadAheadWorkerThreads());
        } else {
            this.readAheadExecutor = this.scheduler;
            LOG.info("Used shared executor for readahead.");
        }
        this.lockStateExecutor = OrderedSafeExecutor.newBuilder()
                .name("DLM-LockState")
                .numThreads(conf.getNumLockStateThreads())
                .statsLogger(statsLogger)
                .build();
        this.channelFactory = new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("DL-netty-boss-%d").build()),
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("DL-netty-worker-%d").build()),
            conf.getBKClientNumberIOThreads());
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
                bkdlConfig.getBkLedgersPath(),
                Optional.of(featureProvider.scope("bkc")));
        this.writerBKC = this.sharedWriterBKCBuilder.build();

        // Build bookkeeper client for readers
        if (bkdlConfig.getBkZkServersForWriter().equals(bkdlConfig.getBkZkServersForReader())) {
            this.sharedReaderBKCBuilder = this.sharedWriterBKCBuilder;
        } else {
            this.sharedReaderBKCBuilder = createBKCBuilder(
                String.format("bk:%s:factory_reader_shared", namespace),
                conf,
                bkdlConfig.getBkZkServersForReader(),
                bkdlConfig.getBkLedgersPath(),
                Optional.<FeatureProvider>absent());
        }
        this.readerBKC = this.sharedReaderBKCBuilder.build();

        this.logSegmentRollingPermitManager = new LimitedPermitManager(
                conf.getLogSegmentRollingConcurrency(), 1, TimeUnit.MINUTES, scheduler);

        if (conf.getGlobalOutstandingWriteLimit() < 0) {
            this.writeLimiter = PermitLimiter.NULL_PERMIT_LIMITER;
        } else {
            Feature disableWriteLimitFeature = featureProvider.getFeature(
                CoreFeatureKeys.DISABLE_WRITE_LIMIT.name().toLowerCase());
            this.writeLimiter = new SimplePermitLimiter(
                conf.getOutstandingWriteLimitDarkmode(),
                conf.getGlobalOutstandingWriteLimit(),
                statsLogger.scope("writeLimiter"),
                true /* singleton */,
                disableWriteLimitFeature);
        }

        // propagate bkdlConfig to configuration
        BKDLConfig.propagateConfiguration(bkdlConfig, conf);

        // Build the allocator
        if (conf.getEnableLedgerAllocatorPool()) {
            String allocatorPoolPath = validateAndGetFullLedgerAllocatorPoolPath(conf, uri);
            allocator = LedgerAllocatorUtils.createLedgerAllocatorPool(allocatorPoolPath, conf.getLedgerAllocatorPoolCoreSize(),
                    conf, sharedWriterZKCForDL, writerBKC, scheduler);
            if (null != allocator) {
                allocator.start();
            }
            LOG.info("Created ledger allocator pool under {} with size {}.", allocatorPoolPath, conf.getLedgerAllocatorPoolCoreSize());
        } else {
            allocator = null;
        }

        // Stats Loggers
        this.readAheadExceptionsLogger = new ReadAheadExceptionsLogger(statsLogger);

        LOG.info("Constructed DLM Factory : clientId = {}, regionId = {}.", clientId, regionId);
    }

    //
    // Namespace Methods
    //


    @Override
    public void createLog(String logName)
            throws InvalidStreamNameException, IOException {
        validateName(logName);
        createUnpartitionedStreams(conf, namespace, Lists.newArrayList(logName));
    }

    @Override
    public void deleteLog(String logName)
            throws InvalidStreamNameException, LogNotFoundException, IOException {
        validateName(logName);
        DistributedLogManager dlm = createDistributedLogManagerWithSharedClients(logName);
        dlm.delete();
    }

    @Override
    public DistributedLogManager openLog(String logName)
            throws InvalidStreamNameException, IOException {
        return createDistributedLogManagerWithSharedClients(logName);
    }

    @Override
    public DistributedLogManager openLog(String logName,
                                         Optional<DistributedLogConfiguration> logConf,
                                         Optional<DynamicDistributedLogConfiguration> dynamicLogConf)
            throws InvalidStreamNameException, IOException {
        return createDistributedLogManager(logName, ClientSharingOption.SharedClients, logConf, dynamicLogConf);
    }

    @Override
    public boolean logExists(String logName)
        throws IOException, IllegalArgumentException {
        return checkIfLogExists(conf, namespace, logName);
    }

    @Override
    public Iterator<String> getLogs() throws IOException {
        return enumerateAllLogsInNamespace().iterator();
    }

    @Override
    public void registerNamespaceListener(NamespaceListener listener) {
        if (namespaceListeners.add(listener) && namespaceWatcherSet.compareAndSet(false, true)) {
            watchNamespaceChanges();
        }
    }

    @Override
    public synchronized AccessControlManager createAccessControlManager() throws IOException {
        // Resolve uri to get bk dl config
        BKDLConfig bkdlConfig = resolveBKDLConfig();
        if (null == accessControlManager) {
            String aclRootPath = bkdlConfig.getACLRootPath();
            // Build the access control manager
            if (aclRootPath == null) {
                accessControlManager = DefaultAccessControlManager.INSTANCE;
                LOG.info("Created default access control manager for {}", namespace);
            } else {
                if (!isReservedStreamName(aclRootPath)) {
                    throw new IOException("Invalid Access Control List Root Path : " + aclRootPath);
                }
                String zkRootPath = namespace.getPath() + "/" + aclRootPath;
                LOG.info("Creating zk based access control manager @ {} for {}",
                        zkRootPath, namespace);
                accessControlManager = new ZKAccessControlManager(conf, sharedReaderZKCForDL,
                        zkRootPath, scheduler);
                LOG.info("Created zk based access control manager @ {} for {}",
                        zkRootPath, namespace);
            }
        }
        return accessControlManager;
    }

    //
    // Legacy methods
    //

    static String validateAndGetFullLedgerAllocatorPoolPath(DistributedLogConfiguration conf, URI uri) throws IOException {
        String poolPath = conf.getLedgerAllocatorPoolPath();
        LOG.info("PoolPath is {}", poolPath);
        if (null == poolPath || !poolPath.startsWith(".") || poolPath.endsWith("/")) {
            LOG.error("Invalid ledger allocator pool path specified when enabling ledger allocator pool : {}", poolPath);
            throw new IOException("Invalid ledger allocator pool path specified : " + poolPath);
        }
        String poolName = conf.getLedgerAllocatorPoolName();
        if (null == poolName) {
            LOG.error("No ledger allocator pool name specified when enabling ledger allocator pool.");
            throw new IOException("No ledger allocator name specified when enabling ledger allocator pool.");
        }
        String rootPath = uri.getPath() + "/" + poolPath + "/" + poolName;
        try {
            PathUtils.validatePath(rootPath);
        } catch (IllegalArgumentException iae) {
            LOG.error("Invalid ledger allocator pool path specified when enabling ledger allocator pool : {}", poolPath);
            throw new IOException("Invalid ledger allocator pool path specified : " + poolPath);
        }
        return rootPath;
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
                                                     String ledgersPath,
                                                     Optional<FeatureProvider> featureProviderOptional) {
        BookKeeperClientBuilder builder = BookKeeperClientBuilder.newBuilder()
                .name(bkcName)
                .dlConfig(conf)
                .zkServers(zkServers)
                .ledgersPath(ledgersPath)
                .channelFactory(channelFactory)
                .requestTimer(requestTimer)
                .featureProvider(featureProviderOptional)
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

    private void scheduleTask(Runnable r, long ms) {
        try {
            scheduler.schedule(r, ms, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            LOG.error("Task {} scheduled in {} ms is rejected : ", new Object[] { r, ms, ree });
        }
    }

    @Override
    public void run() {
        watchNamespaceChanges();
    }

    private void watchNamespaceChanges() {
        try {
            sharedReaderZKCForDL.get().getChildren(namespace.getPath(), this, this, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        } catch (InterruptedException e) {
            LOG.warn("Interrupted on watching namespace changes for {} : ", namespace, e);
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        if (KeeperException.Code.OK.intValue() == rc) {
            LOG.info("Received updated streams under {} : {}", namespace, children);
            List<String> result = new ArrayList<String>(children.size());
            for (String s : children) {
                if (isReservedStreamName(s)) {
                    continue;
                }
                result.add(s);
            }
            for (NamespaceListener listener : namespaceListeners) {
                listener.onStreamsChanged(result.iterator());
            }
        } else {
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            if (event.getState() == Event.KeeperState.Expired) {
                scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
            }
            return;
        }
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            // watch namespace changes again.
            watchNamespaceChanges();
        }
    }

    /**
     * Create a DistributedLogManager for <i>nameOfLogStream</i>, with default shared clients.
     *
     * @param nameOfLogStream
     *          name of log stream
     * @return distributedlog manager
     * @throws com.twitter.distributedlog.exceptions.InvalidStreamNameException if stream name is invalid
     * @throws IOException
     */
    public DistributedLogManager createDistributedLogManagerWithSharedClients(String nameOfLogStream)
        throws InvalidStreamNameException, IOException {
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
     * @throws com.twitter.distributedlog.exceptions.InvalidStreamNameException if stream name is invalid
     * @throws IOException
     */
    public DistributedLogManager createDistributedLogManager(
            String nameOfLogStream,
            ClientSharingOption clientSharingOption)
        throws InvalidStreamNameException, IOException {
        Optional<DistributedLogConfiguration> streamConfiguration = Optional.absent();
        Optional<DynamicDistributedLogConfiguration> dynamicStreamConfiguration = Optional.absent();
        return createDistributedLogManager(
                nameOfLogStream,
                clientSharingOption,
                streamConfiguration,
                dynamicStreamConfiguration);
    }

    /**
     * Create a DistributedLogManager for <i>nameOfLogStream</i>, with specified client sharing options.
     * This method allows the caller to override global configuration options by supplying stream
     * configuration overrides. Stream config overrides come in two flavors, static and dynamic. Static
     * config never changes, and DynamicDistributedLogConfiguration is a) reloaded periodically and
     * b) safe to access from any context.
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @param clientSharingOption
     *          specifies if the ZK/BK clients are shared
     * @param streamConfiguration
     *          stream configuration overrides.
     * @param dynamicStreamConfiguration
     *          dynamic stream configuration overrides.
     * @return distributedlog manager instance.
     * @throws com.twitter.distributedlog.exceptions.InvalidStreamNameException if stream name is invalid
     * @throws IOException
     */
    public DistributedLogManager createDistributedLogManager(
            String nameOfLogStream,
            ClientSharingOption clientSharingOption,
            Optional<DistributedLogConfiguration> streamConfiguration,
            Optional<DynamicDistributedLogConfiguration> dynamicStreamConfiguration)
        throws InvalidStreamNameException, IOException {
        // Make sure the name is well formed
        validateName(nameOfLogStream);

        DistributedLogConfiguration mergedConfiguration = new DistributedLogConfiguration();
        mergedConfiguration.addConfiguration(conf);
        mergedConfiguration.loadStreamConf(streamConfiguration);
        // If dynamic config was not provided, default to a static view of the global configuration.
        DynamicDistributedLogConfiguration dynConf = null;
        if (dynamicStreamConfiguration.isPresent()) {
            dynConf = dynamicStreamConfiguration.get();
        } else {
            dynConf = ConfUtils.getConstDynConf(mergedConfiguration);
        }

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

        LedgerAllocator dlmLedgerAlloctor = null;
        PermitManager dlmLogSegmentRollingPermitManager = PermitManager.UNLIMITED_PERMIT_MANAGER;
        if (ClientSharingOption.SharedClients == clientSharingOption) {
            dlmLedgerAlloctor = this.allocator;
            dlmLogSegmentRollingPermitManager = this.logSegmentRollingPermitManager;
        }

        return new BKDistributedLogManager(
                nameOfLogStream,                    /* Log Name */
                mergedConfiguration,                /* Configuration */
                dynConf,                            /* Dynamic Configuration */
                namespace,                          /* Namespace */
                writerZKCBuilderForDL,              /* ZKC Builder for DL Writer */
                readerZKCBuilderForDL,              /* ZKC Builder for DL Reader */
                writerZKCForBK,                     /* ZKC for BookKeeper for DL Writers */
                readerZKCForBK,                     /* ZKC for BookKeeper for DL Readers */
                writerBKCBuilder,                   /* BookKeeper Builder for DL Writers */
                readerBKCBuilder,                   /* BookKeeper Builder for DL Readers */
                scheduler,                          /* DL scheduler */
                readAheadExecutor,                  /* Read Aheader Executor */
                lockStateExecutor,                  /* Lock State Executor */
                channelFactory,                     /* Netty Channel Factory */
                requestTimer,                       /* Request Timer */
                readAheadExceptionsLogger,          /* ReadAhead Exceptions Logger */
                clientId,                           /* Client Id */
                regionId,                           /* Region Id */
                dlmLedgerAlloctor,                  /* Ledger Allocator */
                writeLimiter,                       /* Write Limiter */
                dlmLogSegmentRollingPermitManager,  /* Log segment rolling limiter */
                featureProvider.scope("dl"),        /* Feature Provider */
                statsLogger                         /* Stats Logger */
        );
    }

    public MetadataAccessor createMetadataAccessor(String nameOfMetadataNode)
            throws InvalidStreamNameException, IOException {
        validateName(nameOfMetadataNode);
        return new ZKMetadataAccessor(nameOfMetadataNode, conf, namespace,
                sharedWriterZKCBuilderForDL, sharedReaderZKCBuilderForDL, statsLogger);
    }

    public Collection<String> enumerateAllLogsInNamespace()
        throws IOException, IllegalArgumentException {
        return withZooKeeperClient(new ZooKeeperClientHandler<Collection<String>>() {
            @Override
            public Collection<String> handle(ZooKeeperClient zkc) throws IOException {
                return enumerateAllLogsInternal(zkc, conf, namespace);
            }
        });
    }

    public Map<String, byte[]> enumerateLogsWithMetadataInNamespace()
        throws IOException, IllegalArgumentException {
        return withZooKeeperClient(new ZooKeeperClientHandler<Map<String, byte[]>>() {
            @Override
            public Map<String, byte[]> handle(ZooKeeperClient zkc) throws IOException {
                return enumerateLogsWithMetadataInternal(zkc, conf, namespace);
            }
        });
    }

    private static void validateInput(DistributedLogConfiguration conf, URI uri, String nameOfStream)
        throws IllegalArgumentException, InvalidStreamNameException {
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

    private static void validateName(String nameOfStream) throws InvalidStreamNameException {
        if (nameOfStream.contains("/")) {
            throw new InvalidStreamNameException(nameOfStream, "Stream Name contains invalid characters");
        }
        if (isReservedStreamName(nameOfStream)) {
            throw new InvalidStreamNameException(nameOfStream, "Stream Name is reserved");
        }
    }

    public static boolean checkIfLogExists(DistributedLogConfiguration conf, URI uri, String name)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri, name);
        final String logRootPath = uri.getPath() + String.format("/%s", name);
        return withZooKeeperClient(new ZooKeeperClientHandler<Boolean>() {
            @Override
            public Boolean handle(ZooKeeperClient zkc) throws IOException {
                try {
                    if (null != zkc.get().exists(logRootPath, false)) {
                        return true;
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted checkIfLogExists  " + logRootPath, ie);
                } catch (KeeperException ke) {
                    LOG.error("Error reading" + logRootPath + "entry in zookeeper", ke);
                }
                return false;
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
            Stat currentStat = zkc.get().exists(namespaceRootPath, false);
            if (currentStat == null) {
                return new LinkedList<String>();
            }
            List<String> children = zkc.get().getChildren(namespaceRootPath, false);
            List<String> result = new ArrayList<String>(children.size());
            for (String child : children) {
                if (!isReservedStreamName(child)) {
                    result.add(child);
                }
            }
            return result;
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new IOException("Interrupted while deleting " + namespaceRootPath, ie);
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
            Stat currentStat = zkc.get().exists(namespaceRootPath, false);
            if (currentStat == null) {
                return result;
            }
            List<String> children = zkc.get().getChildren(namespaceRootPath, false);
            for(String child: children) {
                if (isReservedStreamName(child)) {
                    continue;
                }
                String zkPath = String.format("%s/%s", namespaceRootPath, child);
                currentStat = zkc.get().exists(zkPath, false);
                if (currentStat == null) {
                    result.put(child, new byte[0]);
                } else {
                    result.put(child, zkc.get().getData(zkPath, false, currentStat));
                }
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new IOException("Interrupted while reading " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
            throw new IOException("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
        }
        return result;
    }

    public static void createUnpartitionedStreams(final DistributedLogConfiguration conf, final URI uri,
                                                  final List<String> streamNames)
        throws IOException, IllegalArgumentException {
        withZooKeeperClient(new ZooKeeperClientHandler<Void>() {
            @Override
            public Void handle(ZooKeeperClient zkc) throws IOException {
                for (String s : streamNames) {
                    try {
                        BKDistributedLogManager.createUnpartitionedStream(conf, zkc, uri, s);
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted on creating unpartitioned stream {} : ", s, e);
                        return null;
                    }
                }
                return null;
            }
        }, conf, uri);
    }

    /**
     * Close the distributed log manager factory, freeing any resources it may hold.
     */
    @Override
    public void close() {
        ZooKeeperClient writerZKC;
        ZooKeeperClient readerZKC;
        AccessControlManager acm;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            writerZKC = sharedWriterZKCForBK;
            readerZKC = sharedReaderZKCForBK;
            acm = accessControlManager;
        }

        if (null != acm) {
            acm.close();
            LOG.info("Access Control Manager Stopped.");
        }

        SchedulerUtils.shutdownScheduler(scheduler, conf.getSchedulerShutdownTimeoutMs(),
                TimeUnit.MILLISECONDS);
        LOG.info("Executor Service Stopped.");
        if (scheduler != readAheadExecutor) {
            SchedulerUtils.shutdownScheduler(readAheadExecutor, conf.getSchedulerShutdownTimeoutMs(),
                    TimeUnit.MILLISECONDS);
            LOG.info("ReadAhead Executor Service Stopped.");
        }
        if (null != allocator) {
            allocator.close(false);
            LOG.info("Ledger Allocator stopped.");
        }
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
