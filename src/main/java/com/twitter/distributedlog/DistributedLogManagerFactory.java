package com.twitter.distributedlog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.LedgerAllocatorUtils;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.LimitedPermitManager;
import com.twitter.distributedlog.util.MonitoredScheduledThreadPoolExecutor;
import com.twitter.distributedlog.util.PermitManager;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedLogManagerFactory implements Watcher, AsyncCallback.Children2Callback, Runnable {
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
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .uri(namespace)
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .buildNew();
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
    private final StatsLogger statsLogger;
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private final ScheduledThreadPoolExecutor readAheadExecutor;
    private final ClientSocketChannelFactory channelFactory;
    private final HashedWheelTimer requestTimer;
    // zk & bk client
    private final ZooKeeperClientBuilder sharedZKClientBuilderForDL;
    private final ZooKeeperClient sharedZKClientForDL;
    private ZooKeeperClientBuilder sharedZKClientBuilderForBK;
    private ZooKeeperClient sharedZKClientForBK = null;
    private final BookKeeperClientBuilder bookKeeperClientBuilder;
    private BookKeeperClient bookKeeperClient = null;
    // ledger allocator
    private final LedgerAllocator allocator;
    // log segment rolling permit manager
    private final PermitManager logSegmentRollingPermitManager;
    // namespace listener
    private final AtomicBoolean namespaceWatcherSet = new AtomicBoolean(false);
    private final CopyOnWriteArraySet<NamespaceListener> namespaceListeners =
            new CopyOnWriteArraySet<NamespaceListener>();

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri) throws IOException, IllegalArgumentException {
        this(conf, uri, NullStatsLogger.INSTANCE);
    }

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri,
                                        StatsLogger statsLogger) throws IOException, IllegalArgumentException {
        this(conf, uri, statsLogger, DistributedLogConstants.UNKNOWN_CLIENT_ID, DistributedLogConstants.LOCAL_REGION_ID);
    }

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri,
                                        StatsLogger statsLogger, String clientId, int regionId)
            throws IOException, IllegalArgumentException {
        validateConfAndURI(conf, uri);
        this.conf = conf;
        this.namespace = uri;
        this.statsLogger = statsLogger;
        this.clientId = clientId;
        this.regionId = regionId;
        this.scheduledThreadPoolExecutor = new MonitoredScheduledThreadPoolExecutor(
                conf.getNumWorkerThreads(),
                new ThreadFactoryBuilder().setNameFormat("DLM-" + uri.getPath() + "-executor-%d").build(),
                statsLogger.scope("factory").scope("thread_pool"),
                conf.getTraceReadAheadDeliveryLatency()
        );
        if (conf.getNumReadAheadWorkerThreads() > 0) {
            this.readAheadExecutor = new MonitoredScheduledThreadPoolExecutor(
                    conf.getNumReadAheadWorkerThreads(),
                    new ThreadFactoryBuilder().setNameFormat("DLM-" + uri.getPath() + "-readahead-executor-%d").build(),
                    statsLogger.scope("factory").scope("readahead_thread_pool"),
                    conf.getTraceReadAheadDeliveryLatency()
            );
            LOG.info("Created dedicated readahead executor : threads = {}", conf.getNumReadAheadWorkerThreads());
        } else {
            this.readAheadExecutor = this.scheduledThreadPoolExecutor;
            LOG.info("Used shared executor for readahead.");
        }
        this.channelFactory = new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("DL-netty-boss-%d").build()),
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("DL-netty-worker-%d").build()),
            conf.getBKClientNumberIOThreads());
        this.requestTimer = new HashedWheelTimer(
            new ThreadFactoryBuilder().setNameFormat("DLFactoryTimer-%d").build(),
            conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
            conf.getTimeoutTimerNumTicks());
        // Build zookeeper client
        RetryPolicy retryPolicy = null;
        if (conf.getZKNumRetries() > 0) {
            retryPolicy = new BoundExponentialBackoffRetryPolicy(
                conf.getZKRetryBackoffStartMillis(),
                conf.getZKRetryBackoffMaxMillis(), conf.getZKNumRetries());
        }
        this.sharedZKClientBuilderForDL = ZooKeeperClientBuilder.newBuilder()
            .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
            .retryThreadCount(conf.getZKClientNumberRetryThreads())
            .uri(uri)
            .retryPolicy(retryPolicy)
            .statsLogger(statsLogger)
            .buildNew(conf.getSeparateZKClients());
        LOG.info("ZooKeeper Client : numRetries = {}, sessionTimeout = {}, retryBackoff = {}," +
            " maxRetryBackoff = {}.", new Object[] { conf.getZKNumRetries(), conf.getZKSessionTimeoutMilliseconds(),
            conf.getZKRetryBackoffStartMillis(), conf.getZKRetryBackoffMaxMillis() });
        this.sharedZKClientForDL = this.sharedZKClientBuilderForDL.build();
        // Resolve uri to get bk dl config
        BKDLConfig bkdlConfig = resolveBKDLConfig();
        // Build bookkeeper client
        this.bookKeeperClientBuilder = BookKeeperClientBuilder.newBuilder()
                .dlConfig(conf).bkdlConfig(bkdlConfig).name(String.format("%s:shared", namespace))
                .channelFactory(channelFactory).requestTimer(requestTimer).statsLogger(statsLogger);
        LOG.info("BookKeeper Client : numIOThreads = {}, numWorkerThreads = {}",
                 conf.getBKClientNumberIOThreads(), conf.getBKClientNumberWorkerThreads());

        this.logSegmentRollingPermitManager = new LimitedPermitManager(
                conf.getLogSegmentRollingConcurrency(), 1, TimeUnit.MINUTES, scheduledThreadPoolExecutor);

        // propagate bkdlConfig to configuration
        BKDLConfig.propagateConfiguration(bkdlConfig, conf);

        // Build the allocator
        if (conf.getEnableLedgerAllocatorPool()) {
            String poolName = conf.getLedgerAllocatorPoolName();
            if (null == poolName) {
                LOG.error("No ledger allocator pool name specified when enabling ledger allocator pool.");
                throw new IOException("No ledger allocator name specified when enabling ledger allocator pool.");
            }
            String rootPath = uri.getPath() + "/" + DistributedLogConstants.ALLOCATION_POOL_NODE + "/" + poolName;
            getBookKeeperClientBuilder();
            allocator = LedgerAllocatorUtils.createLedgerAllocatorPool(rootPath, conf.getLedgerAllocatorPoolCoreSize(), conf,
                sharedZKClientForDL, getBookKeeperClient());
            if (null != allocator) {
                allocator.start();
            }
            LOG.info("Created ledger allocator pool under {} with size {}.", rootPath, conf.getLedgerAllocatorPoolCoreSize());
        } else {
            allocator = null;
        }

        LOG.info("Constructed DLM Factory : clientId = {}, regionId = {}.", clientId, regionId);
    }

    synchronized BookKeeperClientBuilder getBookKeeperClientBuilder() throws IOException {
        if (null == bookKeeperClient) {
            // get a reference of shared bookkeeper client
            try {
                bookKeeperClient = bookKeeperClientBuilder.build();
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while building bookkeeper client", ie);
                throw new IOException("Error building bookkeeper client", ie);
            } catch (KeeperException ke) {
                LOG.error("Error building bookkeeper client", ke);
                throw new IOException("Error building bookkeeper client", ke);
            }
        }
        return bookKeeperClientBuilder;
    }

    synchronized BookKeeperClient getBookKeeperClient() {
        return bookKeeperClient;
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
        return handler.handle(sharedZKClientForDL);
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
            scheduledThreadPoolExecutor.schedule(r, ms, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            LOG.error("Task {} scheduled in {} ms is rejected : ", new Object[] { r, ms, ree });
        }
    }

    public DistributedLogManagerFactory registerNamespaceListener(NamespaceListener listener) {
        if (namespaceListeners.add(listener) && namespaceWatcherSet.compareAndSet(false, true)) {
            watchNamespaceChanges();
        }
        return this;
    }

    @Override
    public void run() {
        watchNamespaceChanges();
    }

    private void watchNamespaceChanges() {
        try {
            sharedZKClientForDL.get().getChildren(namespace.getPath(), this, this, null);
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
                listener.onStreamsChanged(result);
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
    public DistributedLogManager createDistributedLogManager(String nameOfLogStream) throws IOException, IllegalArgumentException {
        DistributedLogManagerFactory.validateName(nameOfLogStream);
        BKDistributedLogManager distLogMgr = new BKDistributedLogManager(nameOfLogStream, conf, namespace,
            null, null, scheduledThreadPoolExecutor, readAheadExecutor, channelFactory, requestTimer, statsLogger);
        distLogMgr.setClientId(clientId);
        return distLogMgr;
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
    public DistributedLogManager createDistributedLogManagerWithSharedZK(String nameOfLogStream) throws IOException, IllegalArgumentException {
        DistributedLogManagerFactory.validateName(nameOfLogStream);
        if(null == this.sharedZKClientForBK) {
            RetryPolicy retryPolicy = null;
            if (conf.getZKNumRetries() > 0) {
                retryPolicy = new BoundExponentialBackoffRetryPolicy(
                    conf.getZKRetryBackoffStartMillis(),
                    conf.getZKRetryBackoffMaxMillis(), conf.getZKNumRetries());
            }

            this.sharedZKClientBuilderForBK = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .uri(namespace)
                .retryPolicy(retryPolicy)
                .statsLogger(statsLogger)
                .buildNew(conf.getSeparateZKClients());
            LOG.info("ZooKeeper Client : numRetries = {}, sessionTimeout = {}, retryBackoff = {}," +
                " maxRetryBackoff = {}.", new Object[] { conf.getZKNumRetries(), conf.getZKSessionTimeoutMilliseconds(),
                conf.getZKRetryBackoffStartMillis(), conf.getZKRetryBackoffMaxMillis() });
            this.sharedZKClientForBK = this.sharedZKClientBuilderForBK.build();
        }

        BKDLConfig bkdlConfig = resolveBKDLConfig();
        // Build bookkeeper client
        BookKeeperClientBuilder bookKeeperClientBuilder = BookKeeperClientBuilder.newBuilder()
            .dlConfig(conf).bkdlConfig(bkdlConfig).name(String.format("%s:shared", nameOfLogStream))
            .zkc(sharedZKClientForBK).channelFactory(channelFactory)
            .requestTimer(requestTimer).statsLogger(statsLogger);

        BKDistributedLogManager distLogMgr = new BKDistributedLogManager(nameOfLogStream, conf, namespace,
                sharedZKClientBuilderForDL, bookKeeperClientBuilder, scheduledThreadPoolExecutor, readAheadExecutor,
                channelFactory, requestTimer, statsLogger);
        distLogMgr.setClientId(clientId);
        distLogMgr.setRegionId(regionId);
        distLogMgr.setLedgerAllocator(allocator);
        distLogMgr.setLogSegmentRollingPermitManager(logSegmentRollingPermitManager);
        return distLogMgr;
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
    public DistributedLogManager createDistributedLogManagerWithSharedClients(String nameOfLogStream)
        throws IOException, IllegalArgumentException {
        DistributedLogManagerFactory.validateName(nameOfLogStream);
        BKDistributedLogManager distLogMgr = new BKDistributedLogManager(nameOfLogStream, conf, namespace,
            sharedZKClientBuilderForDL, getBookKeeperClientBuilder(), scheduledThreadPoolExecutor, readAheadExecutor,
            channelFactory, requestTimer, statsLogger);
        distLogMgr.setClientId(clientId);
        distLogMgr.setRegionId(regionId);
        distLogMgr.setLedgerAllocator(allocator);
        distLogMgr.setLogSegmentRollingPermitManager(logSegmentRollingPermitManager);
        return distLogMgr;
    }

    public MetadataAccessor createMetadataAccessor(String nameOfMetadataNode) throws IOException, IllegalArgumentException {
        DistributedLogManagerFactory.validateName(nameOfMetadataNode);
        return new ZKMetadataAccessor(nameOfMetadataNode, conf, namespace, sharedZKClientBuilderForDL);
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

    public static DistributedLogManager createDistributedLogManager(String name, URI uri) throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, new DistributedLogConfiguration(), uri);
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

    public static DistributedLogManager createDistributedLogManager(
            String name,
            DistributedLogConfiguration conf,
            URI uri)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, (ZooKeeperClientBuilder) null, (BookKeeperClientBuilder) null,
                NullStatsLogger.INSTANCE);
    }

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

    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                                                    ZooKeeperClient zkc, BookKeeperClient bkc, StatsLogger statsLogger)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, ZooKeeperClientBuilder.newBuilder().zkc(zkc),
                BookKeeperClientBuilder.newBuilder().bkc(bkc).statsLogger(statsLogger), statsLogger);
    }

    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                ZooKeeperClientBuilder zkcBuilder, BookKeeperClientBuilder bkcBuilder, StatsLogger statsLogger)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri, name);
        return new BKDistributedLogManager(name, conf, uri, zkcBuilder, bkcBuilder, statsLogger);
    }

    public static MetadataAccessor createMetadataAccessor(
            String name,
            URI uri,
            DistributedLogConfiguration conf)
        throws IOException, IllegalArgumentException {
        return createMetadataAccessor(name, uri, conf, (ZooKeeperClientBuilder) null);
    }

    public static MetadataAccessor createMetadataAccessor(
            String name,
            URI uri,
            DistributedLogConfiguration conf,
            ZooKeeperClient zkc)
        throws IOException, IllegalArgumentException {
        return createMetadataAccessor(name, uri, conf, ZooKeeperClientBuilder.newBuilder().zkc(zkc));
    }

    public static MetadataAccessor createMetadataAccessor(
            String name,
            URI uri,
            DistributedLogConfiguration conf,
            ZooKeeperClientBuilder zkcBuilder)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri, name);
        return new ZKMetadataAccessor(name, conf, uri, zkcBuilder);
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
                        BKDistributedLogManager.createUnpartitionedStream(conf, zkc.get(), uri, s);
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
    public void close() {
        scheduledThreadPoolExecutor.shutdown();
        LOG.info("Executor Service Stopped.");
        if (scheduledThreadPoolExecutor != readAheadExecutor) {
            readAheadExecutor.shutdown();
            LOG.info("ReadAhead Executor Service Stopped.");
        }
        if (null != allocator) {
            allocator.close(false);
            LOG.info("Ledger Allocator stopped.");
        }
        // force closing all bk/zk clients to avoid zookeeper threads not being
        // shutdown due to any reference count issue, which make the factory still
        // holding locks w/o releasing ownerships.
        BookKeeperClient bkc = getBookKeeperClient();
        if (null != bkc) {
            bkc.release(true);
        }
        sharedZKClientForDL.close(true);
        if (null != sharedZKClientForBK) {
            sharedZKClientForBK.close(true);
        }
        channelFactory.releaseExternalResources();
        requestTimer.stop();
    }
}
