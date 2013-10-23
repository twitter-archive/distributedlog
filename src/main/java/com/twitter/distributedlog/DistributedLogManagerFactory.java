package com.twitter.distributedlog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.metadata.BKDLConfig;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
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

public class DistributedLogManagerFactory {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogManagerFactory.class);

    private DistributedLogConfiguration conf;
    private URI namespace;
    private final ZooKeeperClientBuilder zooKeeperClientBuilder;
    private final ZooKeeperClient zooKeeperClient;
    private final BookKeeperClientBuilder bookKeeperClientBuilder;
    private final BookKeeperClient bookKeeperClient;
    private final StatsLogger statsLogger;
    private final ScheduledExecutorService scheduledExecutorService;

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri) throws IOException, IllegalArgumentException {
        this(conf, uri, NullStatsLogger.INSTANCE);
    }

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri,
                                        StatsLogger statsLogger) throws IOException, IllegalArgumentException {
        validateInput(conf, uri);
        this.conf = conf;
        this.namespace = uri;
        this.statsLogger = statsLogger;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(
                conf.getNumWorkerThreads(),
                new ThreadFactoryBuilder().setNameFormat("DLM-" + uri.getPath() + "-executor-%d").build()
        );

        try {
            // Build zookeeper client
            this.zooKeeperClientBuilder = ZooKeeperClientBuilder.newBuilder()
                    .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds()).uri(uri)
                    .buildNew(conf.getSeparateZKClients());
            this.zooKeeperClient = this.zooKeeperClientBuilder.build();
            // Resolve uri to get bk dl config.
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zooKeeperClient, uri);
            // Build bookkeeper client
            this.bookKeeperClientBuilder = BookKeeperClientBuilder.newBuilder()
                    .dlConfig(conf).bkdlConfig(bkdlConfig).name(String.format("%s:shared", namespace))
                    .buildNew(conf.getSeparateBKClients());
            if (conf.getShareZKClientWithBKC()) {
                this.bookKeeperClientBuilder.zkc(zooKeeperClient);
            }
            this.bookKeeperClient = this.bookKeeperClientBuilder.build();
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
        }
    }

    /**
     * Create a DistributedLogManager as <i>nameOfLogStream</i>.
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @return distributedlog manager instance.
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public DistributedLogManager createDistributedLogManager(String nameOfLogStream) throws IOException, IllegalArgumentException {
        return new BKDistributedLogManager(nameOfLogStream, conf, namespace,
                zooKeeperClientBuilder, bookKeeperClientBuilder, scheduledExecutorService, statsLogger);
    }

    public boolean checkIfLogExists(String nameOfLogStream)
        throws IOException, InterruptedException, IllegalArgumentException {
        return DistributedLogManagerFactory.checkIfLogExists(conf, namespace, nameOfLogStream);
    }

    public Collection<String> enumerateAllLogsInNamespace()
        throws IOException, InterruptedException, IllegalArgumentException {
        return DistributedLogManagerFactory.enumerateAllLogsInternal(zooKeeperClient, conf, namespace);
    }

    public Map<String, byte[]> enumerateLogsWithMetadataInNamespace()
        throws IOException, InterruptedException, IllegalArgumentException {
        return DistributedLogManagerFactory.enumerateLogsWithMetadataInternal(zooKeeperClient, conf, namespace);
    }

    public static DistributedLogManager createDistributedLogManager(String name, URI uri) throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, new DistributedLogConfiguration(), uri);
    }

    private static void validateInput(DistributedLogConfiguration conf, URI uri) throws IllegalArgumentException {
        // input validation
        //
        if (null == conf) {
            throw new IllegalArgumentException("Incorrect Configuration");
        }

        if ((null == uri) || (null == uri.getAuthority()) || (null == uri.getPath())) {
            throw new IllegalArgumentException("Incorrect ZK URI");
        }
    }

    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, (ZooKeeperClientBuilder) null, (BookKeeperClientBuilder) null,
                NullStatsLogger.INSTANCE);
    }

    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                                                    ZooKeeperClient zkc, BookKeeperClient bkc)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, ZooKeeperClientBuilder.newBuilder().zkc(zkc),
                BookKeeperClientBuilder.newBuilder().bkc(bkc), NullStatsLogger.INSTANCE);
    }

    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                                                    ZooKeeperClient zkc, BookKeeperClient bkc, StatsLogger statsLogger)
        throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, conf, uri, ZooKeeperClientBuilder.newBuilder().zkc(zkc),
                BookKeeperClientBuilder.newBuilder().bkc(bkc), statsLogger);
    }

    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                ZooKeeperClientBuilder zkcBuilder, BookKeeperClientBuilder bkcBuilder, StatsLogger statsLogger)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri);
        return new BKDistributedLogManager(name, conf, uri, zkcBuilder, bkcBuilder, statsLogger);
    }

    public static boolean checkIfLogExists(DistributedLogConfiguration conf, URI uri, String name)
        throws IOException, InterruptedException, IllegalArgumentException {
        validateInput(conf, uri);
        String logRootPath = uri.getPath() + String.format("/%s", name);
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds()).uri(uri).buildNew();
        try {
            if (null != zkc.get().exists(logRootPath, false)) {
                return true;
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted checkIfLogExists  " + logRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + logRootPath + "entry in zookeeper", ke);
        } finally {
            zkc.close();
        }

        return false;
    }

    public static Collection<String> enumerateAllLogsInNamespace(DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        return enumerateAllLogsInternal(null, conf, uri);
    }

    private static Collection<String> enumerateAllLogsInternal(ZooKeeperClient zkcShared, DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        validateInput(conf, uri);
        String namespaceRootPath = uri.getPath();
        ZooKeeperClient zkc = zkcShared;
        if (null == zkcShared) {
            zkc = ZooKeeperClientBuilder.newBuilder().sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                    .uri(uri).buildNew(false).build();
        }
        try {
            Stat currentStat = zkc.get().exists(namespaceRootPath, false);
            if (currentStat == null) {
                return new LinkedList<String>();
            }
            return zkc.get().getChildren(namespaceRootPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new IOException("Interrupted while deleting " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
            throw new IOException("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
        } finally {
            if (null == zkcShared) {
                zkc.close();
            }
        }
    }

    public static Map<String, byte[]> enumerateLogsWithMetadataInNamespace(DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        return enumerateLogsWithMetadataInternal(null, conf, uri);
    }


    private static Map<String, byte[]> enumerateLogsWithMetadataInternal(ZooKeeperClient zkcShared, DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        validateInput(conf, uri);
        String namespaceRootPath = uri.getPath();
        ZooKeeperClient zkc = zkcShared;
        if (null == zkcShared) {
            zkc = ZooKeeperClientBuilder.newBuilder().sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                    .uri(uri).buildNew(false).build();
        }
        HashMap<String, byte[]> result = new HashMap<String, byte[]>();

        try {
            Stat currentStat = zkc.get().exists(namespaceRootPath, false);
            if (currentStat == null) {
                return result;
            }
            List<String> children = zkc.get().getChildren(namespaceRootPath, false);
            for(String child: children) {
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
        } finally {
            if (null == zkcShared) {
                zkc.close();
            }
        }
        return result;
    }

    /**
     * Close the distributed log manager factory, freeing any resources it may hold.
     */
    public void close() throws IOException {
        scheduledExecutorService.shutdown();
        LOG.info("Executor Service Stopped.");
        try {
            bookKeeperClient.release();
            zooKeeperClient.close();
        } catch (Exception e) {
            LOG.warn("Exception while closing distributed log manager factory", e);
        }
    }
}
