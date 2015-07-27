package com.twitter.distributedlog;

import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.PermitLimiter;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogBase.class);

    // Num worker threads should be one, since the exec service is used for the ordered
    // future pool in test cases, and setting to > 1 will therefore result in unordered
    // write ops.
    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration()
                .setLockTimeout(1)
                .setNumWorkerThreads(1)
                .setSchedulerShutdownTimeoutMs(0)
                .setDLLedgerMetadataLayoutVersion(LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    protected ZooKeeper zkc;
    protected static LocalDLMEmulator bkutil;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;
    protected static int zkPort;
    protected static int numBookies = 3;
    protected static final List<File> tmpDirs = new ArrayList<File>();
    protected static final int MAX_RETRIES = 10;

    @BeforeClass
    public static void setupCluster() throws Exception {
        boolean success = false;
        int retries = 0;
        File zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
        tmpDirs.add(zkTmpDir);
        Pair<ZooKeeperServerShim, Integer> serverAndPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkTmpDir);
        zks = serverAndPort.getLeft();
        zkPort = serverAndPort.getRight();
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", zkPort, DLMTestUtil.loadTestBkConf());
        bkutil.start();
        zkServers = "127.0.0.1:" + zkPort;
        success = true;
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
        for (File dir : tmpDirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    @Before
    public void setup() throws Exception {
        try {
            zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", zkPort);
        } catch (Exception ex) {
            LOG.error("hit exception connecting to zookeeper at {}:{}", new Object[] { "127.0.0.1", zkPort, ex });
            throw ex;
        }
    }

    @After
    public void teardown() throws Exception {
        if (null != zkc) {
            zkc.close();
        }
    }

    public URI createDLMURI(String path) throws Exception {
        return DLMTestUtil.createDLMURI(zkPort, path);
    }

    public DistributedLogManager createNewDLM(DistributedLogConfiguration conf,
                                              String name) throws Exception {
        URI uri = createDLMURI("/" + name);
        return new BKDistributedLogManager(
                name,
                conf,
                uri,
                null,
                null,
                null,
                null,
                null,
                null,
                new SettableFeatureProvider("", 0),
                PermitLimiter.NULL_PERMIT_LIMITER,
                NullStatsLogger.INSTANCE
        );
    }

    public DistributedLogManager createNewDLM(DistributedLogConfiguration conf,
                                              String name,
                                              PermitLimiter writeLimiter)
            throws Exception {
        URI uri = createDLMURI("/" + name);
        return new BKDistributedLogManager(
                name,
                conf,
                uri,
                null,
                null,
                null,
                null,
                null,
                null,
                new SettableFeatureProvider("", 0),
                writeLimiter,
                NullStatsLogger.INSTANCE
        );
    }

    public DLMTestUtil.BKLogPartitionWriteHandlerAndClients createNewBKDLM(
            DistributedLogConfiguration conf,
            String path) throws Exception {
        return DLMTestUtil.createNewBKDLM(new PartitionId(0), conf, path, zkPort);
    }

    protected ZooKeeperClient getZooKeeperClient(DistributedLogManagerFactory factory) throws Exception {
        DistributedLogNamespace namespace = factory.getNamespace();
        assert(namespace instanceof BKDistributedLogNamespace);
        return ((BKDistributedLogNamespace) namespace).getSharedWriterZKCForDL();
    }

    protected BookKeeperClient getBookKeeperClient(DistributedLogManagerFactory factory) throws Exception {
        DistributedLogNamespace namespace = factory.getNamespace();
        assert(namespace instanceof BKDistributedLogNamespace);
        return ((BKDistributedLogNamespace) namespace).getReaderBKC();
    }
}
