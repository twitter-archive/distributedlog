package com.twitter.distributedlog;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestDistributedLogBase {

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration().setLockTimeout(10);
    protected ZooKeeper zkc;
    protected static LocalDLMEmulator bkutil;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;
    static int numBookies = 3;
    protected static final List<File> tmpDirs = new ArrayList<File>();

    @BeforeClass
    public static void setupCluster() throws Exception {
        File zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
        tmpDirs.add(zkTmpDir);
        zks = LocalBookKeeper.runZookeeper(1000, 7000, zkTmpDir);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
        zkServers = "127.0.0.1:7000";
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
        zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", 7000);
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

}
