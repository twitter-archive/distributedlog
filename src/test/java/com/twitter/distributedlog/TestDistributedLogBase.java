package com.twitter.distributedlog;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestDistributedLogBase {

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration().setLockTimeout(10);
    protected ZooKeeper zkc;
    protected static LocalDLMEmulator bkutil;
    protected static ZooKeeperServerShim zks;
    static int numBookies = 3;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
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
