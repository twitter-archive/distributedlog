package com.twitter.distributedlog;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

public class ZooKeeperClusterTestCase {

    protected static File zkDir;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;

    @BeforeClass
    public static void setupZooKeeper() throws Exception {
        zkDir = IOUtils.createTempDir("zookeeper", ZooKeeperClusterTestCase.class.getName());
        zks = LocalBookKeeper.runZookeeper(1000, 7000, zkDir);
        zkServers = "127.0.0.1:7000";
    }

    @AfterClass
    public static void shutdownZooKeeper() throws Exception {
        zks.stop();
        if (null != zkDir) {
            FileUtils.deleteDirectory(zkDir);
        }
    }
}
