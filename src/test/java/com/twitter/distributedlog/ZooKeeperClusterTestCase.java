package com.twitter.distributedlog;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

public class ZooKeeperClusterTestCase {

    protected static File zkDir;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;
    protected static int zkPort;

    @BeforeClass
    public static void setupZooKeeper() throws Exception {
        zkDir = IOUtils.createTempDir("zookeeper", ZooKeeperClusterTestCase.class.getName());
        Pair<ZooKeeperServerShim, Integer> serverAndPort = DLMTestUtil.runZookeeperOnAnyPort(zkDir);
        zks = serverAndPort.getLeft();
        zkPort = serverAndPort.getRight();
        zkServers = "127.0.0.1:" + zkPort;
    }

    @AfterClass
    public static void shutdownZooKeeper() throws Exception {
        zks.stop();
        if (null != zkDir) {
            FileUtils.deleteDirectory(zkDir);
        }
    }
}
