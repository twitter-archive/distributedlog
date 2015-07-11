package com.twitter.distributedlog;

import java.io.File;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDLMTestUtil {
    static final Logger LOG = LoggerFactory.getLogger(TestDLMTestUtil.class);

    @Rule
    public TestName testNames = new TestName();

    @Test(timeout = 60000)
    public void testRunZookeeperOnAnyPort() throws Exception {
        Pair<ZooKeeperServerShim, Integer> serverAndPort1 = null;
        Pair<ZooKeeperServerShim, Integer> serverAndPort2 = null;
        Pair<ZooKeeperServerShim, Integer> serverAndPort3 = null;
        try {
            File zkTmpDir1 = IOUtils.createTempDir("zookeeper1", "distrlog");
            serverAndPort1 = LocalDLMEmulator.runZookeeperOnAnyPort(7000, zkTmpDir1);
            File zkTmpDir2 = IOUtils.createTempDir("zookeeper2", "distrlog");
            serverAndPort2 = LocalDLMEmulator.runZookeeperOnAnyPort(7000, zkTmpDir2);
            File zkTmpDir3 = IOUtils.createTempDir("zookeeper3", "distrlog");
            serverAndPort3 = LocalDLMEmulator.runZookeeperOnAnyPort(7000, zkTmpDir3);
        } catch (Exception ex) {
            if (null != serverAndPort1) {
                serverAndPort1.getLeft().stop();
            }
            if (null != serverAndPort2) {
                serverAndPort2.getLeft().stop();
            }
            if (null != serverAndPort3) {
                serverAndPort3.getLeft().stop();
            }
        }
    }
}
