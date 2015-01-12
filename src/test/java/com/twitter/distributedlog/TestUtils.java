package com.twitter.distributedlog;

import com.google.common.base.Optional;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

/**
 * Test Utils
 */
public class TestUtils extends ZooKeeperClusterTestCase {

    private final static int sessionTimeoutMs = 30000;

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI("/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .zkAclId(null)
                .build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testZkAsyncCreateFulPathOptimisticRecursive() throws Exception {
        String path1 = "/a/b/c/d";
        Optional<String> parentPathShouldNotCreate = Optional.absent();
        final CountDownLatch doneLatch1 = new CountDownLatch(1);
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zkc, path1, parentPathShouldNotCreate,
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        doneLatch1.countDown();
                    }
                }, null);
        doneLatch1.await();
        assertNotNull(zkc.get().exists(path1, false));

        String path2 = "/a/b/c/d/e/f/g";
        parentPathShouldNotCreate = Optional.of("/a/b/c/d/e");
        final CountDownLatch doneLatch2 = new CountDownLatch(1);
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zkc, path2, parentPathShouldNotCreate,
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        doneLatch2.countDown();
                    }
                }, null);
        doneLatch2.await();
        assertNull(zkc.get().exists("/a/b/c/d/e", false));
        assertNull(zkc.get().exists("/a/b/c/d/e/f", false));
        assertNull(zkc.get().exists("/a/b/c/d/e/f/g", false));

        parentPathShouldNotCreate = Optional.of("/a/b");
        final CountDownLatch doneLatch3 = new CountDownLatch(1);
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zkc, path2, parentPathShouldNotCreate,
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        doneLatch3.countDown();
                    }
                }, null);
        doneLatch3.await();
        assertNotNull(zkc.get().exists(path2, false));
    }
}
