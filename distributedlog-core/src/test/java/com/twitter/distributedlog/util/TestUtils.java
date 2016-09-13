/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.util;

import com.google.common.base.Optional;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.TestZooKeeperClientBuilder;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClusterTestCase;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test Utils
 */
public class TestUtils extends ZooKeeperClusterTestCase {

    private final static int sessionTimeoutMs = 30000;

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI(zkPort, "/"))
                .sessionTimeoutMs(sessionTimeoutMs)
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

    @Test(timeout = 60000)
    public void testZkGetData() throws Exception {
        String path1 = "/zk-get-data/non-existent-path";
        Versioned<byte[]> data = FutureUtils.result(Utils.zkGetData(zkc.get(), path1, false));
        assertNull("No data should return from non-existent-path", data.getValue());
        assertNull("No version should return from non-existent-path", data.getVersion());

        String path2 = "/zk-get-data/path2";
        byte[] rawData = "test-data".getBytes(UTF_8);
        FutureUtils.result(Utils.zkAsyncCreateFullPathOptimistic(zkc, path2, rawData,
                zkc.getDefaultACL(), CreateMode.PERSISTENT));
        data = FutureUtils.result(Utils.zkGetData(zkc.get(), path2, false));
        assertArrayEquals("Data should return as written",
                rawData, data.getValue());
        assertEquals("Version should be zero",
                0, ((ZkVersion) data.getVersion()).getZnodeVersion());
    }
}
