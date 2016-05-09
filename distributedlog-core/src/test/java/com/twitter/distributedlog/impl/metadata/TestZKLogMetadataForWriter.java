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
package com.twitter.distributedlog.impl.metadata;

import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.ZooKeeperClusterTestCase;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.util.List;

import static com.twitter.distributedlog.impl.metadata.ZKLogMetadata.*;
import static org.junit.Assert.*;

/**
 * Test {@link ZKLogMetadataForWriter}
 */
public class TestZKLogMetadataForWriter extends ZooKeeperClusterTestCase {

    private final static int sessionTimeoutMs = 30000;

    @Rule
    public TestName testName = new TestName();

    private ZooKeeperClient zkc;

    private static void createLog(ZooKeeperClient zk, URI uri, String logName, String logIdentifier)
            throws Exception {
        final String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        final String logSegmentsPath = logRootPath + LOGSEGMENTS_PATH;
        final String maxTxIdPath = logRootPath + MAX_TXID_PATH;
        final String lockPath = logRootPath + LOCK_PATH;
        final String readLockPath = logRootPath + READ_LOCK_PATH;
        final String versionPath = logRootPath + VERSION_PATH;
        final String allocationPath = logRootPath + ALLOCATION_PATH;

        Utils.zkCreateFullPathOptimistic(zk, logRootPath, new byte[0],
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        Transaction txn = zk.get().transaction();
        txn.create(logSegmentsPath, DLUtils.serializeLogSegmentSequenceNumber(
                        DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(maxTxIdPath, DLUtils.serializeTransactionId(0L),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(lockPath, DistributedLogConstants.EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(readLockPath, DistributedLogConstants.EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(versionPath, ZKLogMetadataForWriter.intToBytes(LAYOUT_VERSION),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(allocationPath, DistributedLogConstants.EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.commit();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI(zkPort, "/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .zkAclId(null)
                .build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testCheckLogMetadataPathsWithAllocator() throws Exception {
        String logRootPath = "/" + testName.getMethodName();
        List<Versioned<byte[]>> metadatas =
                FutureUtils.result(ZKLogMetadataForWriter.checkLogMetadataPaths(
                        zkc.get(), logRootPath, true));
        assertEquals("Should have 8 paths",
                8, metadatas.size());
        for (Versioned<byte[]> path : metadatas.subList(2, metadatas.size())) {
            assertNull(path.getValue());
            assertNull(path.getVersion());
        }
    }

    @Test(timeout = 60000)
    public void testCheckLogMetadataPathsWithoutAllocator() throws Exception {
        String logRootPath = "/" + testName.getMethodName();
        List<Versioned<byte[]>> metadatas =
                FutureUtils.result(ZKLogMetadataForWriter.checkLogMetadataPaths(
                        zkc.get(), logRootPath, false));
        assertEquals("Should have 7 paths",
                7, metadatas.size());
        for (Versioned<byte[]> path : metadatas.subList(2, metadatas.size())) {
            assertNull(path.getValue());
            assertNull(path.getVersion());
        }
    }

    private void testCreateLogMetadataWithMissingPaths(URI uri,
                                                       String logName,
                                                       String logIdentifier,
                                                       List<String> pathsToDelete,
                                                       boolean ownAllocator,
                                                       boolean createLogFirst)
            throws Exception {
        if (createLogFirst) {
            createLog(zkc, uri, logName, logIdentifier);
        }
        // delete a path
        for (String path : pathsToDelete) {
            zkc.get().delete(path, -1);
        }

        ZKLogMetadataForWriter logMetadata =
                FutureUtils.result(ZKLogMetadataForWriter.of(uri, logName, logIdentifier,
                        zkc.get(), zkc.getDefaultACL(), ownAllocator, true));

        final String logRootPath = getLogRootPath(uri, logName, logIdentifier);

        List<Versioned<byte[]>> metadatas =
                FutureUtils.result(ZKLogMetadataForWriter.checkLogMetadataPaths(zkc.get(), logRootPath, ownAllocator));

        if (ownAllocator) {
            assertEquals("Should have 8 paths : ownAllocator = " + ownAllocator,
                    8, metadatas.size());
        } else {
            assertEquals("Should have 7 paths : ownAllocator = " + ownAllocator,
                    7, metadatas.size());
        }

        for (Versioned<byte[]> metadata : metadatas) {
            assertTrue(ZKLogMetadataForWriter.pathExists(metadata));
            assertTrue(((ZkVersion) metadata.getVersion()).getZnodeVersion() >= 0);
        }

        Versioned<byte[]> logSegmentsData = logMetadata.getMaxLSSNData();

        assertEquals(DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO,
                DLUtils.deserializeLogSegmentSequenceNumber(logSegmentsData.getValue()));

        Versioned<byte[]> maxTxIdData = logMetadata.getMaxTxIdData();

        assertEquals(0L, DLUtils.deserializeTransactionId(maxTxIdData.getValue()));

        if (ownAllocator) {
            Versioned<byte[]> allocationData = logMetadata.getAllocationData();
            assertEquals(0, allocationData.getValue().length);
        }
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingLogSegmentsPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + LOGSEGMENTS_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingMaxTxIdPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + MAX_TXID_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingLockPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + LOCK_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingReadLockPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + READ_LOCK_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingVersionPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + VERSION_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingAllocatorPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + ALLOCATION_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingAllPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + LOGSEGMENTS_PATH,
                logRootPath + MAX_TXID_PATH,
                logRootPath + LOCK_PATH,
                logRootPath + READ_LOCK_PATH,
                logRootPath + VERSION_PATH,
                logRootPath + ALLOCATION_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataOnExistedLog() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        List<String> pathsToDelete = Lists.newArrayList();
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadata() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        List<String> pathsToDelete = Lists.newArrayList();

        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, false);
    }

    @Test(timeout = 60000, expected = LogNotFoundException.class)
    public void testCreateLogMetadataWithCreateIfNotExistsSetToFalse() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        FutureUtils.result(ZKLogMetadataForWriter.of(uri, logName, logIdentifier,
                        zkc.get(), zkc.getDefaultACL(), true, false));
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataWithCustomMetadata() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        List<String> pathsToDelete = Lists.newArrayList();

        DLMetadata.create(new BKDLConfig(zkServers, "/ledgers")).update(uri);

        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
            .conf(new DistributedLogConfiguration())
            .uri(uri)
            .build();

        DistributedLogManager dlm = namespace.openLog(logName);
        dlm.createOrUpdateMetadata(logName.getBytes("UTF-8"));
        dlm.close();

        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, false);
    }

}
