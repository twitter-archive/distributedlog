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

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.util.DLUtils;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.*;

public class TestZKLogMetadataForWriterUtilFunctions {

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingMaxTxnId() throws Exception {
        String rootPath = "/test-missing-max-txn-id";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null));
        ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingVersion() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1)),
                new Versioned<byte[]>(null, null));
        ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasWrongVersion() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1)),
                new Versioned<byte[]>(ZKLogMetadataForWriter.intToBytes(9999), null));
        ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingLockPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1)),
                new Versioned<byte[]>(ZKLogMetadataForWriter.intToBytes(ZKLogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(null, null));
        ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingReadLockPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1)),
                new Versioned<byte[]>(ZKLogMetadataForWriter.intToBytes(ZKLogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                new Versioned<byte[]>(null, null));
        ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingLogSegmentsPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1)),
                new Versioned<byte[]>(ZKLogMetadataForWriter.intToBytes(ZKLogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                new Versioned<byte[]>(null, null));
        ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingAllocatorPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1)),
                new Versioned<byte[]>(ZKLogMetadataForWriter.intToBytes(ZKLogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                new Versioned<byte[]>(DLUtils.serializeLogSegmentSequenceNumber(1L), new ZkVersion(1)),
                new Versioned<byte[]>(null, null));
        ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, true);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000)
    public void testProcessLogMetadatasNoAllocatorPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        Versioned<byte[]> maxTxnIdData =
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1));
        Versioned<byte[]> logSegmentsData =
                new Versioned<byte[]>(DLUtils.serializeLogSegmentSequenceNumber(1L), new ZkVersion(1));
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                maxTxnIdData,
                new Versioned<byte[]>(ZKLogMetadataForWriter.intToBytes(ZKLogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                logSegmentsData);
        ZKLogMetadataForWriter metadata =
                ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
        assertTrue(maxTxnIdData == metadata.getMaxTxIdData());
        assertTrue(logSegmentsData == metadata.getMaxLSSNData());
        assertNull(metadata.getAllocationData().getValue());
        assertNull(metadata.getAllocationData().getVersion());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000)
    public void testProcessLogMetadatasAllocatorPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        Versioned<byte[]> maxTxnIdData =
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new ZkVersion(1));
        Versioned<byte[]> logSegmentsData =
                new Versioned<byte[]>(DLUtils.serializeLogSegmentSequenceNumber(1L), new ZkVersion(1));
        Versioned<byte[]> allocationData =
                new Versioned<byte[]>(DLUtils.ledgerId2Bytes(1L), new ZkVersion(1));
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                maxTxnIdData,
                new Versioned<byte[]>(ZKLogMetadataForWriter.intToBytes(ZKLogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                new Versioned<byte[]>(new byte[0], new ZkVersion(1)),
                logSegmentsData,
                allocationData);
        ZKLogMetadataForWriter metadata =
                ZKLogMetadataForWriter.processLogMetadatas(uri, logName, logIdentifier, metadatas, true);
        assertTrue(maxTxnIdData == metadata.getMaxTxIdData());
        assertTrue(logSegmentsData == metadata.getMaxLSSNData());
        assertTrue(allocationData == metadata.getAllocationData());
    }
}
