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
package com.twitter.distributedlog;

import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.LedgerAllocatorPool;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * Test {@link BKLogWriteHandler}
 */
public class TestBKLogWriteHandler extends TestDistributedLogBase {

    @Rule
    public TestName runtime = new TestName();

    /**
     * Testcase: when write handler encounters exceptions on starting log segment
     * it should abort the transaction and return the ledger to the pool.
     */
    @Test(timeout = 60000)
    public void testAbortTransactionOnStartLogSegment() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        ensureURICreated(zkc, uri);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setEnableLedgerAllocatorPool(true);
        confLocal.setLedgerAllocatorPoolCoreSize(1);
        confLocal.setLedgerAllocatorPoolName("test-allocator-pool");

        BKDistributedLogNamespace namespace = (BKDistributedLogNamespace)
                DistributedLogNamespaceBuilder.newBuilder()
                        .conf(confLocal)
                        .uri(uri)
                        .build();
        DistributedLogManager dlm = namespace.openLog("test-stream");
        FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_StartLogSegmentOnAssignLogSegmentSequenceNumber,
                FailpointUtils.FailPointActions.FailPointAction_Throw);
        try {
            AsyncLogWriter writer =  FutureUtils.result(dlm.openAsyncLogWriter());
            FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
            fail("Should fail opening the writer");
        } catch (IOException ioe) {
            // expected
        } finally {
            FailpointUtils.removeFailpoint(
                    FailpointUtils.FailPointName.FP_StartLogSegmentOnAssignLogSegmentSequenceNumber);
        }

        LedgerAllocator allocator = namespace.getLedgerAllocator();
        assertTrue(allocator instanceof LedgerAllocatorPool);
        LedgerAllocatorPool allocatorPool = (LedgerAllocatorPool) allocator;
        assertEquals(0, allocatorPool.obtainMapSize());

        AsyncLogWriter writer = FutureUtils.result(dlm.openAsyncLogWriter());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        Utils.close(writer);
    }

}
