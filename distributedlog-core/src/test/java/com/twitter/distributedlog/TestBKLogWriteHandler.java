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
