package com.twitter.distributedlog;

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestFailureScenarios extends TestDistributedLogBase {

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration().setLockTimeout(10).setLogSegmentNameVersion(0);

    @Test
    public void testExceptionDuringStartNewSegment() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = DLMTestUtil.createNewBKDLM(conf, "distrlog-exc-new-segment");
        LogWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                out.setReadyToFlush();
                out.flushAndSync();
            }

        }
        out.setReadyToFlush();
        out.flushAndSync();

        out.abort();
        out.close();


        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(-1, 1, 100, 1), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(-1, 1, 1), false));

        bkdlmAndClients.getWriteHandler().recoverIncompleteLogSegments();

        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(-1, 1, 100, 1), false));
        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(-1, 1, 1), false));

        FailpointUtils.setFailpoint(
            FailpointUtils.FailPointName.FP_StartLogSegmentAfterInProgressCreate,
            FailpointUtils.FailPointActions.FailPointAction_Throw);

        LogWriter outAborted = null;
        try {
            outAborted = bkdlmAndClients.getWriteHandler().startLogSegment(101);
            for (long i = 1; i <= 100; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                outAborted.write(op);
                if ((i % 10) == 0) {
                    outAborted.setReadyToFlush();
                    outAborted.flushAndSync();
                }

            }
        } catch (IOException exc) {
            // Expected
        } finally {
            if (null != outAborted) {
                outAborted.abort();
                outAborted.close();
            }
        }

        FailpointUtils.removeFailpoint(
            FailpointUtils.FailPointName.FP_StartLogSegmentAfterInProgressCreate);

        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(-1, 101, 101, 2), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(-1, 101, 2), false));

        bkdlmAndClients.close();
        bkdlmAndClients = DLMTestUtil.createNewBKDLM(conf, "distrlog-exc-new-segment");
        bkdlmAndClients.getWriteHandler().recoverIncompleteLogSegments();

        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(-1, 101, 101, 2), false));
        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(-1, 101, 2), false));
        bkdlmAndClients.close();
    }

    @Test
    public void testFailureInComplete() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = DLMTestUtil.createNewBKDLM(conf, "distrlog-failure-complete-ledger");
        BKPerStreamLogWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                out.setReadyToFlush();
                out.flushAndSync();
            }

        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.close();

        FailpointUtils.setFailpoint(
            FailpointUtils.FailPointName.FP_FinalizeLedgerBeforeDelete,
            FailpointUtils.FailPointActions.FailPointAction_Default
        );

        bkdlmAndClients.getWriteHandler().completeAndCloseLogSegment(out.getLedgerSequenceNumber(),
                out.getLedgerHandle().getId(), 1, 100, 100);

        FailpointUtils.removeFailpoint(
            FailpointUtils.FailPointName.FP_FinalizeLedgerBeforeDelete);

        // both nodes should stay around in the incorrect sequence
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(out.getLedgerHandle().getId(),
                1, out.getLedgerSequenceNumber()), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(out.getLedgerHandle().getId(),
                1, 100, out.getLedgerSequenceNumber()), false));

        // Make sure that the completionTime will be different
        Thread.sleep(2);

        bkdlmAndClients.getWriteHandler().recoverIncompleteLogSegments();

        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(out.getLedgerHandle().getId(), 1,
                out.getLedgerSequenceNumber()), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(out.getLedgerHandle().getId(), 1, 100,
                out.getLedgerSequenceNumber()), false));
    }

}
