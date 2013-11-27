package com.twitter.distributedlog;

import java.io.IOException;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestFailureScenarios {
    static final Log LOG = LogFactory.getLog(TestBookKeeperDistributedLogManager.class);

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration().setLockTimeout(10);
    private ZooKeeper zkc;
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numBookies = 3;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", 7000);
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test
    public void testExceptionDuringStartNewSegment() throws Exception {
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-exc-new-segment");
        LogWriter out = bkdlm.startLogSegment(1);
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


        assertNull(zkc.exists(bkdlm.completedLedgerZNode(1, 100), false));
        assertNotNull(zkc.exists(bkdlm.inprogressZNode(1), false));

        bkdlm.recoverIncompleteLogSegments();

        assertNotNull(zkc.exists(bkdlm.completedLedgerZNode(1, 100), false));
        assertNull(zkc.exists(bkdlm.inprogressZNode(1), false));

        FailpointUtils.setFailpoint(
            FailpointUtils.FailPointName.FP_StartLogSegmentAfterInProgressCreate,
            FailpointUtils.FailPointActions.FailPointAction_Throw);

        LogWriter outAborted = null;
        try {
            outAborted = bkdlm.startLogSegment(101);
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

        assertNull(zkc.exists(bkdlm.completedLedgerZNode(101, 101), false));
        assertNotNull(zkc.exists(bkdlm.inprogressZNode(101), false));

        bkdlm.close();
        bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-exc-new-segment");
        bkdlm.recoverIncompleteLogSegments();

        assertNotNull(zkc.exists(bkdlm.completedLedgerZNode(101, 101), false));
        assertNull(zkc.exists(bkdlm.inprogressZNode(101), false));

    }

    @Test
    public void testFailureInComplete() throws Exception {
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-failure-complete-ledger");
        LogWriter out = bkdlm.startLogSegment(1);
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

        bkdlm.completeAndCloseLogSegment(1, 100);

        FailpointUtils.removeFailpoint(
            FailpointUtils.FailPointName.FP_FinalizeLedgerBeforeDelete);

        // both nodes should stay around in the incorrect sequence
        assertNotNull(zkc.exists(bkdlm.inprogressZNode(1), false));
        assertNotNull(zkc.exists(bkdlm.completedLedgerZNode(1, 100), false));

        // Make sure that the completionTime will be different
        Thread.sleep(2);

        bkdlm.recoverIncompleteLogSegments();

        assertNull(zkc.exists(bkdlm.inprogressZNode(1), false));
        assertNotNull(zkc.exists(bkdlm.completedLedgerZNode(1, 100), false));
    }

}
