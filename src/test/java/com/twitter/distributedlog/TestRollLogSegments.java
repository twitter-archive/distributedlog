package com.twitter.distributedlog;

import com.twitter.util.FutureEventListener;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

public class TestRollLogSegments {
    static final Logger logger = LoggerFactory.getLogger(TestRollLogSegments.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10);
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numberBookies = 3;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numberBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    private static void ensureOnlyOneInprogressLogSegments(List<LogSegmentLedgerMetadata> segments) throws Exception {
        int numInprogress = 0;
        for (LogSegmentLedgerMetadata segment : segments) {
            if (segment.isInProgress()) {
                ++numInprogress;
            }
        }
        assertEquals(1, numInprogress);
    }

    @Test(timeout = 600000)
    public void testRollingLogSegments() throws Exception {
        String name = "distrlog-rolling-logsegments-hightraffic";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(1);

        int numLogSegments = 10;

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        final CountDownLatch latch = new CountDownLatch(numLogSegments);
        long startTime = System.currentTimeMillis();
        // send requests in parallel to have outstanding requests
        for (int i = 1; i <= numLogSegments; i++) {
            final int entryId = i;
            writer.write(DLMTestUtil.getLogRecordInstance(entryId)).addEventListener(new FutureEventListener<DLSN>() {
                @Override
                public void onSuccess(DLSN value) {
                    logger.info("Completed entry {} : {}.", entryId, value);
                    latch.countDown();
                }
                @Override
                public void onFailure(Throwable cause) {
                    logger.error("Failed to write entries : {}", cause);
                }
            });
        }
        latch.await();

        logger.info("Took {} ms to completed all requests.", System.currentTimeMillis() - startTime);

        // make sure all ensure blocks were executed.
        writer.nop().get();

        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        logger.info("LogSegments : {}", segments);

        assertTrue(segments.size() >= 2);
        ensureOnlyOneInprogressLogSegments(segments);

        int numSegmentsAfterAsyncWrites = segments.size();

        // writer should work after rolling log segments
        // there would be (numLogSegments/2) segments based on current rolling policy
        for (int i = 1; i <= numLogSegments; i++) {
            DLSN newDLSN = writer.write(DLMTestUtil.getLogRecordInstance(numLogSegments + i)).get();
            logger.info("Completed entry {} : {}", numLogSegments + i, newDLSN);
        }

        // make sure all ensure blocks were executed.
        writer.nop().get();

        segments = dlm.getLogSegments();
        logger.info("LogSegments : {}", segments);

        assertEquals(numSegmentsAfterAsyncWrites + numLogSegments / 2, segments.size());
        ensureOnlyOneInprogressLogSegments(segments);
    }
}
