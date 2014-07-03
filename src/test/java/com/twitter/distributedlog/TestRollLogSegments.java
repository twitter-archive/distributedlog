package com.twitter.distributedlog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.util.FutureEventListener;

import static org.junit.Assert.*;

public class TestRollLogSegments extends TestDistributedLogBase {
    static final Logger logger = LoggerFactory.getLogger(TestRollLogSegments.class);

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
    public void testLastDLSNInRollingLogSegments() throws Exception {
        final Map<Long, DLSN> lastDLSNs = new HashMap<Long, DLSN>();
        String name = "distrlog-lastdlsn-in-rolling-log-segments";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(40);

        int numEntries = 100;

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        final CountDownLatch latch = new CountDownLatch(numEntries);

        // send requests in parallel to have outstanding requests
        for (int i = 1; i <= numEntries; i++) {
            final int entryId = i;
            writer.write(DLMTestUtil.getLogRecordInstance(entryId)).addEventListener(new FutureEventListener<DLSN>() {

                @Override
                public void onSuccess(DLSN value) {
                    logger.info("Completed entry {} : {}.", entryId, value);
                    synchronized (lastDLSNs) {
                        DLSN lastDLSN = lastDLSNs.get(value.getLedgerSequenceNo());
                        if (null == lastDLSN || lastDLSN.compareTo(value) < 0) {
                            lastDLSNs.put(value.getLedgerSequenceNo(), value);
                        }
                    }
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable cause) {

                }
            });
        }
        latch.await();

        // make sure all ensure blocks were executed.
        writer.closeAndComplete();

        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        assertTrue(lastDLSNs.size() >= 2);
        assertEquals(lastDLSNs.size(), segments.size());
        for (LogSegmentLedgerMetadata segment : segments) {
            DLSN dlsnInMetadata = segment.getLastDLSN();
            DLSN dlsnSeen = lastDLSNs.get(segment.getLedgerSequenceNumber());
            assertNotNull(dlsnInMetadata);
            assertNotNull(dlsnSeen);
            if (dlsnInMetadata.compareTo(dlsnSeen) != 0) {
                logger.error("Last dlsn recorded in log segment {} is different from the one already seen {}.",
                             dlsnInMetadata, dlsnSeen);
            }
            assertEquals(0, dlsnInMetadata.compareTo(dlsnSeen));
        }

        dlm.close();
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
