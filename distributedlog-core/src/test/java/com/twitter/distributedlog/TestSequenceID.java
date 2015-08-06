package com.twitter.distributedlog;

import com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion;
import com.twitter.util.Await;
import com.twitter.util.FutureEventListener;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

/**
 * Test Cases related to sequence ids.
 */
public class TestSequenceID extends TestDistributedLogBase {

    static final Logger logger = LoggerFactory.getLogger(TestSequenceID.class);

    @Test(timeout = 60000)
    public void testCompleteV4LogSegmentAsV4() throws Exception {
        completeSingleInprogressSegment(LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value,
                                        LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value);
    }

    @Test(timeout = 60000)
    public void testCompleteV4LogSegmentAsV5() throws Exception {
        completeSingleInprogressSegment(LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value,
                                        LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value);
    }

    @Test(timeout = 60000)
    public void testCompleteV5LogSegmentAsV4() throws Exception {
        completeSingleInprogressSegment(LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value,
                                        LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value);
    }

    @Test(timeout = 60000)
    public void testCompleteV5LogSegmentAsV5() throws Exception {
        completeSingleInprogressSegment(LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value,
                                        LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value);
    }

    private void completeSingleInprogressSegment(int writeVersion, int completeVersion) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setDLLedgerMetadataLayoutVersion(writeVersion);

        String name = "distrlog-complete-single-inprogress-segment-versions-write-"
                + writeVersion + "-complete-" + completeVersion;

        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        Await.result(writer.write(DLMTestUtil.getLogRecordInstance(0L)));

        dlm.close();

        DistributedLogConfiguration confLocal2 = new DistributedLogConfiguration();
        confLocal2.addConfiguration(confLocal);
        confLocal2.setDLLedgerMetadataLayoutVersion(completeVersion);

        BKDistributedLogManager dlm2 = (BKDistributedLogManager) createNewDLM(confLocal2, name);
        dlm2.startAsyncLogSegmentNonPartitioned();

        List<LogSegmentMetadata> segments = dlm2.getLogSegments();
        assertEquals(1, segments.size());

        if (LogSegmentMetadata.supportsSequenceId(writeVersion)) {
            if (LogSegmentMetadata.supportsSequenceId(completeVersion)) {
                // the inprogress log segment is written in v5 and complete log segment in v5,
                // then it support monotonic sequence id
                assertEquals(0L, segments.get(0).getStartSequenceId());
            } else {
                // the inprogress log segment is written in v5 and complete log segment in v4,
                // then it doesn't support monotonic sequence id
                assertTrue(segments.get(0).getStartSequenceId() < 0);
            }
        } else {
            // if the inprogress log segment is created prior to v5, it won't support monotonic sequence id
            assertTrue(segments.get(0).getStartSequenceId() < 0);
        }

        dlm2.close();
    }

    @Test(timeout = 60000)
    public void testSequenceID() throws Exception {
        DistributedLogConfiguration confLocalv4 = new DistributedLogConfiguration();
        confLocalv4.addConfiguration(conf);
        confLocalv4.setImmediateFlushEnabled(true);
        confLocalv4.setOutputBufferSize(0);
        confLocalv4.setDLLedgerMetadataLayoutVersion(LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value);

        String name = "distrlog-sequence-id";

        BKDistributedLogManager readDLM = (BKDistributedLogManager) createNewDLM(conf, name);
        AsyncLogReader reader = null;
        final LinkedBlockingQueue<LogRecordWithDLSN> readRecords =
                new LinkedBlockingQueue<LogRecordWithDLSN>();

        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(confLocalv4, name);

        long txId = 0L;

        for (int i = 0; i < 3; i++) {
            BKUnPartitionedAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 0; j < 2; j++) {
                Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));

                if (null == reader) {
                    reader = readDLM.getAsyncLogReader(DLSN.InitialDLSN);
                    final AsyncLogReader r = reader;
                    reader.readNext().addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                        @Override
                        public void onSuccess(LogRecordWithDLSN record) {
                            readRecords.add(record);
                            r.readNext().addEventListener(this);
                        }

                        @Override
                        public void onFailure(Throwable cause) {
                            logger.error("Encountered exception on reading next : ", cause);
                        }
                    });
                }
            }
            writer.closeAndComplete();
        }

        BKUnPartitionedAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(4, segments.size());
        for (int i = 0; i < 3; i++) {
            assertFalse(segments.get(i).isInProgress());
            assertTrue(segments.get(i).getStartSequenceId() < 0);
        }
        assertTrue(segments.get(3).isInProgress());
        assertTrue(segments.get(3).getStartSequenceId() < 0);

        dlm.close();

        // simulate upgrading from v4 -> v5

        DistributedLogConfiguration confLocalv5 = new DistributedLogConfiguration();
        confLocalv5.addConfiguration(conf);
        confLocalv5.setImmediateFlushEnabled(true);
        confLocalv5.setOutputBufferSize(0);
        confLocalv5.setDLLedgerMetadataLayoutVersion(LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value);

        BKDistributedLogManager dlmv5 = (BKDistributedLogManager) createNewDLM(confLocalv5, name);
        for (int i = 0; i < 3; i++) {
            BKUnPartitionedAsyncLogWriter writerv5 = dlmv5.startAsyncLogSegmentNonPartitioned();
            for (int j = 0; j < 2; j++) {
                Await.result(writerv5.write(DLMTestUtil.getLogRecordInstance(txId++)));
            }
            writerv5.closeAndComplete();
        }
        BKUnPartitionedAsyncLogWriter writerv5 = dlmv5.startAsyncLogSegmentNonPartitioned();
        Await.result(writerv5.write(DLMTestUtil.getLogRecordInstance(txId++)));

        List<LogSegmentMetadata> segmentsv5 = dlmv5.getLogSegments();
        assertEquals(8, segmentsv5.size());

        assertFalse(segmentsv5.get(3).isInProgress());
        assertTrue(segmentsv5.get(3).getStartSequenceId() < 0);

        long startSequenceId = 0L;
        for (int i = 4; i < 7; i++) {
            assertFalse(segmentsv5.get(i).isInProgress());
            assertEquals(startSequenceId, segmentsv5.get(i).getStartSequenceId());
            startSequenceId += 2L;
        }

        assertTrue(segmentsv5.get(7).isInProgress());
        assertEquals(startSequenceId, segmentsv5.get(7).getStartSequenceId());

        dlmv5.close();

        // rollback from v5 to v4

        BKDistributedLogManager dlmv4 = (BKDistributedLogManager) createNewDLM(confLocalv4, name);
        for (int i = 0; i < 3; i++) {
            BKUnPartitionedAsyncLogWriter writerv4 = dlmv4.startAsyncLogSegmentNonPartitioned();
            for (int j = 0; j < 2; j++) {
                Await.result(writerv4.write(DLMTestUtil.getLogRecordInstance(txId++)));
            }
            writerv4.closeAndComplete();
        }

        List<LogSegmentMetadata> segmentsv4 = dlmv4.getLogSegments();
        assertEquals(11, segmentsv4.size());

        for(int i = 7; i < 11; i++) {
            assertFalse(segmentsv4.get(i).isInProgress());
            assertTrue(segmentsv4.get(i).getStartSequenceId() < 0);
        }

        dlmv4.close();

        // wait until readers read all records
        while (readRecords.size() < txId) {
            Thread.sleep(100);
        }

        assertEquals(txId, readRecords.size());
        long sequenceId = Long.MIN_VALUE;
        for (LogRecordWithDLSN record : readRecords) {
            if (record.getDlsn().getLedgerSequenceNo() <= 4) {
                assertTrue(record.getSequenceId() < 0);
                assertTrue(record.getSequenceId() > sequenceId);
                sequenceId = record.getSequenceId();
            } else if (record.getDlsn().getLedgerSequenceNo() <= 7) {
                if (sequenceId < 0L) {
                    sequenceId = 0L;
                }
                assertEquals(sequenceId, record.getSequenceId());
                ++sequenceId;
            } else if (record.getDlsn().getLedgerSequenceNo() >= 9) {
                if (sequenceId > 0) {
                    sequenceId = Long.MIN_VALUE;
                }
                assertTrue(record.getSequenceId() < 0);
                assertTrue(record.getSequenceId() > sequenceId);
                sequenceId = record.getSequenceId();
            }
        }

        readDLM.close();
    }

}
