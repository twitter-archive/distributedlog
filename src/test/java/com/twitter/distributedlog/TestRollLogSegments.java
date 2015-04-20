package com.twitter.distributedlog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.twitter.distributedlog.feature.CoreFeatureKeys;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.feature.SettableFeature;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.util.DistributedLogAnnotations.FlakyTest;
import com.twitter.util.Await;
import com.twitter.util.Function0;
import com.twitter.util.FutureEventListener;

import static com.google.common.base.Charsets.UTF_8;
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

    @Test(timeout = 60000)
    public void testDisableRollingLogSegments() throws Exception {
        String name = "distrlog-disable-rolling-log-segments";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(40);

        int numEntries = 100;
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();

        SettableFeature disableLogSegmentRolling =
                (SettableFeature) dlm.getFeatureProvider()
                        .getFeature(CoreFeatureKeys.DISABLE_LOGSEGMENT_ROLLING.name().toLowerCase());
        disableLogSegmentRolling.set(true);

        final CountDownLatch latch = new CountDownLatch(numEntries);

        // send requests in parallel
        for (int i = 1; i <= numEntries; i++) {
            final int entryId = i;
            writer.write(DLMTestUtil.getLogRecordInstance(entryId)).addEventListener(new FutureEventListener<DLSN>() {

                @Override
                public void onSuccess(DLSN value) {
                    logger.info("Completed entry {} : {}.", entryId, value);
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable cause) {
                    // nope
                }
            });
        }
        latch.await();

        // make sure all ensure blocks were executed
        writer.closeAndComplete();

        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        assertEquals(1, segments.size());

        dlm.close();
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

        DistributedLogManager dlm = createNewDLM(confLocal, name);
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
        logger.info("lastDLSNs after writes {} {}", lastDLSNs.size(), lastDLSNs);
        logger.info("segments after writes {} {}", segments.size(), segments);
        assertTrue(segments.size() >= 2);
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

    @Test(timeout = 60000)
    public void testUnableToRollLogSegments() throws Exception {
        String name = "distrlog-unable-to-roll-log-segments";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(1);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        long txId = 1L;

        // Create Log Segments
        Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId)));

        FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate,
                FailpointUtils.FailPointActions.FailPointAction_Throw);

        try {
            final CountDownLatch startLatch = new CountDownLatch(1);
            writer.getOrderedFuturePool().apply(new Function0<Void>() {
                                                    @Override
                                                    public Void apply() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
            }
            });

            // If we couldn't open new log segment, we should keep using the old one
            final int numRecords = 10;
            final CountDownLatch latch = new CountDownLatch(numRecords);
            for (int i = 0; i < numRecords; i++) {
                writer.write(DLMTestUtil.getLogRecordInstance(++txId)).addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        logger.info("Completed entry : {}.", value);
                        latch.countDown();
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        logger.error("Failed to write entries : ", cause);
                    }
                });
            }

            // start writes
            startLatch.countDown();
            latch.await();

            writer.close();

            List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
            logger.info("LogSegments: {}", segments);

            assertEquals(1, segments.size());

            long expectedTxID = 1L;
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assertEquals(expectedTxID++, record.getTransactionId());
                assertEquals(record.getTransactionId() - 1, record.getSequenceId());

                record = reader.readNext(false);
            }

            assertEquals(12L, expectedTxID);

            reader.close();

            dlm.close();
        } finally {
            FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate);
        }
    }

    @Test(timeout = 60000)
    public void testRollingLogSegments() throws Exception {
        logger.info("start testRollingLogSegments");
        String name = "distrlog-rolling-logsegments-hightraffic";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(1);

        int numLogSegments = 10;

        DistributedLogManager dlm = createNewDLM(confLocal, name);
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

        writer.close();
        dlm.close();
    }

    private void checkAndWaitWriterReaderPosition(BKPerStreamLogWriter writer, long expectedWriterPosition,
                                                  BKAsyncLogReaderDLSN reader, long expectedReaderPosition,
                                                  LedgerHandle inspector, long expectedLac) throws Exception {
        while (writer.getLedgerHandle().getLastAddConfirmed() < expectedWriterPosition) {
            Thread.sleep(1000);
        }
        assertEquals(expectedWriterPosition, writer.getLedgerHandle().getLastAddConfirmed());
        assertEquals(expectedLac, inspector.readLastConfirmed());
        LedgerReadPosition readPosition = reader.bkLedgerManager.readAheadWorker.nextReadAheadPosition;
        logger.info("ReadAhead moved read position {} : ", readPosition);
        while (readPosition.getEntryId() < expectedReaderPosition) {
            Thread.sleep(1000);
            readPosition = reader.bkLedgerManager.readAheadWorker.nextReadAheadPosition;
            logger.info("ReadAhead moved read position {} : ", readPosition);
        }
        assertEquals(expectedReaderPosition, readPosition.getEntryId());
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testCaughtUpReaderOnLogSegmentRolling() throws Exception {
        String name = "distrlog-caughtup-reader-on-logsegment-rolling";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(4 * 1024 * 1024);
        confLocal.setTraceReadAheadMetadataChanges(true);
        confLocal.setEnsembleSize(1);
        confLocal.setWriteQuorumSize(1);
        confLocal.setAckQuorumSize(1);
        confLocal.setReadLACLongPollTimeout(99999999);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();

        // 1) writer added 5 entries.
        final int numEntries = 5;
        for (int i = 1; i <= numEntries; i++) {
            writer.write(DLMTestUtil.getLogRecordInstance(i));
            writer.setReadyToFlush();
            writer.flushAndSync();
        }

        BKDistributedLogManager readDLM = (BKDistributedLogManager) createNewDLM(confLocal, name);
        final BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) readDLM.getAsyncLogReader(DLSN.InitialDLSN);

        // 2) reader should be able to read 5 entries.
        for (long i = 1; i <= numEntries; i++) {
            LogRecordWithDLSN record = Await.result(reader.readNext());
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(i, record.getTransactionId());
            assertEquals(record.getTransactionId() - 1, record.getSequenceId());
        }

        BKPerStreamLogWriter perStreamWriter = writer.perStreamWriter;
        BookKeeperClient bkc = readDLM.getReaderBKC();
        LedgerHandle readLh = bkc.get().openLedgerNoRecovery(perStreamWriter.getLedgerHandle().getId(),
                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));

        // Writer moved to lac = 9, while reader knows lac = 8 and moving to wait on 9
        checkAndWaitWriterReaderPosition(perStreamWriter, 9, reader, 9, readLh, 8);

        // write 6th record
        writer.write(DLMTestUtil.getLogRecordInstance(numEntries + 1));
        writer.setReadyToFlush();
        // Writer moved to lac = 10, while reader knows lac = 9 and moving to wait on 10
        checkAndWaitWriterReaderPosition(perStreamWriter, 10, reader, 10, readLh, 9);

        // write records without commit to simulate similar failure cases
        writer.write(DLMTestUtil.getLogRecordInstance(numEntries + 2));
        writer.setReadyToFlush();
        // Writer moved to lac = 11, while reader knows lac = 10 and moving to wait on 11
        checkAndWaitWriterReaderPosition(perStreamWriter, 11, reader, 11, readLh, 10);

        while (null == reader.bkLedgerManager.readAheadWorker.getMetadataNotification()) {
            Thread.sleep(1000);
        }
        logger.info("Waiting for long poll getting interrupted with metadata changed");

        // simulate a recovery without closing ledger causing recording wrong last dlsn
        BKLogPartitionWriteHandler writeHandler = writer.getCachedPartitionHandler(conf.getUnpartitionedStreamName());
        writeHandler.completeAndCloseLogSegment(
                writeHandler.inprogressZNodeName(perStreamWriter.getLedgerHandle().getId(), perStreamWriter.getStartTxId(), perStreamWriter.getLedgerSequenceNumber()),
                perStreamWriter.getLedgerSequenceNumber(),
                perStreamWriter.getLedgerHandle().getId(),
                perStreamWriter.getStartTxId(), perStreamWriter.getLastTxId(),
                perStreamWriter.getPositionWithinLogSegment() - 1,
                9,
                0,
                true);

        BKUnPartitionedSyncLogWriter anotherWriter = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        anotherWriter.write(DLMTestUtil.getLogRecordInstance(numEntries + 3));
        anotherWriter.setReadyToFlush();
        anotherWriter.flushAndSync();
        anotherWriter.closeAndComplete();

        for (long i = numEntries + 1; i <= numEntries + 3; i++) {
            LogRecordWithDLSN record = Await.result(reader.readNext());
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(i, record.getTransactionId());
        }

        reader.close();
        readDLM.close();
    }
}
