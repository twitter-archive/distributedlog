package com.twitter.distributedlog;

import com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * {@link BKLogReadHandler.ReadAheadWorker} related test cases.
 */
public class TestReadAhead extends TestDistributedLogBase {

    static final Logger logger = LoggerFactory.getLogger(TestReadAhead.class);

    @Test(timeout = 60000)
    public void testNoSuchLedgerExceptionOnReadLAC() throws Exception {
        String name = "distrlog-nosuchledger-exception-on-readlac";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadWaitTime(500);
        confLocal.setReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis(2000);
        confLocal.setDLLedgerMetadataLayoutVersion(LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, confLocal, 1L, 1L, false, 0, false);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, confLocal, 2L, 11L, true, 10, true);

        BKDistributedLogManager readDLM = (BKDistributedLogManager) createNewDLM(confLocal, name);
        final BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) readDLM.getAsyncLogReader(DLSN.InitialDLSN);
        final Future<LogRecordWithDLSN> readFuture = reader.readNext();
        try {
            Await.result(readFuture, Duration.fromMilliseconds(2000));
            fail("Should not read any data beyond an empty inprogress log segment");
        } catch (TimeoutException e) {
            // expected
        }

        LedgerDescriptor ld1;
        while (null == (ld1 = reader.bkLedgerManager.readAheadWorker.currentLH)) {
            Thread.sleep(100);
        }

        TimeUnit.MILLISECONDS.sleep(2 * 2000);

        LedgerDescriptor ld2;
        while (null == (ld2 = reader.bkLedgerManager.readAheadWorker.currentLH)) {
            Thread.sleep(100);
        }

        // ledger handle would be re-initialized after reaching error threshold
        assertTrue("ledger handle should be reinitialized, after reaching error threshold.", ld1 != ld2);

        dlm.close();

        dlm = createNewDLM(confLocal, name);
        dlm.recover();

        long expectedTxId = 11L;
        LogRecord record = Await.result(readFuture);
        assertNotNull(record);
        DLMTestUtil.verifyLogRecord(record);
        assertEquals(expectedTxId, record.getTransactionId());
        expectedTxId++;

        for (int i = 1; i < 10; i++) {
            record = Await.result(reader.readNext());
            assertNotNull(record);
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
        }

        reader.close();
        readDLM.close();

    }

    @Test(timeout = 60000)
    public void testReadAheadWaitOnEndOfStream() throws Exception {
        String name = "distrlog-readahead-wait-on-end-of-stream";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setZKNumRetries(0);
        confLocal.setReadAheadWaitTime(500);
        confLocal.setReadAheadWaitTimeOnEndOfStream(Integer.MAX_VALUE);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        DLMTestUtil.generateCompletedLogSegments(dlm, confLocal, 3, 10);

        BKDistributedLogManager readDLM = (BKDistributedLogManager) createNewDLM(confLocal, name);
        final BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) readDLM.getAsyncLogReader(DLSN.InitialDLSN);

        int numReads = 0;
        long expectedID = 1;
        for (long i = 0; i < 3; i++) {
            for (long j = 1; j <= 10; j++) {
                LogRecordWithDLSN record = Await.result(reader.readNext());
                assertEquals(expectedID++, record.getTransactionId());
                DLMTestUtil.verifyLogRecord(record);
                ++numReads;
            }
        }
        assertEquals(30, numReads);
        // we are at the end of the stream and there isn't inprogress log segment
        Future<LogRecordWithDLSN> readFuture = reader.readNext();

        // make sure readahead is backing off on reading log segment on Integer.MAX_VALUE
        AsyncNotification notification1;
        while (null == (notification1 = reader.bkLedgerManager.readAheadWorker.getMetadataNotification())) {
            Thread.sleep(200);
        }
        Thread.sleep(1000);

        // Expire the session, so the readahead should be awaken from backoff
        ZooKeeperClientUtils.expireSession(reader.bkLedgerManager.zooKeeperClient, zkServers, 1000);
        AsyncNotification notification2;
        do {
            Thread.sleep(200);
            notification2 = reader.bkLedgerManager.readAheadWorker.getMetadataNotification();
        } while (null == notification2 || notification1 == notification2);

        // write another record
        BKSyncLogWriter writer =
                    (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(31L));
        writer.closeAndComplete();

        LogRecordWithDLSN record = Await.result(readFuture);
        assertEquals(31L, record.getTransactionId());
        DLMTestUtil.verifyLogRecord(record);

        reader.close();
        readDLM.close();

        dlm.close();
    }

}
