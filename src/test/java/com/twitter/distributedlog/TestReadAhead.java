package com.twitter.distributedlog;

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
 * {@link com.twitter.distributedlog.BKLogPartitionReadHandler.ReadAheadWorker} related test cases.
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

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, conf, 1L, 1L, false, 0, false);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, conf, 2L, 11L, true, 10, true);

        BKDistributedLogManager readDLM = (BKDistributedLogManager) DLMTestUtil.createNewDLM(confLocal, name);
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

        dlm = DLMTestUtil.createNewDLM(confLocal, name);
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
}
