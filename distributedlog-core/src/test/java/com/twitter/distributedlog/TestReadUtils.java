package com.twitter.distributedlog;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.util.Await;
import com.twitter.util.Future;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test {@link ReadUtils}
 */
public class TestReadUtils extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestReadUtils.class);

    @Rule
    public TestName runtime = new TestName();

    private Future<LogRecordWithDLSN> getFirstGreaterThanRecord(BKDistributedLogManager bkdlm, int ledgerNo, DLSN dlsn) throws Exception {
        BKLogReadHandler readHandler = bkdlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        List<LogSegmentMetadata> ledgerList = readHandler.getLedgerList(false, false, LogSegmentMetadata.COMPARATOR, false);
        return ReadUtils.asyncReadFirstUserRecord(
            bkdlm.getStreamName(), ledgerList.get(ledgerNo), 2, 16, new AtomicInteger(0), Executors.newFixedThreadPool(1),
            bkdlm.getWriterBKC(), conf.getBKDigestPW(), dlsn);
    }

    private Future<LogRecordWithDLSN> getLastUserRecord(BKDistributedLogManager bkdlm, int ledgerNo) throws Exception {
        BKLogReadHandler readHandler = bkdlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        List<LogSegmentMetadata> ledgerList = readHandler.getLedgerList(false, false, LogSegmentMetadata.COMPARATOR, false);
        return ReadUtils.asyncReadLastRecord(
            bkdlm.getStreamName(), ledgerList.get(ledgerNo), false, false, false, 2, 16, new AtomicInteger(0), Executors.newFixedThreadPool(1),
            bkdlm.getWriterBKC(), conf.getBKDigestPW());
    }

    @Test(timeout = 60000)
    public void testForwardScanFirstRecord() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, 1 /* txid */);

        DLSN dlsn = new DLSN(1,0,0);
        Future<LogRecordWithDLSN> futureLogrec = getFirstGreaterThanRecord(bkdlm, 0, dlsn);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals("should be an exact match", dlsn, logrec.getDlsn());
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testForwardScanNotFirstRecord() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, 1 /* txid */);

        DLSN dlsn = new DLSN(1,1,0);
        Future<LogRecordWithDLSN> futureLogrec = getFirstGreaterThanRecord(bkdlm, 0, dlsn);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals("should be an exact match", dlsn, logrec.getDlsn());
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testForwardScanValidButNonExistentRecord() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, 1 /* txid */);

        DLSN dlsn = new DLSN(1,0,1);
        Future<LogRecordWithDLSN> futureLogrec = getFirstGreaterThanRecord(bkdlm, 0, dlsn);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals(new DLSN(1,1,0), logrec.getDlsn());
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testForwardScanForRecordAfterLedger() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5 /* user recs */ , 1 /* txid */);

        DLSN dlsn = new DLSN(2,0,0);
        Future<LogRecordWithDLSN> futureLogrec = getFirstGreaterThanRecord(bkdlm, 0, dlsn);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals(null, logrec);
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testForwardScanForRecordBeforeLedger() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5 /* user recs */ , txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5 /* user recs */ , txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5 /* user recs */ , txid);

        DLSN dlsn = new DLSN(1,3,0);
        Future<LogRecordWithDLSN> futureLogrec = getFirstGreaterThanRecord(bkdlm, 1, dlsn);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals(new DLSN(2,0,0), logrec.getDlsn());
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testForwardScanControlRecord() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 5 /* control recs */, 5, 1 /* txid */);

        DLSN dlsn = new DLSN(1,3,0);
        Future<LogRecordWithDLSN> futureLogrec = getFirstGreaterThanRecord(bkdlm, 0, dlsn);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals(new DLSN(1,5,0), logrec.getDlsn());
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testGetLastRecordUserRecord() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 5 /* control recs */, 5, 1 /* txid */);

        Future<LogRecordWithDLSN> futureLogrec = getLastUserRecord(bkdlm, 0);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals(new DLSN(1,9,0), logrec.getDlsn());
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testGetLastRecordControlRecord() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);

        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        int txid = 1;
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, true)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, true)));
        out.close();

        Future<LogRecordWithDLSN> futureLogrec = getLastUserRecord(bkdlm, 0);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals(new DLSN(1,2,0), logrec.getDlsn());
        bkdlm.close();
    }

    @Test(timeout = 60000)
    public void testGetLastRecordAllControlRecords() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 5 /* control recs */, 0, 1 /* txid */);

        Future<LogRecordWithDLSN> futureLogrec = getLastUserRecord(bkdlm, 0);
        LogRecordWithDLSN logrec = Await.result(futureLogrec);
        assertEquals(null, logrec);
        bkdlm.close();
    }
}
