package com.twitter.distributedlog;

import com.twitter.util.Future;
import com.twitter.util.Await;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.twitter.util.Await;
import com.twitter.util.Future;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.Rule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test {@link BKLogPartitionReadHandler}
 */
public class TestBKLogPartitionReadHandler extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestBKLogPartitionReadHandler.class);

    @Rule
    public TestName runtime = new TestName();

    private void prepareLogSegments(String name, int numSegments, int numEntriesPerSegment) throws Exception {
        BKLogPartitionWriteHandler writer = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (int sid = 0; sid < numSegments; ++sid) {
            BKPerStreamLogWriter out = writer.startLogSegment(txid);
            for (int eid = 0; eid < numEntriesPerSegment; ++eid) {
                LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
                out.write(record);
                ++txid;
            }
            out.close();
            writer.completeAndCloseLogSegment(out.getLedgerSequenceNumber(), out.getLedgerHandle().getId(),
                    1 + sid * numEntriesPerSegment, (sid + 1) * numEntriesPerSegment, numEntriesPerSegment);
        }
        writer.close();
    }

    private void prepareLogSegmentsNonPartitioned(String name, int numSegments, int numEntriesPerSegment) throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        long txid = 1;
        for (int sid = 0; sid < numSegments; ++sid) {
            LogWriter out = dlm.startLogSegmentNonPartitioned();
            for (int eid = 0; eid < numEntriesPerSegment; ++eid) {
                LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
                out.write(record);
                ++txid;
            }
            out.close();
        }
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testGetLedgerList() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegments(dlName, 3, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> ledgerList = readHandler.getLedgerList(false, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        List<LogSegmentLedgerMetadata> ledgerList2 = readHandler.getFilteredLedgerList(true, false);
        List<LogSegmentLedgerMetadata> ledgerList3 = readHandler.getLedgerList(false, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        assertEquals(3, ledgerList.size());
        assertEquals(3, ledgerList2.size());
        assertEquals(3, ledgerList3.size());
        for (int i=0; i<3; i++) {
            assertEquals(ledgerList3.get(i), ledgerList2.get(i));
        }
    }

    @Test(timeout = 60000)
    public void testForceGetLedgerList() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegments(dlName, 3, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> ledgerList = readHandler.getLedgerList(true, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        final AtomicReference<List<LogSegmentLedgerMetadata>> resultHolder =
                new AtomicReference<List<LogSegmentLedgerMetadata>>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        readHandler.asyncGetLedgerList(LogSegmentLedgerMetadata.COMPARATOR, null, new BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>>() {
            @Override
            public void operationComplete(int rc, List<LogSegmentLedgerMetadata> result) {
                resultHolder.set(result);
                latch.countDown();
            }
        });
        latch.await();
        List<LogSegmentLedgerMetadata> newLedgerList = resultHolder.get();
        assertNotNull(newLedgerList);
        LOG.info("Force sync get list : {}", ledgerList);
        LOG.info("Async get list : {}", newLedgerList);
        assertEquals(3, ledgerList.size());
        assertEquals(3, newLedgerList.size());
        for (int i=0; i<3; i++) {
            assertEquals(ledgerList.get(i), newLedgerList.get(i));
        }
    }

    @Test(timeout = 60000)
    public void testGetFilteredLedgerListInWriteHandler() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegments(dlName, 11, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);

        // Get full list.
        BKLogPartitionWriteHandler writeHandler0 = ((BKDistributedLogManager) dlm).createWriteLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> cachedFullLedgerList =
                writeHandler0.getCachedFullLedgerList(LogSegmentLedgerMetadata.DESC_COMPARATOR);
        assertTrue(cachedFullLedgerList.size() <= 1);
        List<LogSegmentLedgerMetadata> fullLedgerList = writeHandler0.getFullLedgerListDesc(false, false);
        assertEquals(11, fullLedgerList.size());

        // Get filtered list.
        BKLogPartitionWriteHandler writeHandler1 = ((BKDistributedLogManager) dlm).createWriteLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> filteredLedgerListDesc = writeHandler1.getFilteredLedgerListDesc(false, false);
        assertEquals(1, filteredLedgerListDesc.size());
        assertEquals(fullLedgerList.get(0), filteredLedgerListDesc.get(0));
        List<LogSegmentLedgerMetadata> filteredLedgerList = writeHandler1.getFilteredLedgerList(false, false);
        assertEquals(1, filteredLedgerList.size());
        assertEquals(fullLedgerList.get(0), filteredLedgerList.get(0));
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNNoLogSegments() throws Exception {
        String dlName = runtime.getMethodName();
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(new PartitionId(0));
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        try {
            Await.result(futureRecord);
            fail("should have thrown exception");
        } catch (LogNotFoundException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNWithLogSegments() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegments(dlName, 3, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(new PartitionId(0));
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        try {
            LogRecordWithDLSN record = Await.result(futureRecord);
            assertEquals(new DLSN(1, 0, 0), record.getDlsn());
        } catch (Exception ex) {
            fail("should not have thrown exception: " + ex);
        }
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNAfterCleanTruncation() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 3, 10);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler =
            ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        Future<Boolean> futureSuccess = writer.truncate(new DLSN(2, 0, 0));
        Boolean success = Await.result(futureSuccess);
        assertTrue(success);
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        LogRecordWithDLSN record = Await.result(futureRecord);
        assertEquals(new DLSN(2, 0, 0), record.getDlsn());
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNAfterPartialTruncation() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 3, 10);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler =
            ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();

        // Only truncates at ledger boundary.
        Future<Boolean> futureSuccess = writer.truncate(new DLSN(2, 5, 0));
        Boolean success = Await.result(futureSuccess);
        assertTrue(success);
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        LogRecordWithDLSN record = Await.result(futureRecord);
        assertEquals(new DLSN(2, 0, 0), record.getDlsn());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountEmptyLedger() throws Exception {
        String dlName = runtime.getMethodName();
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(DLSN.InitialDLSN);
        try {
            Await.result(count);
            fail("log is empty, should have returned log empty ex");
        } catch (LogNotFoundException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountTotalCount() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(DLSN.InitialDLSN);
        assertEquals(33, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountAtLedgerBoundary() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(2, 0, 0));
        assertEquals(30, Await.result(count).longValue());
        count = readHandler.asyncGetLogRecordCount(new DLSN(3, 0, 0));
        assertEquals(27, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountPastEnd() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(12, 0, 0));
        assertEquals(0, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountLastRecord() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(11, 2, 0));
        assertEquals(1, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountInteriorRecords() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 5, 10);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(3, 5, 0));
        assertEquals(25, Await.result(count).longValue());
        count = readHandler.asyncGetLogRecordCount(new DLSN(2, 5, 0));
        assertEquals(35, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithControlRecords() throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, runtime.getMethodName());
        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 5, 5, txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 0, 10, txid);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(15, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithAllControlRecords() throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, runtime.getMethodName());
        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 5, 0, txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 10, 0, txid);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(conf.getUnpartitionedStreamName());
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(0, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithSingleInProgressLedger() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(conf, streamName);

        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        int txid = 1;

        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));

        BKLogPartitionReadHandler readHandler = bkdlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        List<LogSegmentLedgerMetadata> ledgerList = readHandler.getLedgerList(false, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        assertEquals(1, ledgerList.size());
        assertTrue(ledgerList.get(0).isInProgress());

        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(2, Await.result(count).longValue());

        out.close();
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithCompletedAndInprogressLedgers() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(conf, streamName);

        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, txid);
        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));

        BKLogPartitionReadHandler readHandler = bkdlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        List<LogSegmentLedgerMetadata> ledgerList = readHandler.getLedgerList(false, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        assertEquals(2, ledgerList.size());
        assertFalse(ledgerList.get(0).isInProgress());
        assertTrue(ledgerList.get(1).isInProgress());

        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(7, Await.result(count).longValue());

        out.close();
    }

    @Test(timeout = 60000)
    public void testLockStreamWithMissingLog() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(conf, streamName);
        BKLogPartitionReadHandler readHandler = bkdlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        try {
            readHandler.lockStream();
        } catch (LogNotFoundException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testLockStream() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, 1);
        BKLogPartitionReadHandler readHandler = bkdlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        readHandler.lockStream();
    }
}
