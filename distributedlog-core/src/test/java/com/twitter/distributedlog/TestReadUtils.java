package com.twitter.distributedlog;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Await;
import com.twitter.util.Future;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import static org.junit.Assert.*;

/**
 * Test {@link ReadUtils}
 */
public class TestReadUtils extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestReadUtils.class);

    @Rule
    public TestName runtime = new TestName();

    private Future<Optional<LogRecordWithDLSN>> getLogRecordNotLessThanTxId(
            BKDistributedLogManager bkdlm, int logsegmentIdx, long transactionId) throws Exception {
        List<LogSegmentMetadata> logSegments = bkdlm.getLogSegments();
        final LedgerHandleCache handleCache = LedgerHandleCache.newBuilder()
                .bkc(bkdlm.getWriterBKC())
                .conf(conf)
                .build();
        return ReadUtils.getLogRecordNotLessThanTxId(
                bkdlm.getStreamName(),
                logSegments.get(logsegmentIdx),
                transactionId,
                Executors.newSingleThreadExecutor(),
                handleCache,
                10
        ).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
    }

    private Future<LogRecordWithDLSN> getFirstGreaterThanRecord(BKDistributedLogManager bkdlm, int ledgerNo, DLSN dlsn) throws Exception {
        List<LogSegmentMetadata> ledgerList = bkdlm.getLogSegments();
        final LedgerHandleCache handleCache = LedgerHandleCache.newBuilder()
                .bkc(bkdlm.getWriterBKC())
                .conf(conf)
                .build();
        return ReadUtils.asyncReadFirstUserRecord(
                bkdlm.getStreamName(), ledgerList.get(ledgerNo), 2, 16, new AtomicInteger(0), Executors.newFixedThreadPool(1),
                handleCache, dlsn
        ).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
    }

    private Future<LogRecordWithDLSN> getLastUserRecord(BKDistributedLogManager bkdlm, int ledgerNo) throws Exception {
        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        List<LogSegmentMetadata> ledgerList = readHandler.getLedgerList(false, false, LogSegmentMetadata.COMPARATOR, false);
        final LedgerHandleCache handleCache = LedgerHandleCache.newBuilder()
                .bkc(bkdlm.getWriterBKC())
                .conf(conf)
                .build();
        return ReadUtils.asyncReadLastRecord(
                bkdlm.getStreamName(), ledgerList.get(ledgerNo), false, false, false, 2, 16, new AtomicInteger(0), Executors.newFixedThreadPool(1),
                handleCache
        ).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
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
        Utils.close(out);

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

    @Test(timeout = 60000)
    public void testGetEntriesToSearch() throws Exception {
        assertTrue(ReadUtils.getEntriesToSearch(2L, 1L, 10).isEmpty());
        assertEquals(Lists.newArrayList(1L),
                ReadUtils.getEntriesToSearch(1L, 1L, 10));
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                ReadUtils.getEntriesToSearch(1L, 10L, 10));
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L),
                ReadUtils.getEntriesToSearch(1L, 9L, 10));
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
                ReadUtils.getEntriesToSearch(1L, 8L, 10));
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 11L),
                ReadUtils.getEntriesToSearch(1L, 11L, 10));
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 12L),
                ReadUtils.getEntriesToSearch(1L, 12L, 10));
    }

    @Test(timeout = 60000)
    public void testGetEntriesToSearchByTxnId() throws Exception {
        LogRecordWithDLSN firstRecord =
                DLMTestUtil.getLogRecordWithDLSNInstance(new DLSN(1L, 0L, 0L), 999L);
        LogRecordWithDLSN secondRecord =
                DLMTestUtil.getLogRecordWithDLSNInstance(new DLSN(1L, 10L, 0L), 99L);
        LogRecordWithDLSN thirdRecord =
                DLMTestUtil.getLogRecordWithDLSNInstance(new DLSN(1L, 100L, 0L), 1099L);
        // out-of-order sequence
        assertTrue(ReadUtils.getEntriesToSearch(888L, firstRecord, secondRecord, 10).isEmpty());
        // same transaction id
        assertTrue(ReadUtils.getEntriesToSearch(888L, firstRecord, firstRecord, 10).isEmpty());
        // small nways (nways = 2)
        assertEquals(2, ReadUtils.getEntriesToSearch(888L, firstRecord, thirdRecord, 2).size());
        // small nways with equal transaction id
        assertEquals(3, ReadUtils.getEntriesToSearch(1099L, firstRecord, thirdRecord, 2).size());
        LogRecordWithDLSN record1 =
                DLMTestUtil.getLogRecordWithDLSNInstance(new DLSN(1L, 0L, 0L), 88L);
        LogRecordWithDLSN record2 =
                DLMTestUtil.getLogRecordWithDLSNInstance(new DLSN(1L, 12L, 0L), 888L);
        LogRecordWithDLSN record3 =
                DLMTestUtil.getLogRecordWithDLSNInstance(new DLSN(1L, 12L, 0L), 999L);
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 10L, 11L),
                ReadUtils.getEntriesToSearch(888L, record1, record2, 10));
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 11L),
                ReadUtils.getEntriesToSearch(888L, record1, record3, 10));
    }

    @Test(timeout = 60000)
    public void testGetLogRecordNotLessThanTxIdWithGreaterTxId() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0 /* control recs */, 1, 1 /* txid */);

        Optional<LogRecordWithDLSN> result =
                FutureUtils.result(getLogRecordNotLessThanTxId(bkdlm, 0, 999L));
        assertFalse(result.isPresent());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordNotLessThanTxIdWithLessTxId() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0 /* control recs */, 1, 999L /* txid */);

        Optional<LogRecordWithDLSN> result =
                FutureUtils.result(getLogRecordNotLessThanTxId(bkdlm, 0, 99L));
        assertTrue(result.isPresent());
        assertEquals(999L, result.get().getTransactionId());
        assertEquals(0L, result.get().getDlsn().getEntryId());
        assertEquals(0L, result.get().getDlsn().getSlotId());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordNotLessThanTxIdOnSmallSegment() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0 /* control recs */, 5, 1L /* txid */);

        Optional<LogRecordWithDLSN> result =
                FutureUtils.result(getLogRecordNotLessThanTxId(bkdlm, 0, 3L));
        assertTrue(result.isPresent());
        assertEquals(3L, result.get().getTransactionId());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordNotLessThanTxIdOnLargeSegment() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0 /* control recs */, 100, 1L /* txid */);

        Optional<LogRecordWithDLSN> result =
                FutureUtils.result(getLogRecordNotLessThanTxId(bkdlm, 0, 9L));
        assertTrue(result.isPresent());
        assertEquals(9L, result.get().getTransactionId());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordGreaterThanTxIdOnLargeSegment() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0 /* control recs */, 100, 1L /* txid */, 3L);

        Optional<LogRecordWithDLSN> result =
                FutureUtils.result(getLogRecordNotLessThanTxId(bkdlm, 0, 23L));
        assertTrue(result.isPresent());
        assertEquals(25L, result.get().getTransactionId());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordGreaterThanTxIdOnSameTxId() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        long txid = 1L;
        for (int i = 0; i < 10; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            Await.result(out.write(record));
            txid += 1;
        }
        long txidToSearch = txid;
        for (int i = 0; i < 10; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txidToSearch);
            Await.result(out.write(record));
        }
        for (int i = 0; i < 10; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            Await.result(out.write(record));
            txid += 1;
        }
        Utils.close(out);
        Optional<LogRecordWithDLSN> result =
                FutureUtils.result(getLogRecordNotLessThanTxId(bkdlm, 0, txidToSearch));
        assertTrue(result.isPresent());
        assertEquals(10L, result.get().getDlsn().getEntryId());
    }

}
