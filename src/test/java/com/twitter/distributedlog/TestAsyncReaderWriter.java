package com.twitter.distributedlog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.IdleReaderException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.exceptions.ReadCancelledException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.util.DistributedLogAnnotations.FlakyTest;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import junit.framework.Assert;
import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestAsyncReaderWriter extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestAsyncReaderWriter.class);

    protected DistributedLogConfiguration testConf;

    public TestAsyncReaderWriter() {
        this.testConf = new DistributedLogConfiguration();
        this.testConf.loadConf(conf);
        this.testConf.setReaderIdleErrorThresholdMillis(1200000);
    }

    @Rule
    public TestName runtime = new TestName();

    @Test
    public void testWriteControlRecord() throws Exception {
        String name = "distrlog-writecontrolrecord";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            DLSN dlsn = writer.writeControlRecord(new LogRecord(txid++, "control".getBytes(UTF_8))).get();
            assertEquals(currentLedgerSeqNo, dlsn.getLedgerSequenceNo());
            assertEquals(0, dlsn.getEntryId());
            assertEquals(0, dlsn.getSlotId());
            for (long j = 1; j < 10; j++) {
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                writer.write(record).get();
            }
            writer.closeAndComplete();
        }
        dlm.close();

        DistributedLogManager readDlm = createNewDLM(confLocal, name);
        LogReader reader = readDlm.getInputStream(1);

        long numTrans = 0;
        long expectedTxId = 2;
        LogRecord record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLargeLogRecord(record);
            numTrans++;
            assertEquals(expectedTxId, record.getTransactionId());
            if (expectedTxId % 10 == 0) {
                expectedTxId += 2;
            } else {
                ++expectedTxId;
            }
            record = reader.readNext(false);
        }
        assertEquals(3 * 9, numTrans);
        assertEquals(3 * 9, readDlm.getLogRecordCount());
        readDlm.close();
    }

    public <T> void validateFutureFailed(Future<T> future, Class exClass) {
        try {
            Await.result(future, Duration.fromSeconds(10));
        } catch (Exception ex) {
            LOG.info("Expected: {} Actual: {}", exClass.getName(), ex.getClass().getName());
            assertTrue("exceptions types equal", exClass.isInstance(ex));
        }
    }

    public <T> T validateFutureSucceededAndGetResult(Future<T> future) throws Exception {
        try {
            return Await.result(future, Duration.fromSeconds(10));
        } catch (Exception ex) {
            fail("unexpected exception " + ex.getClass().getName());
            throw ex;
        }
    }

    @Test
    public void testAsyncBulkWritePartialFailureBufferFailure() throws Exception {
        String name = "distrlog-testAsyncBulkWritePartialFailure";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        final int goodRecs = 10;
        final List<LogRecord> records = DLMTestUtil.getLargeLogRecordInstanceList(1, goodRecs);
        records.add(DLMTestUtil.getLogRecordInstance(goodRecs, DistributedLogConstants.MAX_LOGRECORD_SIZE + 1));
        records.addAll(DLMTestUtil.getLargeLogRecordInstanceList(1, goodRecs));
        Future<List<Future<DLSN>>> futureResults = writer.writeBulk(records);
        List<Future<DLSN>> results = validateFutureSucceededAndGetResult(futureResults);

        // One future returned for each write.
        assertEquals(2*goodRecs + 1, results.size());

        // First goodRecs are good.
        for (int i = 0; i < goodRecs; i++) {
            DLSN dlsn = validateFutureSucceededAndGetResult(results.get(i));
        }

        // First failure is log rec too big.
        validateFutureFailed(results.get(goodRecs), LogRecordTooLongException.class);

        // Rest are WriteCancelledException.
        for (int i = goodRecs+1; i < 2*goodRecs+1; i++) {
            validateFutureFailed(results.get(i), WriteCancelledException.class);
        }

        writer.closeAndComplete();
        dlm.close();
    }

    @Test
    public void testAsyncBulkWriteTotalFailureTransmitFailure() throws Exception {
        String name = "distrlog-testAsyncBulkWriteTotalFailureDueToTransmitFailure";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        final int batchSize = 100;

        // First entry.
        long ledgerIndex = 1;
        long entryIndex = 0;
        long slotIndex = 0;

        FailpointUtils.setFailpoint(
            FailpointUtils.FailPointName.FP_TransmitComplete,
            FailpointUtils.FailPointActions.FailPointAction_Default
        );

        try {
            // Since we don't hit MAX_TRANSMISSION_SIZE, the failure is triggered on final flush, which
            // will enqueue cancel promises task to the ordered future pool.
            checkAllSubmittedButFailed(writer, batchSize, 1024, 1);
        } finally {
            FailpointUtils.removeFailpoint(
                FailpointUtils.FailPointName.FP_TransmitComplete
            );
        }

        writer.abort();
        dlm.close();
    }

    @Test
    public void testAsyncBulkWriteNoLedgerRollWithPartialFailures() throws Exception {
        String name = "distrlog-testAsyncBulkWriteNoLedgerRollWithPartialFailures";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        confLocal.setMaxLogSegmentBytes(1024);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // Write one record larger than max seg size. Ledger doesn't roll until next write.
        int txid = 1;
        LogRecord record = DLMTestUtil.getLogRecordInstance(txid++, 2048);
        Future<DLSN> result = writer.write(record);
        DLSN dlsn = validateFutureSucceededAndGetResult(result);
        assertEquals(1, dlsn.getLedgerSequenceNo());

        // Write two more via bulk. Ledger doesn't roll because there's a partial failure.
        List<LogRecord> records = null;
        Future<List<Future<DLSN>>> futureResults = null;
        List<Future<DLSN>> results = null;
        records = new ArrayList<LogRecord>(2);
        records.add(DLMTestUtil.getLogRecordInstance(txid++, 2048));
        records.add(DLMTestUtil.getLogRecordInstance(txid++, DistributedLogConstants.MAX_LOGRECORD_SIZE + 1));
        futureResults = writer.writeBulk(records);
        results = validateFutureSucceededAndGetResult(futureResults);
        result = results.get(0);
        dlsn = validateFutureSucceededAndGetResult(result);
        assertEquals(1, dlsn.getLedgerSequenceNo());

        // Now writer is in a bad state.
        records = new ArrayList<LogRecord>(1);
        records.add(DLMTestUtil.getLogRecordInstance(txid++, 2048));
        futureResults = writer.writeBulk(records);
        validateFutureFailed(futureResults, WriteException.class);

        writer.closeAndComplete();
        dlm.close();
    }

    @Test
    public void testAsyncWritePendingWritesAbortedWhenLedgerRollTriggerFails() throws Exception {
        String name = "distrlog-testAsyncWritePendingWritesAbortedWhenLedgerRollTriggerFails";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        confLocal.setMaxLogSegmentBytes(1024);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // Write one record larger than max seg size. Ledger doesn't roll until next write.
        int txid = 1;
        LogRecord record = DLMTestUtil.getLogRecordInstance(txid++, 2048);
        Future<DLSN> result = writer.write(record);
        DLSN dlsn = Await.result(result, Duration.fromSeconds(10));
        assertEquals(1, dlsn.getLedgerSequenceNo());

        record = DLMTestUtil.getLogRecordInstance(txid++, DistributedLogConstants.MAX_LOGRECORD_SIZE + 1);
        result = writer.write(record);
        validateFutureFailed(result, LogRecordTooLongException.class);

        record = DLMTestUtil.getLogRecordInstance(txid++, DistributedLogConstants.MAX_LOGRECORD_SIZE + 1);
        result = writer.write(record);
        validateFutureFailed(result, WriteException.class);

        record = DLMTestUtil.getLogRecordInstance(txid++, DistributedLogConstants.MAX_LOGRECORD_SIZE + 1);
        validateFutureFailed(result, WriteException.class);

        writer.closeAndComplete();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncBulkWriteSpanningEntryAndLedger() throws Exception {
        String name = "distrlog-testSimpleAsyncBulkWriteSpanningEntryAndLedger";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        int batchSize = 100;
        int recSize = 1024;

        // First entry.
        long ledgerIndex = 1;
        long entryIndex = 0;
        long slotIndex = 0;
        long txIndex = 1;
        checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);

        // New entry.
        entryIndex++;
        slotIndex = 0;
        txIndex += batchSize;
        checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);

        // Roll ledger.
        ledgerIndex++;
        entryIndex = 0;
        slotIndex = 0;
        txIndex += batchSize;
        writer.closeAndComplete();
        writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);

        writer.closeAndComplete();
        dlm.close();
    }

    @Test
    public void testAsyncBulkWriteSpanningPackets() throws Exception {
        String name = "distrlog-testAsyncBulkWriteSpanningPackets";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // First entry.
        int numTransmissions = 4;
        int recSize = 10*1024;
        int batchSize = (numTransmissions*DistributedLogConstants.MAX_TRANSMISSION_SIZE+1)/recSize;
        long ledgerIndex = 1;
        long entryIndex = 0;
        long slotIndex = 0;
        long txIndex = 1;
        DLSN dlsn = checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);
        assertEquals(4, dlsn.getEntryId());
        assertEquals(1, dlsn.getLedgerSequenceNo());

        writer.closeAndComplete();
        dlm.close();
    }

    @Test
    public void testAsyncBulkWriteSpanningPacketsWithTransmitFailure() throws Exception {
        String name = "distrlog-testAsyncBulkWriteSpanningPacketsWithTransmitFailure";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // First entry.
        int numTransmissions = 4;
        int recSize = 10*1024;
        int batchSize = (numTransmissions*DistributedLogConstants.MAX_TRANSMISSION_SIZE+1)/recSize;
        long ledgerIndex = 1;
        long entryIndex = 0;
        long slotIndex = 0;
        long txIndex = 1;

        DLSN dlsn = checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);
        assertEquals(4, dlsn.getEntryId());
        assertEquals(1, dlsn.getLedgerSequenceNo());

        FailpointUtils.setFailpoint(
            FailpointUtils.FailPointName.FP_TransmitComplete,
            FailpointUtils.FailPointActions.FailPointAction_Default
        );

        try {
            checkAllSubmittedButFailed(writer, batchSize, recSize, 1);
        } finally {
            FailpointUtils.removeFailpoint(
                FailpointUtils.FailPointName.FP_TransmitComplete
            );
        }
        writer.abort();
        dlm.close();
    }

    private DLSN checkAllSucceeded(BKUnPartitionedAsyncLogWriter writer,
                                   int batchSize,
                                   int recSize,
                                   long ledgerIndex,
                                   long entryIndex,
                                   long slotIndex,
                                   long txIndex) throws Exception {

        List<LogRecord> records = DLMTestUtil.getLogRecordInstanceList(txIndex, batchSize, recSize);
        Future<List<Future<DLSN>>> futureResults = writer.writeBulk(records);
        assertNotNull(futureResults);
        List<Future<DLSN>> results = Await.result(futureResults, Duration.fromSeconds(10));
        assertNotNull(results);
        assertEquals(results.size(), records.size());
        long prevEntryId = 0;
        DLSN lastDlsn = null;
        for (Future<DLSN> result : results) {
            DLSN dlsn = Await.result(result, Duration.fromSeconds(10));
            lastDlsn = dlsn;

            // If we cross a transmission boundary, slot id gets reset.
            if (dlsn.getEntryId() > prevEntryId) {
                slotIndex = 0;
            }
            assertEquals(ledgerIndex, dlsn.getLedgerSequenceNo());
            assertEquals(slotIndex, dlsn.getSlotId());
            slotIndex++;
            prevEntryId = dlsn.getEntryId();
        }
        return lastDlsn;
    }

    private void checkAllSubmittedButFailed(BKUnPartitionedAsyncLogWriter writer,
                                            int batchSize,
                                            int recSize,
                                            long txIndex) throws Exception {

        List<LogRecord> records = DLMTestUtil.getLogRecordInstanceList(txIndex, batchSize, recSize);
        Future<List<Future<DLSN>>> futureResults = writer.writeBulk(records);
        assertNotNull(futureResults);
        List<Future<DLSN>> results = Await.result(futureResults, Duration.fromSeconds(10));
        assertNotNull(results);
        assertEquals(results.size(), records.size());
        for (Future<DLSN> result : results) {
            validateFutureFailed(result, IOException.class);
        }
    }

    @Test
    public void testSimpleAsyncWrite() throws Exception {
        String name = "distrlog-simpleasyncwrite";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();
        final CountDownLatch syncLatch = new CountDownLatch(30);
        final AtomicReference<DLSN> maxDLSN = new AtomicReference<DLSN>(DLSN.InvalidDLSN);
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                            LOG.debug("EntryId: " + value.getLedgerSequenceNo() + ", TxId " + currentLedgerSeqNo);
                            currentThread.interrupt();
                        }

                        if(value.getEntryId() != currentEntryId) {
                            LOG.debug("EntryId: " + value.getEntryId() + ", TxId " + record.getTransactionId() + "Expected " + currentEntryId);
                            currentThread.interrupt();
                        }

                        if (value.compareTo(maxDLSN.get()) > 0) {
                            maxDLSN.set(value);
                        }

                        syncLatch.countDown();
                        LOG.debug("SyncLatch: " + syncLatch.getCount());
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        currentThread.interrupt();
                    }
                });
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);

        LogRecordWithDLSN last = dlm.getLastLogRecord();
        assertEquals(last.getDlsn(), maxDLSN.get());
        assertEquals(last.getDlsn(), dlm.getLastDLSN());
        assertEquals(last.getDlsn(), dlm.getLastDLSNAsync().get());
        DLMTestUtil.verifyLargeLogRecord(last);

        dlm.close();
    }

    private static void readNext(final Thread threadToInterrupt,
                                 final CountDownLatch syncLatch,
                                 final AsyncLogReader reader,
                                 final DLSN startPosition) {
        Future<LogRecordWithDLSN> record = null;
        try {
            record = reader.readNext();
        } catch (Exception exc) {
            LOG.debug("Encountered Exception");
            threadToInterrupt.interrupt();
        }
        if (null != record) {
            record.addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                @Override
                public void onSuccess(LogRecordWithDLSN value) {
                    assert(value.getDlsn().compareTo(startPosition) >= 0);
                    try {
                        LOG.debug("DLSN: " + value.getDlsn());
                        assert(!value.isControl());
                        assert(value.getDlsn().getSlotId() == 0);
                        assert(value.getDlsn().compareTo(startPosition) >= 0);
                        DLMTestUtil.verifyLargeLogRecord(value);
                    } catch (Exception exc) {
                        LOG.debug("Exception Encountered when verifying log records" + value.getDlsn(), exc);
                        threadToInterrupt.interrupt();
                    }
                    syncLatch.countDown();
                    LOG.debug("SyncLatch: " + syncLatch.getCount());
                    TestAsyncReaderWriter.readNext(threadToInterrupt, syncLatch, reader, value.getDlsn().getNextDLSN());
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.debug("Encountered Exception");
                    threadToInterrupt.interrupt();
                }
            });
        }
    }

    private static void readNextWithRetry(final Thread threadToInterrupt,
                                          final CountDownLatch syncLatch,
                                          final DistributedLogManager dlm,
                                          final AsyncLogReader reader,
                                          final boolean simulateErrors,
                                          final DLSN startPosition,
                                          final ScheduledThreadPoolExecutor executorService,
                                          final int delay,
                                          final AtomicInteger readCount,
                                          final AtomicInteger executionCount) {
        Future<LogRecordWithDLSN> record = null;
        try {
            record = reader.readNext();
        } catch (Exception exc) {
            LOG.debug("Encountered Exception");
            threadToInterrupt.interrupt();
        }
        if (null != record) {
            record.addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                @Override
                public void onSuccess(LogRecordWithDLSN value) {
                    assert(value.getDlsn().compareTo(startPosition) >= 0);
                    try {
                        LOG.debug("DLSN: " + value.getDlsn());
                        assert(!value.isControl());
                        assert(value.getDlsn().getSlotId() == 0);
                        assert(value.getDlsn().compareTo(startPosition) >= 0);
                        DLMTestUtil.verifyLargeLogRecord(value);
                    } catch (Exception exc) {
                        LOG.debug("Exception Encountered when verifying log records" + value.getDlsn(), exc);
                        threadToInterrupt.interrupt();
                    }
                    synchronized (syncLatch) {
                        syncLatch.countDown();
                        readCount.incrementAndGet();
                    }
                    LOG.debug("SyncLatch: " + syncLatch.getCount());
                    if(0 == syncLatch.getCount()) {
                        try {
                            reader.close();
                        } catch (Exception exc) {
                            //
                        }
                    } else {
                        TestAsyncReaderWriter.readNextWithRetry(threadToInterrupt, syncLatch,
                            dlm, reader, simulateErrors,
                            value.getDlsn().getNextDLSN(), executorService, delay, readCount, executionCount);
                    }
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.debug("Encountered Exception", cause);
                    try {
                        reader.close();
                    } catch (Exception exc) {
                        //
                    }
                    if (cause instanceof IOException) {
                        int newDelay = Math.min(delay * 2, 500);
                        if (0 == delay) {
                            newDelay = 10;
                        }
                        positionReader(threadToInterrupt, syncLatch, dlm, simulateErrors,
                            startPosition, executorService, newDelay, readCount, executionCount);
                    }
                }
            });
        }
    }

    private static void positionReader(final Thread threadToInterrupt,
                                       final CountDownLatch syncLatch,
                                       final DistributedLogManager dlm,
                                       final boolean simulateErrors,
                                       final DLSN startPosition,
                                       final ScheduledThreadPoolExecutor executorService,
                                       final int delay,
                                       final AtomicInteger readCount,
                                       final AtomicInteger executionCount,
                                       final CountDownLatch done) {
        executionCount.incrementAndGet();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncLogReader reader = dlm.getAsyncLogReader(startPosition);
                    if (simulateErrors) {
                        ((BKAsyncLogReaderDLSN)reader).simulateErrors();
                    }
                    readNextWithRetry(threadToInterrupt, syncLatch, dlm, reader, simulateErrors,
                        startPosition, executorService, delay, readCount, executionCount);
                    done.countDown();
                } catch (IOException exc) {
                    int newDelay = Math.min(delay * 2, 500);
                    if (0 == delay) {
                        newDelay = 10;
                    }
                    positionReader(threadToInterrupt, syncLatch, dlm,
                        simulateErrors, startPosition, executorService,
                        newDelay, readCount, executionCount);
                }
            }
        };
        executorService.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }

    private static void positionReader(final Thread threadToInterrupt,
                                       final CountDownLatch syncLatch,
                                       final DistributedLogManager dlm,
                                       final boolean simulateErrors,
                                       final DLSN startPosition,
                                       final ScheduledThreadPoolExecutor executorService,
                                       final int delay,
                                       final AtomicInteger readCount,
                                       final AtomicInteger executionCount) {
        positionReader(threadToInterrupt,
                       syncLatch,
                       dlm,
                       simulateErrors,
                       startPosition,
                       executorService,
                       delay,
                       readCount,
                       executionCount,
                       new CountDownLatch(1));
    }

    public void simpleAsyncReadTest(String name, DistributedLogConfiguration confLocal) throws Exception {
        confLocal.setOutputBufferSize(1024);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= 10; j++) {
                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }

        long start = txid;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= 5; j++) {
            writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
            if (j % 2 == 0) {
                writer.setReadyToFlush();
                writer.flushAndSync();
            }
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        final CountDownLatch syncLatch = new CountDownLatch(txid - 1);
        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        final Thread currentThread = Thread.currentThread();

        TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncRead() throws Exception {
        String name = "distrlog-simpleasyncread";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        simpleAsyncReadTest(name, confLocal);
    }

    @Test
    public void testSimpleAsyncReadWriteWithMonitoredFuturePool() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setTaskExecutionWarnTimeMicros(1000);
        confLocal.setEnableTaskExecutionStats(true);
        simpleAsyncReadTest(name, confLocal);
    }

    @Test
    public void testAsyncReadEmptyRecords() throws Exception {
        String name = "distrlog-simpleasyncreadempty";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= 10; j++) {
                writer.write(DLMTestUtil.getEmptyLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }

        long start = txid;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= 5; j++) {
            writer.write(DLMTestUtil.getEmptyLogRecordInstance(txid++));
            if (j % 2 == 0) {
                writer.setReadyToFlush();
                writer.flushAndSync();
            }
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        AsyncLogReader asyncReader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        assertEquals(name, asyncReader.getStreamName());
        long numTrans = 0;
        DLSN lastDLSN = DLSN.InvalidDLSN;
        LogRecordWithDLSN record = asyncReader.readNext().get();
        while (null != record) {
            DLMTestUtil.verifyEmptyLogRecord(record);
            assert(record.getDlsn().getSlotId() == 0);
            assert (record.getDlsn().compareTo(lastDLSN) > 0);
            lastDLSN = record.getDlsn();
            numTrans++;
            if (numTrans >= (txid - 1)) {
                break;
            }
            record = asyncReader.readNext().get();
        }
        assertEquals((txid - 1), numTrans);
        asyncReader.close();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadPosition() throws Exception {
        String name = "distrlog-simpleasyncreadpos";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)(dlm.startLogSegmentNonPartitioned());
            for (long j = 1; j <= 10; j++) {
                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }

        long start = txid;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= 5; j++) {
            writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        final CountDownLatch syncLatch = new CountDownLatch(txid - 14);
        final AsyncLogReader reader = dlm.getAsyncLogReader(new DLSN(2, 2, 4));
        assertEquals(name, reader.getStreamName());
        final Thread currentThread = Thread.currentThread();

        TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, new DLSN(2, 3, 0));

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadWrite() throws Exception {
        testSimpleAsyncReadWriteInternal("distrlog-simpleasyncreadwrite", false);
    }

    @Test
    public void testSimpleAsyncReadWriteImmediateFlush() throws Exception {
        testSimpleAsyncReadWriteInternal("distrlog-simpleasyncreadwrite-imm-flush", true);
    }

    /**
     * Test if entries written using log segment metadata that doesn't support enveloping
     * can be read correctly by a reader supporting both.
     *
     * NOTE: An older reader cannot read enveloped entry, so we don't have a test case covering
     *       the other scenario.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testNoEnvelopeWriterEnvelopeReader() throws Exception {
        testSimpleAsyncReadWriteInternal("distributedlog-envelope-test", true,
                                         LogSegmentLedgerMetadata.LogSegmentLedgerMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value - 1);
    }

    class WriteFutureEventListener implements FutureEventListener<DLSN> {
        private final LogRecord record;
        private final long currentLedgerSeqNo;
        private final long currentEntryId;
        private final Thread currentThread;

        WriteFutureEventListener(LogRecord record, long currentLedgerSeqNo, long currentEntryId, Thread currentThread) {
            this.record = record;
            this.currentLedgerSeqNo = currentLedgerSeqNo;
            this.currentEntryId = currentEntryId;
            this.currentThread = currentThread;
        }

        /**
         * Invoked if the computation completes successfully
         */
        @Override
        public void onSuccess(DLSN value) {
            if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                LOG.debug("Thread Interrupted - Ledger Seq No: " + value.getLedgerSequenceNo() + ", Expected: " + currentLedgerSeqNo);
                currentThread.interrupt();
            }

            if(value.getEntryId() != currentEntryId) {
                LOG.debug("Thread Interrupted - EntryId: " + value.getEntryId() + ", TxId " + record.getTransactionId() + "Expected " + currentEntryId);
                currentThread.interrupt();
            }
        }

        /**
         * Invoked if the computation completes unsuccessfully
         */
        @Override
        public void onFailure(Throwable cause) {
            LOG.debug("Thread Interrupted - onFailure", cause);
            currentThread.interrupt();
        }
    }

    public void testSimpleAsyncReadWriteInternal(String name, boolean immediateFlush)
            throws Exception {
        testSimpleAsyncReadWriteInternal(name, immediateFlush,
                                         LogSegmentLedgerMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    public void testSimpleAsyncReadWriteInternal(String name, boolean immediateFlush,
                                                 int logSegmentVersion) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        confLocal.setDLLedgerMetadataLayoutVersion(logSegmentVersion);
        confLocal.setImmediateFlushEnabled(immediateFlush);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(30);
        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        assertEquals(name, reader.getStreamName());
        final Thread currentThread = Thread.currentThread();

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
                if (i == 0 && j == 0) {
                    TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);
                }
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadWriteStartEmpty() throws Exception {
        String name = "distrlog-simpleasyncreadwritestartempty";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(30);
        final Thread currentThread = Thread.currentThread();
        final AtomicInteger executionCount = new AtomicInteger(0);
        final AtomicInteger readCount = new AtomicInteger(0);

        positionReader(currentThread, syncLatch, dlm, false, DLSN.InvalidDLSN, new ScheduledThreadPoolExecutor(1), 0, readCount, executionCount);

        // Increase the probability of reader failure and retry
        Thread.sleep(500);
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        assertEquals(true, executionCount.get() > 1);
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadWriteStartEmptyFactory() throws Exception {
        int count = 50;
        String name = "distrlog-simpleasyncreadwritestartemptyfactory";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(confLocal, createDLMURI("/" + name));
        DistributedLogManager[] dlms = new DistributedLogManager[count];
        final AtomicInteger[] readCounts = new AtomicInteger[count];
        final AtomicInteger[] executionCounts = new AtomicInteger[count];
        final CountDownLatch syncLatch = new CountDownLatch(3 * count);
        final Thread currentThread = Thread.currentThread();
        final CountDownLatch doneLatch = new CountDownLatch(count);
        for (int s = 0; s < count; s++) {
            executionCounts[s] = new AtomicInteger(0);
            readCounts[s] = new AtomicInteger(0);
            dlms[s] = factory.createDistributedLogManagerWithSharedClients(name + String.format("%d", s));
            positionReader(currentThread, syncLatch, dlms[s], false,
                    DLSN.InvalidDLSN, new ScheduledThreadPoolExecutor(1), 0, readCounts[s], executionCounts[s],
                    doneLatch);
        }


        // Increase the probability of reader failure and retry
        doneLatch.await();
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter[] writers = new BKUnPartitionedAsyncLogWriter[count];
            for (int s = 0; s < count; s++) {
                writers[s] = (BKUnPartitionedAsyncLogWriter)(dlms[s].startAsyncLogSegmentNonPartitioned());
            }
            for (long j = 0; j < 1; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                for (int s = 0; s < count; s++) {
                    Future<DLSN> dlsnFuture = writers[s].write(record);
                    dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
                }
            }
            for (int s = 0; s < count; s++) {
                writers[s].abort();
            }
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(90, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        if (!success) {
            for (int s = 0; s < count; s++) {
                assertEquals(String.format("%d Stream", s), 3, readCounts[s].get());
            }
        }
        for (int s = 0; s < count; s++) {
            assertEquals(true, executionCounts[s].get() > 1);
            dlms[s].close();
        }
    }

    @Test
    public void testSimpleAsyncReadWriteSimulateErrors() throws Exception {

        String name = "distrlog-simpleasyncreadwritesimulateerrors";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(200);
        final Thread currentThread = Thread.currentThread();
        final AtomicInteger executionCount = new AtomicInteger(0);
        final AtomicInteger readCount = new AtomicInteger(0);

        positionReader(currentThread, syncLatch, dlm, true, DLSN.InvalidDLSN,
            new ScheduledThreadPoolExecutor(1), 0, readCount, executionCount);

        int txid = 1;
        for (long i = 0; i < 20; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        assertEquals(true, executionCount.get() > 1);
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadWritePolling() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritepolling", 0);
    }

    @Test
    public void testSimpleAsyncReadWriteLongPoll() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritelongpoll", 1);
    }

    @Test
    public void testSimpleAsyncReadWritePiggyBack() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritepiggyback", 2);
    }

    @Test
    public void testSimpleAsyncReadWritePiggyBackSpec() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritepiggybackspec", 3);
    }

    private void testSimpleAsyncReadWriteLACOptions(String name, int lacOption) throws Exception {

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setEnableReadAhead(true);
        confLocal.setReadAheadWaitTime(500);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setReadAheadMaxEntries(100);
        confLocal.setOutputBufferSize(1024);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(100);
        confLocal.setReadLACOption(lacOption);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(30);
        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        final Thread currentThread = Thread.currentThread();

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                Thread.sleep(50);
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                            LOG.debug("Thread Interrupted - EntryId: " + value.getLedgerSequenceNo() + ", TxId " + currentLedgerSeqNo);
                            currentThread.interrupt();
                        }
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        LOG.debug("Thread Interrupted - onFailure", cause);
                        currentThread.interrupt();
                    }
                });
                if (i == 0 && j == 0) {
                    TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);
                }
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                LOG.debug("Thread Interrupted - onFailure", exc);
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

    @Test
    public void testWritesAfterErrorOutPendingRequests() throws Exception {
        final String name = "distrlog-writes-after-error-out-pending-requests";

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread {} received uncaught exception : ", t.getName(), e);
            }
        });

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        final BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        final CountDownLatch startLatch = new CountDownLatch(1);
        writer.getOrderedFuturePool().apply(new Function0<Object>() {
            @Override
            public Object apply() {
                try {
                    startLatch.await();
                    LOG.info("Starting writes and set force rolling to true");
                    writer.setForceRolling(true);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted on waiting start latch : ", e);
                }
                return null;
            }
        });

        writer.getOrderedFuturePool().apply(new Function0<Object>() {
            @Override
            public Object apply() {
                try {
                    startLatch.await();
                    LOG.info("Starting writes and set force rolling to true");
                    writer.setForceRolling(true);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted on waiting start latch : ", e);
                }
                return null;
            }
        });

        final Future<DLSN> firstWrite;
        final Future<DLSN> secondWrite;
        try {
            FailpointUtils.setFailpoint(
                FailpointUtils.FailPointName.FP_LogWriterIssuePending,
                FailpointUtils.FailPointActions.FailPointAction_Throw);

            // rolls ledger, exception hit in rollLogSegmentAndIssuePendingRequests
            firstWrite = writer.write(DLMTestUtil.getLogRecordInstance(1L));

            // added to queue, aborted when rollLogSegmentAndIssuePendingRequests fails
            secondWrite = writer.write(DLMTestUtil.getLogRecordInstance(2L));

            startLatch.countDown();

            LOG.info("waiting for write to be completed");

            Await.result(firstWrite);

            LOG.info("first write completed");

            try {
                Await.result(secondWrite);
            } catch (IOException ioe) {
                LOG.info("caught expected exception ", ioe);
            }

            LOG.info("second write completed");

            writer.closeAndComplete();
        } finally {
            FailpointUtils.removeFailpoint(
                FailpointUtils.FailPointName.FP_LogWriterIssuePending);
        }

        LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
        int numReads = 0;
        long expectedTxID = 1L;
        LogRecord record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxID, record.getTransactionId());

            ++numReads;
            ++expectedTxID;

            record = reader.readNext(false);
        }

        assertEquals(1, numReads);

        reader.close();
    }

    @Test(timeout = 60000)
    public void testCancelReadRequestOnReaderClosed() throws Exception {
        final String name = "distrlog-cancel-read-requests-on-reader-closed";

        DistributedLogManager dlm = createNewDLM(testConf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.closeAndComplete();

        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
        LogRecordWithDLSN record = Await.result(reader.readNext());
        assertEquals(1L, record.getTransactionId());
        DLMTestUtil.verifyLogRecord(record);

        final CountDownLatch readLatch = new CountDownLatch(1);
        final AtomicBoolean receiveExpectedException = new AtomicBoolean(false);
        Thread readThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Await.result(reader.readNext());
                } catch (ReadCancelledException rce) {
                    receiveExpectedException.set(true);
                } catch (Throwable t) {
                    LOG.error("Receive unexpected exception on reading stream {} : ", name, t);
                }
                readLatch.countDown();
            }
        }, "read-thread");
        readThread.start();

        Thread.sleep(1000);

        // close reader should cancel the pending read next
        reader.close();

        readLatch.await();
        readThread.join();

        assertTrue("Read request should be cancelled.", receiveExpectedException.get());

        // closed reader should reject any readNext
        try {
            Await.result(reader.readNext());
            fail("Reader should reject readNext if it is closed.");
        } catch (ReadCancelledException rce) {
            // expected
        }

        dlm.close();
    }

    @Test
    public void testAsyncWriteWithMinDelayBetweenFlushes() throws Exception {
        String name = "distrlog-asyncwrite-mindelay";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setMinDelayBetweenImmediateFlushMs(100);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();
        final int COUNT = 5000;
        final CountDownLatch syncLatch = new CountDownLatch(COUNT);
        int txid = 1;
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        Stopwatch executionTime = Stopwatch.createStarted();
        for (long i = 0; i < COUNT; i++) {
            Thread.sleep(1);
            final LogRecord record = DLMTestUtil.getLogRecordInstance(txid++);
            Future<DLSN> dlsnFuture = writer.write(record);
            dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                @Override
                public void onSuccess(DLSN value) {
                    syncLatch.countDown();
                    LOG.debug("SyncLatch: {} ; DLSN: {} ", syncLatch.getCount(), value);
                }
                @Override
                public void onFailure(Throwable cause) {
                    currentThread.interrupt();
                }
            });
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        // Abort, not graceful close, since the latter will
        // flush as well, and may add an entry.
        writer.abort();

        executionTime.stop();
        assert(!(Thread.interrupted()));
        assert(success);

        LogRecordWithDLSN last = dlm.getLastLogRecord();
        LOG.info("Last Entry {}; elapsed time {}", last.getDlsn().getEntryId(), executionTime.elapsed(TimeUnit.MILLISECONDS));

        // Regardless of how many records we wrote; the number of BK entries should always be bounded by the min delay.
        // Since there are two flush processes--data flush and control flush, and since control flush may also end up flushing
        // data if data is available, the upper bound is 2*(time/min_delay + 1)
        assertTrue(last.getDlsn().getEntryId() <= ((executionTime.elapsed(TimeUnit.MILLISECONDS) / confLocal.getMinDelayBetweenImmediateFlushMs() + 1))*2);
        DLMTestUtil.verifyLogRecord(last);

        dlm.close();
    }

    public void writeRecordsWithOutstandingWriteLimit(int stream, int global, boolean shouldFail) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setPerWriterOutstandingWriteLimit(stream);
        confLocal.setOutstandingWriteLimitDarkmode(false);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        if (global > -1) {
            ((BKDistributedLogManager) dlm).setWriteLimiter(new SimplePermitLimiter(false, global, new NullStatsLogger(), true));
        }
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        ArrayList<Future<DLSN>> results = new ArrayList<Future<DLSN>>(1000);
        for (int i = 0; i < 1000; i++) {
            results.add(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
        }
        for (Future<DLSN> result : results) {
            try {
                Await.result(result);
                if (shouldFail) {
                    fail("should fail due to no outstanding writes permitted");
                }
            } catch (OverCapacityException ex) {
                assertTrue(shouldFail);
            }
        }
        writer.closeAndComplete();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testOutstandingWriteLimitNoLimit() throws Exception {
        writeRecordsWithOutstandingWriteLimit(-1, -1, false);
    }

    @Test(timeout = 60000)
    public void testOutstandingWriteLimitVeryHighLimit() throws Exception {
        writeRecordsWithOutstandingWriteLimit(Integer.MAX_VALUE, Integer.MAX_VALUE, false);
    }

    @Test(timeout = 60000)
    public void testOutstandingWriteLimitBlockAllStreamLimit() throws Exception {
        writeRecordsWithOutstandingWriteLimit(0, Integer.MAX_VALUE, true);
    }

    @Test(timeout = 60000)
    public void testOutstandingWriteLimitBlockAllGlobalLimit() throws Exception {
        writeRecordsWithOutstandingWriteLimit(Integer.MAX_VALUE, 0, true);
    }

    @Test(timeout = 60000)
    public void testOutstandingWriteLimitBlockAllLimitWithDarkmode() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setPerWriterOutstandingWriteLimit(0);
        confLocal.setOutstandingWriteLimitDarkmode(true);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        ArrayList<Future<DLSN>> results = new ArrayList<Future<DLSN>>(1000);
        for (int i = 0; i < 1000; i++) {
            results.add(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
        }
        for (Future<DLSN> result : results) {
            Await.result(result);
        }
        writer.closeAndComplete();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testCloseAndCompleteLogSegmentWhenStreamIsInError() throws Exception {
        String name = "distrlog-close-and-complete-logsegment-when-stream-is-in-error";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);

        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        long txId = 1L;
        for (int i = 0; i < 5; i++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
        }

        BKPerStreamLogWriter logWriter = writer.perStreamWriter;

        // fence the ledger
        dlm.getWriterBKC().get().openLedger(logWriter.getLedgerHandle().getId(),
                BookKeeper.DigestType.CRC32, confLocal.getBKDigestPW().getBytes(UTF_8));

        try {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
            fail("Should fail write to a fenced ledger with BKTransmitException");
        } catch (BKTransmitException bkte) {
            // expected
        }

        try {
            writer.closeAndComplete();
            fail("Should fail to complete a log segment when its ledger is fenced");
        } catch (BKTransmitException bkte) {
            // expected
        }

        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        assertEquals(1, segments.size());
        assertTrue(segments.get(0).isInProgress());

        dlm.close();
    }

    @Test(timeout = 60000)
    public void testCloseAndCompleteLogSegmentWhenCloseFailed() throws Exception {
        String name = "distrlog-close-and-complete-logsegment-when-close-failed";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);

        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        long txId = 1L;
        for (int i = 0; i < 5; i++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
        }

        BKPerStreamLogWriter logWriter = writer.perStreamWriter;

        // fence the ledger
        dlm.getWriterBKC().get().openLedger(logWriter.getLedgerHandle().getId(),
                BookKeeper.DigestType.CRC32, confLocal.getBKDigestPW().getBytes(UTF_8));

        try {
            writer.closeAndComplete();
            fail("Should fail to complete a log segment when its ledger is fenced");
        } catch (IOException ioe) {
            // expected
            LOG.error("Failed to close and complete log segment {} : ", logWriter.getFullyQualifiedLogSegment(), ioe);
        }

        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        assertEquals(1, segments.size());
        assertTrue(segments.get(0).isInProgress());

        dlm.close();
    }

    private void testAsyncReadIdleErrorInternal(String name, final int idleReaderErrorThreshold, final boolean heartBeatUsingControlRecs) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(50);
        confLocal.setReaderIdleErrorThresholdMillis(idleReaderErrorThreshold);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();
        final int segmentSize = 3;
        final int numSegments = 3;
        final CountDownLatch latch = new CountDownLatch(1);
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        int txid = 1;
                        for (long i = 0; i < numSegments; i++) {
                            long start = txid;
                            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
                            for (long j = 1; j <= segmentSize; j++) {
                                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
                                if ((i == 0) && (j == 1)) {
                                    latch.countDown();
                                }
                            }

                            if (heartBeatUsingControlRecs) {
                                // There should be a control record such that
                                // wait time + commit time (BK) < Idle Reader Threshold
                                int threadSleepTime = idleReaderErrorThreshold
                                    - 200 // BK commitTime
                                    - 100; //safety margin

                                for (int iter = 1; iter <= (2 * idleReaderErrorThreshold / threadSleepTime) ; iter++) {
                                    Thread.sleep(threadSleepTime);
                                    writer.write(DLMTestUtil.getLargeLogRecordInstance(txid, true));
                                    writer.setReadyToFlush();
                                }
                                Thread.sleep(threadSleepTime);
                            }

                            writer.closeAndComplete();
                            if (!heartBeatUsingControlRecs) {
                                Thread.sleep(2 * idleReaderErrorThreshold);
                            }
                        }
                    } catch (Exception exc) {
                        if (!executor.isShutdown()) {
                            currentThread.interrupt();
                        }
                    }
                }
            }, 0, TimeUnit.MILLISECONDS);

        latch.await();
        AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
        boolean exceptionEncountered = false;
        int recordCount = 0;
        try {
            while (true) {
                Future<LogRecordWithDLSN> record = reader.readNext();
                Await.result(record);
                recordCount++;

                if (recordCount >= segmentSize * numSegments) {
                    break;
                }
            }
        } catch (IdleReaderException exc) {
            exceptionEncountered = true;
        }

        if (heartBeatUsingControlRecs) {
            assert (!exceptionEncountered);
            Assert.assertEquals(segmentSize * numSegments, recordCount);
        } else {
            assert (exceptionEncountered);
            Assert.assertEquals(segmentSize, recordCount);
        }
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void testAsyncReadIdleControlRecord() throws Exception {
        String name = "distrlog-async-reader-idle-error-control";
        testAsyncReadIdleErrorInternal(name, 500, true);
    }

    @Test(timeout = 10000)
    public void testAsyncReadIdleError() throws Exception {
        String name = "distrlog-async-reader-idle-error";
        testAsyncReadIdleErrorInternal(name, 1000, false);
    }


    @Test(timeout = 60000)
    public void testReleaseLockAfterFailedToRecover() throws Exception {
        String name = "release-lock-after-failed-to-recover";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setLockTimeout(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer =
                (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        Await.result(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
        writer.abort();

        for (int i = 0; i < 2; i++) {
            FailpointUtils.setFailpoint(
                    FailpointUtils.FailPointName.FP_RecoverIncompleteLogSegments,
                    FailpointUtils.FailPointActions.FailPointAction_Throw);

            try {
                dlm.startAsyncLogSegmentNonPartitioned();
                fail("Should fail during recovering incomplete log segments");
            } catch (IOException ioe) {
                // expected;
            } finally {
                FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_RecoverIncompleteLogSegments);
            }
        }

        writer = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());

        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        assertEquals(1, segments.size());
        assertFalse(segments.get(0).isInProgress());

        writer.close();
        dlm.close();
    }

    @Test(timeout = 10000)
    public void testAsyncReadMissingZKNotification() throws Exception {
        String name = "distrlog-async-reader-missing-zk-notification";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        confLocal.setReaderIdleErrorThresholdMillis(2000);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();
        final int segmentSize = 10;
        final int numSegments = 3;
        final CountDownLatch latch = new CountDownLatch(1);
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        int txid = 1;
                        for (long i = 0; i < numSegments; i++) {
                            long start = txid;
                            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
                            for (long j = 1; j <= segmentSize; j++) {
                                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
                                if ((i == 0) && (j == 1)) {
                                    latch.countDown();
                                }
                            }
                            writer.closeAndComplete();
                            Thread.sleep(100);
                        }
                    } catch (Exception exc) {
                        if (!executor.isShutdown()) {
                            currentThread.interrupt();
                        }
                    }
                }
            }, 0, TimeUnit.MILLISECONDS);

        latch.await();
        BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN)dlm.getAsyncLogReader(DLSN.InitialDLSN);
        reader.disableReadAheadZKNotification();
        boolean exceptionEncountered = false;
        int recordCount = 0;
        try {
            while (true) {
                Future<LogRecordWithDLSN> record = reader.readNext();
                Await.result(record);
                recordCount++;

                if (recordCount >= segmentSize * numSegments) {
                    break;
                }
            }
        } catch (IdleReaderException exc) {
            exceptionEncountered = true;
        }
        assert(!exceptionEncountered);
        Assert.assertEquals(recordCount, segmentSize * numSegments);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

}
