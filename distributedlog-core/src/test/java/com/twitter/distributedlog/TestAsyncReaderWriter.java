package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.twitter.distributedlog.config.ConcurrentBaseConfiguration;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAccessor;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.feature.FixedValueFeature;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.annotations.DistributedLogAnnotations;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.IdleReaderException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.exceptions.ReadCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import junit.framework.Assert;
import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.DLMTestUtil.validateFutureFailed;
import static com.twitter.distributedlog.LogRecordSet.MAX_LOGRECORD_SIZE;
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

    /**
     * Test writing control records to writers: writers should be able to write control records, and
     * the readers should skip control records while reading.
     */
    @Test(timeout = 60000)
    public void testWriteControlRecord() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        // Write 3 log segments. For each log segments, write one control record and nine user records.
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLogSegmentSeqNo = i + 1;
            BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            DLSN dlsn = Await.result(writer.writeControlRecord(new LogRecord(txid++, "control".getBytes(UTF_8))));
            assertEquals(currentLogSegmentSeqNo, dlsn.getLogSegmentSequenceNo());
            assertEquals(0, dlsn.getEntryId());
            assertEquals(0, dlsn.getSlotId());
            for (long j = 1; j < 10; j++) {
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Await.result(writer.write(record));
            }
            writer.closeAndComplete();
        }
        dlm.close();

        // Read all the written data: It should skip control records and only return user records.
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

    @Test(timeout = 60000)
    public void testAsyncWritePendingWritesAbortedWhenLedgerRollTriggerFails() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        confLocal.setMaxLogSegmentBytes(1024);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // Write one record larger than max seg size. Ledger doesn't roll until next write.
        int txid = 1;
        LogRecord record = DLMTestUtil.getLogRecordInstance(txid++, 2048);
        Future<DLSN> result = writer.write(record);
        DLSN dlsn = Await.result(result, Duration.fromSeconds(10));
        assertEquals(1, dlsn.getLogSegmentSequenceNo());

        record = DLMTestUtil.getLogRecordInstance(txid++, MAX_LOGRECORD_SIZE + 1);
        result = writer.write(record);
        validateFutureFailed(result, LogRecordTooLongException.class);

        record = DLMTestUtil.getLogRecordInstance(txid++, MAX_LOGRECORD_SIZE + 1);
        result = writer.write(record);
        validateFutureFailed(result, WriteException.class);

        record = DLMTestUtil.getLogRecordInstance(txid++, MAX_LOGRECORD_SIZE + 1);
        result = writer.write(record);
        validateFutureFailed(result, WriteException.class);

        writer.closeAndComplete();
        dlm.close();
    }

    /**
     * Test Case: Simple Async Writes. Writes 30 records. They should be written correctly.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSimpleAsyncWrite() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 10;

        DistributedLogManager dlm = createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(numLogSegments * numRecordsPerLogSegment);
        final AtomicBoolean errorsFound = new AtomicBoolean(false);
        final AtomicReference<DLSN> maxDLSN = new AtomicReference<DLSN>(DLSN.InvalidDLSN);
        int txid = 1;
        for (long i = 0; i < numLogSegments; i++) {
            final long currentLogSegmentSeqNo = i + 1;
            BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < numRecordsPerLogSegment; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLogSegmentSequenceNo() != currentLogSegmentSeqNo) {
                            LOG.debug("LogSegmentSequenceNumber: {}, Expected {}", value.getLogSegmentSequenceNo(), currentLogSegmentSeqNo);
                            errorsFound.set(true);
                        }

                        if(value.getEntryId() != currentEntryId) {
                            LOG.debug("EntryId: {}, Expected {}", value.getEntryId(), currentEntryId);
                            errorsFound.set(true);
                        }

                        if (value.compareTo(maxDLSN.get()) > 0) {
                            maxDLSN.set(value);
                        }

                        syncLatch.countDown();
                        LOG.debug("SyncLatch: {}", syncLatch.getCount());
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        LOG.error("Encountered exception on writing record {} in log segment {}", currentEntryId, currentLogSegmentSeqNo);
                        errorsFound.set(true);
                    }
                });
            }
            writer.closeAndComplete();
        }

        syncLatch.await();
        assertFalse("Should not encounter any errors for async writes", errorsFound.get());

        LogRecordWithDLSN last = dlm.getLastLogRecord();
        assertEquals("Last DLSN" + last.getDlsn() + " isn't the maximum DLSN " + maxDLSN.get(),
                last.getDlsn(), maxDLSN.get());
        assertEquals(last.getDlsn(), dlm.getLastDLSN());
        assertEquals(last.getDlsn(), Await.result(dlm.getLastDLSNAsync()));
        DLMTestUtil.verifyLargeLogRecord(last);

        dlm.close();
    }

    /**
     * Write records into <i>numLogSegments</i> log segments. Each log segment has <i>numRecordsPerLogSegment</i> records.
     *
     * @param dlm
     *          distributedlog manager
     * @param numLogSegments
     *          number of log segments
     * @param numRecordsPerLogSegment
     *          number records per log segment
     * @param startTxId
     *          start tx id
     * @return next tx id
     */
    private static long writeRecords(DistributedLogManager dlm,
                                     int numLogSegments,
                                     int numRecordsPerLogSegment,
                                     long startTxId,
                                     boolean emptyRecord) throws IOException {
        long txid = startTxId;
        for (long i = 0; i < numLogSegments; i++) {
            BKSyncLogWriter writer = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= numRecordsPerLogSegment; j++) {
                if (emptyRecord) {
                    writer.write(DLMTestUtil.getEmptyLogRecordInstance(txid++));
                } else {
                    writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
                }
            }
            writer.closeAndComplete();
        }
        return txid;
    }

    /**
     * Write <code>numRecords</code> records to the log, starting with <code>startTxId</code>.
     * It flushes every <code>flushPerNumRecords</code> records.
     *
     * @param dlm
     *          distributedlog manager
     * @param numRecords
     *          num records to write
     * @param startTxId
     *          start tx id
     * @param flushPerNumRecords
     *          number records to flush
     * @return next tx id
     * @throws IOException
     */
    private static long writeLogSegment(DistributedLogManager dlm,
                                        int numRecords,
                                        long startTxId,
                                        int flushPerNumRecords,
                                        boolean emptyRecord) throws IOException {
        long txid = startTxId;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= numRecords; j++) {
            if (emptyRecord) {
                writer.write(DLMTestUtil.getEmptyLogRecordInstance(txid++));
            } else {
                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
            }
            if (j % flushPerNumRecords == 0 ) {
                writer.setReadyToFlush();
                writer.flushAndSync();
            }
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();
        return txid;
    }

    private static void readNext(final AsyncLogReader reader,
                                 final DLSN startPosition,
                                 final long startSequenceId,
                                 final boolean monotonic,
                                 final CountDownLatch syncLatch,
                                 final CountDownLatch completionLatch,
                                 final AtomicBoolean errorsFound) {
        Future<LogRecordWithDLSN> record = reader.readNext();
        record.addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN value) {
                try {
                    if (monotonic) {
                        assertEquals(startSequenceId, value.getSequenceId());
                    } else {
                        assertTrue(value.getSequenceId() < 0);
                        assertTrue(value.getSequenceId() > startSequenceId);
                    }
                    LOG.debug("Recevied record {} from {}", value.getDlsn(), reader.getStreamName());
                    assertTrue(!value.isControl());
                    assertTrue(value.getDlsn().getSlotId() == 0);
                    assertTrue(value.getDlsn().compareTo(startPosition) >= 0);
                    DLMTestUtil.verifyLargeLogRecord(value);
                } catch (Exception exc) {
                    LOG.debug("Exception Encountered when verifying log record {} : ", value.getDlsn(), exc);
                    errorsFound.set(true);
                    completionLatch.countDown();
                    return;
                }
                syncLatch.countDown();
                if (syncLatch.getCount() <= 0) {
                    completionLatch.countDown();
                } else {
                    TestAsyncReaderWriter.readNext(
                            reader,
                            value.getDlsn().getNextDLSN(),
                            monotonic ? value.getSequenceId() + 1 : value.getSequenceId(),
                            monotonic,
                            syncLatch,
                            completionLatch,
                            errorsFound);
                }
            }
            @Override
            public void onFailure(Throwable cause) {
                LOG.debug("Encountered Exception on reading {}", reader.getStreamName(), cause);
                errorsFound.set(true);
                completionLatch.countDown();
            }
        });
    }

    void simpleAsyncReadTest(String name, DistributedLogConfiguration confLocal) throws Exception {
        confLocal.setOutputBufferSize(1024);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 10;

        // Write 30 records: 3 log segments, 10 records per log segment
        long txid = 1L;
        txid = writeRecords(dlm, numLogSegments, numRecordsPerLogSegment, txid, false);
        // Write another log segment with 5 records and flush every 2 records
        txid = writeLogSegment(dlm, 5, txid, 2, false);

        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        final CountDownLatch syncLatch = new CountDownLatch((int) (txid - 1));
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final AtomicBoolean errorsFound = new AtomicBoolean(false);

        boolean monotonic = LogSegmentMetadata.supportsSequenceId(confLocal.getDLLedgerMetadataLayoutVersion());
        TestAsyncReaderWriter.readNext(
                reader,
                DLSN.InvalidDLSN,
                monotonic ? 0L : Long.MIN_VALUE,
                monotonic,
                syncLatch,
                completionLatch,
                errorsFound);

        completionLatch.await();
        assertFalse("Errors encountered on reading records", errorsFound.get());
        syncLatch.await();

        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testSimpleAsyncRead() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        simpleAsyncReadTest(name, confLocal);
    }

    @Test(timeout = 60000)
    public void testSimpleAsyncReadWriteWithMonitoredFuturePool() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setTaskExecutionWarnTimeMicros(1000);
        confLocal.setEnableTaskExecutionStats(true);
        simpleAsyncReadTest(name, confLocal);
    }

    @Test(timeout = 60000)
    public void testBulkAsyncRead() throws Exception {
        String name = "distrlog-bulkasyncread";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadMaxRecords(10000);
        confLocal.setReadAheadBatchSize(10);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 20;

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        writeRecords(dlm, numLogSegments, numRecordsPerLogSegment, 1L, false);

        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
        int expectedTxID = 1;
        int numReads = 0;
        while (expectedTxID <= numLogSegments * numRecordsPerLogSegment) {
            if (expectedTxID == numLogSegments * numRecordsPerLogSegment) {
                break;
            }
            List<LogRecordWithDLSN> records = Await.result(reader.readBulk(20));
            LOG.info("Bulk read {} entries.", records.size());

            assertTrue(records.size() >= 1);
            for (LogRecordWithDLSN record : records) {
                assertEquals(expectedTxID, record.getTransactionId());
                ++expectedTxID;
            }
            ++numReads;
        }

        // we expect bulk read works
        assertTrue(numReads < 60);

        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testBulkAsyncReadWithWriteBatch() throws Exception {
        String name = "distrlog-bulkasyncread-with-writebatch";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(1024000);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadMaxRecords(10000);
        confLocal.setReadAheadBatchSize(10);

        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 20;

        writeRecords(dlm, numLogSegments, numRecordsPerLogSegment, 1L, false);

        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
        int expectedTxID = 1;
        for (long i = 0; i < 3; i++) {
            // since we batched 20 entries into single bookkeeper entry
            // we should be able to read 20 entries as a batch.
            List<LogRecordWithDLSN> records = Await.result(reader.readBulk(20));
            assertEquals(20, records.size());
            for (LogRecordWithDLSN record : records) {
                assertEquals(expectedTxID, record.getTransactionId());
                ++expectedTxID;
            }
        }

        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testAsyncReadEmptyRecords() throws Exception {
        String name = "distrlog-simpleasyncreadempty";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 10;

        long txid = 1L;
        // write 3 log segments, 10 records per log segment
        txid = writeRecords(dlm, numLogSegments, numRecordsPerLogSegment, txid, true);
        // write another log segment with 5 records and flush every 2 records
        txid = writeLogSegment(dlm, 5, txid, 2, true);

        AsyncLogReader asyncReader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        assertEquals("Expected stream name = " + name + " but " + asyncReader.getStreamName() + " found",
                name, asyncReader.getStreamName());
        long numTrans = 0;
        DLSN lastDLSN = DLSN.InvalidDLSN;
        LogRecordWithDLSN record = Await.result(asyncReader.readNext());
        while (null != record) {
            DLMTestUtil.verifyEmptyLogRecord(record);
            assertEquals(0, record.getDlsn().getSlotId());
            assertTrue(record.getDlsn().compareTo(lastDLSN) > 0);
            lastDLSN = record.getDlsn();
            numTrans++;
            if (numTrans >= (txid - 1)) {
                break;
            }
            record = Await.result(asyncReader.readNext());
        }
        assertEquals((txid - 1), numTrans);
        asyncReader.close();
        dlm.close();
    }

    /**
     * Test Async Read by positioning to a given position in the log
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSimpleAsyncReadPosition() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 10;

        long txid = 1L;
        // write 3 log segments, 10 records per log segment
        txid = writeRecords(dlm, numLogSegments, numRecordsPerLogSegment, txid, false);
        // write another log segment with 5 records
        txid = writeLogSegment(dlm, 5, txid, Integer.MAX_VALUE, false);

        final CountDownLatch syncLatch = new CountDownLatch((int)(txid - 14));
        final CountDownLatch doneLatch = new CountDownLatch(1);
        final AtomicBoolean errorsFound = new AtomicBoolean(false);
        final AsyncLogReader reader = dlm.getAsyncLogReader(new DLSN(2, 2, 4));
        assertEquals(name, reader.getStreamName());

        boolean monotonic = LogSegmentMetadata.supportsSequenceId(confLocal.getDLLedgerMetadataLayoutVersion());
        TestAsyncReaderWriter.readNext(
                reader,
                new DLSN(2, 3, 0),
                monotonic ? 13L : Long.MIN_VALUE,
                monotonic,
                syncLatch,
                doneLatch,
                errorsFound);

        doneLatch.await();
        assertFalse("Errors found on reading records", errorsFound.get());
        syncLatch.await();

        reader.close();
        dlm.close();
    }

    /**
     * Test write/read entries when immediate flush is disabled.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSimpleAsyncReadWrite() throws Exception {
        testSimpleAsyncReadWriteInternal(runtime.getMethodName(), false);
    }

    /**
     * Test write/read entries when immediate flush is enabled.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSimpleAsyncReadWriteImmediateFlush() throws Exception {
        testSimpleAsyncReadWriteInternal(runtime.getMethodName(), true);
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
        testSimpleAsyncReadWriteInternal(runtime.getMethodName(), true,
                LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value - 1);
    }

    static class WriteFutureEventListener implements FutureEventListener<DLSN> {
        private final LogRecord record;
        private final long currentLogSegmentSeqNo;
        private final long currentEntryId;
        private final CountDownLatch syncLatch;
        private final AtomicBoolean errorsFound;
        private final boolean verifyEntryId;

        WriteFutureEventListener(LogRecord record,
                                 long currentLogSegmentSeqNo,
                                 long currentEntryId,
                                 CountDownLatch syncLatch,
                                 AtomicBoolean errorsFound,
                                 boolean verifyEntryId) {
            this.record = record;
            this.currentLogSegmentSeqNo = currentLogSegmentSeqNo;
            this.currentEntryId = currentEntryId;
            this.syncLatch = syncLatch;
            this.errorsFound = errorsFound;
            this.verifyEntryId = verifyEntryId;
        }

        /**
         * Invoked if the computation completes successfully
         */
        @Override
        public void onSuccess(DLSN value) {
            if(value.getLogSegmentSequenceNo() != currentLogSegmentSeqNo) {
                LOG.error("Ledger Seq No: {}, Expected: {}", value.getLogSegmentSequenceNo(), currentLogSegmentSeqNo);
                errorsFound.set(true);
            }

            if(verifyEntryId && value.getEntryId() != currentEntryId) {
                LOG.error("EntryId: {}, Expected: {}", value.getEntryId(), currentEntryId);
                errorsFound.set(true);
            }
            syncLatch.countDown();
        }

        /**
         * Invoked if the computation completes unsuccessfully
         */
        @Override
        public void onFailure(Throwable cause) {
            LOG.error("Encountered failures on writing record as (lid = {}, eid = {}) :",
                    new Object[]{currentLogSegmentSeqNo, currentEntryId, cause});
            errorsFound.set(true);
            syncLatch.countDown();
        }
    }

    void testSimpleAsyncReadWriteInternal(String name, boolean immediateFlush)
            throws Exception {
        testSimpleAsyncReadWriteInternal(name, immediateFlush,
                                         LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    void testSimpleAsyncReadWriteInternal(String name, boolean immediateFlush,
                                          int logSegmentVersion) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        confLocal.setDLLedgerMetadataLayoutVersion(logSegmentVersion);
        confLocal.setImmediateFlushEnabled(immediateFlush);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 10;

        final CountDownLatch readLatch = new CountDownLatch(numLogSegments * numRecordsPerLogSegment);
        final CountDownLatch readDoneLatch = new CountDownLatch(1);
        final AtomicBoolean readErrors = new AtomicBoolean(false);
        final CountDownLatch writeLatch = new CountDownLatch(numLogSegments * numRecordsPerLogSegment);
        final AtomicBoolean writeErrors = new AtomicBoolean(false);
        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        assertEquals(name, reader.getStreamName());

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLogSegmentSeqNo = i + 1;
            BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(
                        record, currentLogSegmentSeqNo, currentEntryId, writeLatch, writeErrors, true));
                if (i == 0 && j == 0) {
                    boolean monotonic = LogSegmentMetadata.supportsSequenceId(logSegmentVersion);
                    TestAsyncReaderWriter.readNext(
                            reader,
                            DLSN.InvalidDLSN,
                            monotonic ? 0L : Long.MIN_VALUE,
                            monotonic,
                            readLatch,
                            readDoneLatch,
                            readErrors);
                }
            }
            writer.closeAndComplete();
        }

        writeLatch.await();
        assertFalse("All writes should succeed", writeErrors.get());

        readDoneLatch.await();
        assertFalse("All reads should succeed", readErrors.get());
        readLatch.await();

        reader.close();
        dlm.close();
    }


    /**
     * Test Case: starting reading when the streams don't exist.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSimpleAsyncReadWriteStartEmpty() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 10;

        DistributedLogManager dlm = createNewDLM(confLocal, name);

        final CountDownLatch readerReadyLatch = new CountDownLatch(1);
        final CountDownLatch readerDoneLatch = new CountDownLatch(1);
        final CountDownLatch readerSyncLatch = new CountDownLatch(numLogSegments * numRecordsPerLogSegment);

        final TestReader reader = new TestReader(
                "test-reader",
                dlm,
                DLSN.InitialDLSN,
                false,
                0,
                readerReadyLatch,
                readerSyncLatch,
                readerDoneLatch);

        reader.start();

        // Increase the probability of reader failure and retry
        Thread.sleep(500);

        final AtomicBoolean writeErrors = new AtomicBoolean(false);
        final CountDownLatch writeLatch = new CountDownLatch(30);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLogSegmentSeqNo = i + 1;
            BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(
                        record, currentLogSegmentSeqNo, currentEntryId, writeLatch, writeErrors, true));
            }
            writer.closeAndComplete();
        }

        writeLatch.await();
        assertFalse("All writes should succeed", writeErrors.get());

        readerDoneLatch.await();
        assertFalse("Should not encounter errors during reading", reader.areErrorsFound());
        readerSyncLatch.await();

        assertTrue("Should position reader at least once", reader.getNumReaderPositions().get() > 1);
        dlm.close();
    }


    /**
     * Test Case: starting reading when the streams don't exist.
     */
    @Test(timeout = 120000)
    public void testSimpleAsyncReadWriteStartEmptyFactory() throws Exception {
        int count = 50;
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 1;

        URI uri = createDLMURI("/" + name);
        ensureURICreated(uri);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(confLocal).uri(uri).build();
        final DistributedLogManager[] dlms = new DistributedLogManager[count];
        final TestReader[] readers = new TestReader[count];
        final CountDownLatch readyLatch = new CountDownLatch(count);
        final CountDownLatch[] syncLatches = new CountDownLatch[count];
        final CountDownLatch[] readerDoneLatches = new CountDownLatch[count];
        for (int s = 0; s < count; s++) {
            dlms[s] = namespace.openLog(name + String.format("%d", s));
            readerDoneLatches[s] = new CountDownLatch(1);
            syncLatches[s] = new CountDownLatch(numLogSegments * numRecordsPerLogSegment);
            readers[s] = new TestReader("reader-" + s,
                    dlms[s], DLSN.InitialDLSN, false, 0, readyLatch, syncLatches[s], readerDoneLatches[s]);
            readers[s].start();
        }

        // wait all readers were positioned at least once
        readyLatch.await();

        final CountDownLatch writeLatch = new CountDownLatch(3 * count);
        final AtomicBoolean writeErrors = new AtomicBoolean(false);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLogSegmentSeqNo = i + 1;
            BKAsyncLogWriter[] writers = new BKAsyncLogWriter[count];
            for (int s = 0; s < count; s++) {
                writers[s] = (BKAsyncLogWriter)(dlms[s].startAsyncLogSegmentNonPartitioned());
            }
            for (long j = 0; j < 1; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                for (int s = 0; s < count; s++) {
                    Future<DLSN> dlsnFuture = writers[s].write(record);
                    dlsnFuture.addEventListener(new WriteFutureEventListener(
                            record, currentLogSegmentSeqNo, currentEntryId, writeLatch, writeErrors, true));
                }
            }
            for (int s = 0; s < count; s++) {
                writers[s].closeAndComplete();
            }
        }

        writeLatch.await();
        assertFalse("All writes should succeed", writeErrors.get());

        for (int s = 0; s < count; s++) {
            readerDoneLatches[s].await();
            assertFalse("Reader " + s + " should not encounter errors", readers[s].areErrorsFound());
            syncLatches[s].await();
            assertEquals(numLogSegments * numRecordsPerLogSegment, readers[s].getNumReads().get());
            assertTrue("Reader " + s + " should position at least once", readers[s].getNumReaderPositions().get() > 0);
        }

        for (int s = 0; s < count; s++) {
            readers[s].stop();
            dlms[s].close();
        }
    }

    @Test(timeout = 300000)
    @DistributedLogAnnotations.FlakyTest
    public void testSimpleAsyncReadWriteSimulateErrors() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        int numLogSegments = 20;
        int numRecordsPerLogSegment = 10;

        final CountDownLatch doneLatch = new CountDownLatch(1);
        final CountDownLatch syncLatch = new CountDownLatch(numLogSegments * numRecordsPerLogSegment);

        TestReader reader = new TestReader(
                "test-reader",
                dlm,
                DLSN.InitialDLSN,
                true,
                0,
                new CountDownLatch(1),
                syncLatch,
                doneLatch);

        reader.start();

        final CountDownLatch writeLatch = new CountDownLatch(200);
        final AtomicBoolean writeErrors = new AtomicBoolean(false);

        int txid = 1;
        for (long i = 0; i < numLogSegments; i++) {
            final long currentLogSegmentSeqNo = i + 1;
            BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < numRecordsPerLogSegment; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(
                        record, currentLogSegmentSeqNo, currentEntryId, writeLatch, writeErrors, true));
            }
            writer.closeAndComplete();
        }

        writeLatch.await();
        assertFalse("All writes should succeed", writeErrors.get());

        doneLatch.await();
        assertFalse("Should not encounter errors during reading", reader.areErrorsFound());
        syncLatch.await();

        assertTrue("Should position reader at least once", reader.getNumReaderPositions().get() > 1);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testSimpleAsyncReadWritePiggyBack() throws Exception {
        String name = runtime.getMethodName();

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setEnableReadAhead(true);
        confLocal.setReadAheadWaitTime(500);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setReadAheadMaxRecords(100);
        confLocal.setOutputBufferSize(1024);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(100);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);

        int numLogSegments = 3;
        int numRecordsPerLogSegment = 10;

        final CountDownLatch readLatch = new CountDownLatch(30);
        final CountDownLatch readDoneLatch = new CountDownLatch(1);
        final AtomicBoolean readErrors = new AtomicBoolean(false);
        final CountDownLatch writeLatch = new CountDownLatch(30);
        final AtomicBoolean writeErrors = new AtomicBoolean(false);

        int txid = 1;
        for (long i = 0; i < numLogSegments; i++) {
            final long currentLogSegmentSeqNo = i + 1;
            BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < numRecordsPerLogSegment; j++) {
                Thread.sleep(50);
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(
                        record, currentLogSegmentSeqNo, j, writeLatch, writeErrors, false));
                if (i == 0 && j == 0) {
                    boolean monotonic = LogSegmentMetadata.supportsSequenceId(confLocal.getDLLedgerMetadataLayoutVersion());
                    TestAsyncReaderWriter.readNext(
                            reader,
                            DLSN.InvalidDLSN,
                            monotonic ? 0L : Long.MIN_VALUE,
                            monotonic,
                            readLatch,
                            readDoneLatch,
                            readErrors);
                }
            }
            writer.closeAndComplete();
        }

        writeLatch.await();
        assertFalse("All writes should succeed", writeErrors.get());

        readDoneLatch.await();
        assertFalse("All reads should succeed", readErrors.get());
        readLatch.await();

        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
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
        final BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

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
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
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

    @Test(timeout = 60000)
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
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
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

    @Test(timeout = 60000)
    public void testAsyncWriteWithMinDelayBetweenFlushesFlushFailure() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setMinDelayBetweenImmediateFlushMs(1);

        URI uri = createDLMURI("/" + name);
        ensureURICreated(uri);

        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(confLocal).uri(uri).clientId("gabbagoo").build();
        DistributedLogManager dlm = namespace.openLog(name);
        DistributedLogNamespace namespace1 = DistributedLogNamespaceBuilder.newBuilder()
                .conf(confLocal).uri(uri).clientId("tortellini").build();
        DistributedLogManager dlm1 = namespace1.openLog(name);

        int txid = 1;
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // First write succeeds since lock isnt checked until transmit, which is scheduled
        Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
        writer.flushAndSyncAll();

        BKLogSegmentWriter perStreamWriter = writer.getCachedLogWriter();
        DistributedLock lock = perStreamWriter.getLock();
        FutureUtils.result(lock.close());

        // Get second writer, steal lock
        BKAsyncLogWriter writer2 = (BKAsyncLogWriter)(dlm1.startAsyncLogSegmentNonPartitioned());

        try {
            // Succeeds, kicks off scheduked flush
            writer.write(DLMTestUtil.getLogRecordInstance(txid++));

            // Succeeds, kicks off scheduled flush
            Thread.sleep(100);
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
            fail("should have thrown");
        } catch (LockingException ex) {
            LOG.debug("caught exception ", ex);
        }

        writer.close();
        dlm.close();
    }

    public void writeRecordsWithOutstandingWriteLimit(int stream, int global, boolean shouldFail) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setPerWriterOutstandingWriteLimit(stream);
        confLocal.setOutstandingWriteLimitDarkmode(false);
        DistributedLogManager dlm;
        if (global > -1) {
            dlm = createNewDLM(confLocal, runtime.getMethodName(),
                    new SimplePermitLimiter(false, global, new NullStatsLogger(), true, new FixedValueFeature("", 0)));
        } else {
            dlm = createNewDLM(confLocal, runtime.getMethodName());
        }
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
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
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
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
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        long txId = 1L;
        for (int i = 0; i < 5; i++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
        }

        BKLogSegmentWriter logWriter = writer.getCachedLogWriter();

        // fence the ledger
        dlm.getWriterBKC().get().openLedger(logWriter.getLogSegmentId(),
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

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
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
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        long txId = 1L;
        for (int i = 0; i < 5; i++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
        }

        BKLogSegmentWriter logWriter = writer.getCachedLogWriter();

        // fence the ledger
        dlm.getWriterBKC().get().openLedger(logWriter.getLogSegmentId(),
                BookKeeper.DigestType.CRC32, confLocal.getBKDigestPW().getBytes(UTF_8));

        try {
            // insert a write to detect the fencing state, to make test more robust.
            writer.write(DLMTestUtil.getLogRecordInstance(txId++));
            writer.closeAndComplete();
            fail("Should fail to complete a log segment when its ledger is fenced");
        } catch (IOException ioe) {
            // expected
            LOG.error("Failed to close and complete log segment {} : ", logWriter.getFullyQualifiedLogSegment(), ioe);
        }

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(1, segments.size());
        assertTrue(segments.get(0).isInProgress());

        dlm.close();
    }

    private void testAsyncReadIdleErrorInternal(String name,
                                                final int idleReaderErrorThreshold,
                                                final boolean heartBeatUsingControlRecs,
                                                final boolean simulateReaderStall) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxRecords(1);
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
                            BKSyncLogWriter writer = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
        BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) dlm.getAsyncLogReader(DLSN.InitialDLSN);
        if (simulateReaderStall) {
            reader.disableProcessingReadRequests();
        }
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

        if (simulateReaderStall) {
            assertTrue(exceptionEncountered);
        } else if (heartBeatUsingControlRecs) {
            assertFalse(exceptionEncountered);
            Assert.assertEquals(segmentSize * numSegments, recordCount);
        } else {
            assertTrue(exceptionEncountered);
            Assert.assertEquals(segmentSize, recordCount);
        }
        assertFalse(currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void testAsyncReadIdleControlRecord() throws Exception {
        String name = "distrlog-async-reader-idle-error-control";
        testAsyncReadIdleErrorInternal(name, 500, true, false);
    }

    @Test(timeout = 10000)
    public void testAsyncReadIdleError() throws Exception {
        String name = "distrlog-async-reader-idle-error";
        testAsyncReadIdleErrorInternal(name, 1000, false, false);
    }

    @Test(timeout = 10000)
    public void testAsyncReadIdleError2() throws Exception {
        String name = "distrlog-async-reader-idle-error-2";
        testAsyncReadIdleErrorInternal(name, 1000, true, true);
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
        BKAsyncLogWriter writer =
                (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

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

        writer = (BKAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
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
        confLocal.setReadAheadMaxRecords(1);
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
                                BKSyncLogWriter writer = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
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

    @Test(timeout = 60000)
    public void testGetLastTxId() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();

        int numRecords = 10;
        for (int i = 0; i < numRecords; i++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(i)));
            assertEquals("last tx id should become " + i,
                    i, writer.getLastTxId());
        }
        // open a writer to recover the inprogress log segment
        AsyncLogWriter recoverWriter = dlm.startAsyncLogSegmentNonPartitioned();
        assertEquals("recovered last tx id should be " + (numRecords - 1),
                numRecords - 1, recoverWriter.getLastTxId());
    }

    @Test(timeout = 60000)
    public void testMaxReadAheadRecords() throws Exception {
        int maxRecords = 1;
        int batchSize = 8;
        int maxAllowedCachedRecords = maxRecords + batchSize - 1;

        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(Integer.MAX_VALUE);
        confLocal.setReadAheadMaxRecords(maxRecords);
        confLocal.setReadAheadBatchSize(batchSize);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();

        int numRecords = 40;
        for (int i = 1; i <= numRecords; i++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(i)));
            assertEquals("last tx id should become " + i,
                    i, writer.getLastTxId());
        }
        LogRecord record = DLMTestUtil.getLogRecordInstance(numRecords);
        record.setControl();
        Await.result(writer.write(record));

        BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) dlm.getAsyncLogReader(DLSN.InitialDLSN);
        record = Await.result(reader.readNext());
        LOG.info("Read record {}", record);
        assertEquals(1L, record.getTransactionId());

        assertNotNull(reader.bkLedgerManager.readAheadWorker);
        assertTrue(reader.bkLedgerManager.readAheadCache.getNumCachedRecords() <= maxAllowedCachedRecords);

        for (int i = 2; i <= numRecords; i++) {
            record = Await.result(reader.readNext());
            LOG.info("Read record {}", record);
            assertEquals((long) i, record.getTransactionId());
            TimeUnit.MILLISECONDS.sleep(20);
            int numCachedRecords = reader.bkLedgerManager.readAheadCache.getNumCachedRecords();
            assertTrue("Should cache less than " + batchSize + " records but already found "
                    + numCachedRecords + " records when reading " + i + "th record",
                    numCachedRecords <= maxAllowedCachedRecords);
        }
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStream() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        final int NUM_RECORDS = 10;
        int i = 1;
        for (; i <= NUM_RECORDS; i++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(i)));
            assertEquals("last tx id should become " + i,
                    i, writer.getLastTxId());
        }

        Await.result(writer.markEndOfStream());

        // Multiple end of streams are ok.
        Await.result(writer.markEndOfStream());

        try {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(i)));
            fail("Should have thrown");
        } catch (EndOfStreamException ex) {
        }

        BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) dlm.getAsyncLogReader(DLSN.InitialDLSN);
        LogRecord record = null;
        for (int j = 0; j < NUM_RECORDS; j++) {
            record = Await.result(reader.readNext());
            assertEquals(j+1, record.getTransactionId());
        }

        try {
            record = Await.result(reader.readNext());
            fail("Should have thrown");
        } catch (EndOfStreamException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStreamAtBeginningOfSegment() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        Await.result(writer.markEndOfStream());
        try {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(1)));
            fail("Should have thrown");
        } catch (EndOfStreamException ex) {
        }

        BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) dlm.getAsyncLogReader(DLSN.InitialDLSN);
        try {
            LogRecord record = Await.result(reader.readNext());
            fail("Should have thrown");
        } catch (EndOfStreamException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testBulkReadWaitingMoreRecords() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
        LogRecord controlRecord = DLMTestUtil.getLogRecordInstance(1L);
        controlRecord.setControl();
        FutureUtils.result(writer.write(controlRecord));

        BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) dlm.getAsyncLogReader(DLSN.InitialDLSN);
        Future<List<LogRecordWithDLSN>> bulkReadFuture = reader.readBulk(2, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        Future<LogRecordWithDLSN> readFuture = reader.readNext();

        // write another records
        for (int i = 0; i < 5; i++) {
            long txid = 2L + i;
            FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(txid)));
            controlRecord = DLMTestUtil.getLogRecordInstance(txid);
            controlRecord.setControl();
            FutureUtils.result(writer.write(controlRecord));
        }

        List<LogRecordWithDLSN> bulkReadRecords = FutureUtils.result(bulkReadFuture);
        assertEquals(2, bulkReadRecords.size());
        assertEquals(1L, bulkReadRecords.get(0).getTransactionId());
        assertEquals(2L, bulkReadRecords.get(1).getTransactionId());
        for (LogRecordWithDLSN record : bulkReadRecords) {
            DLMTestUtil.verifyLogRecord(record);
        }
        LogRecordWithDLSN record = FutureUtils.result(readFuture);
        assertEquals(3L, record.getTransactionId());
        DLMTestUtil.verifyLogRecord(record);

        reader.close();
        writer.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testBulkReadNotWaitingMoreRecords() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
        LogRecord controlRecord = DLMTestUtil.getLogRecordInstance(1L);
        controlRecord.setControl();
        FutureUtils.result(writer.write(controlRecord));

        BKAsyncLogReaderDLSN reader = (BKAsyncLogReaderDLSN) dlm.getAsyncLogReader(DLSN.InitialDLSN);
        Future<List<LogRecordWithDLSN>> bulkReadFuture = reader.readBulk(2, 0, TimeUnit.MILLISECONDS);
        Future<LogRecordWithDLSN> readFuture = reader.readNext();

        List<LogRecordWithDLSN> bulkReadRecords = FutureUtils.result(bulkReadFuture);
        assertEquals(1, bulkReadRecords.size());
        assertEquals(1L, bulkReadRecords.get(0).getTransactionId());
        for (LogRecordWithDLSN record : bulkReadRecords) {
            DLMTestUtil.verifyLogRecord(record);
        }

        // write another records
        for (int i = 0; i < 5; i++) {
            long txid = 2L + i;
            FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(txid)));
            controlRecord = DLMTestUtil.getLogRecordInstance(txid);
            controlRecord.setControl();
            FutureUtils.result(writer.write(controlRecord));
        }

        LogRecordWithDLSN record = FutureUtils.result(readFuture);
        assertEquals(2L, record.getTransactionId());
        DLMTestUtil.verifyLogRecord(record);

        reader.close();
        writer.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testCreateLogStreamWithDifferentReplicationFactor() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(testConf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);

        ConcurrentBaseConfiguration baseConf = new ConcurrentConstConfiguration(confLocal);
        DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(baseConf);
        dynConf.setProperty(DistributedLogConfiguration.BKDL_BOOKKEEPER_ENSEMBLE_SIZE,
                DistributedLogConfiguration.BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT - 1);

        URI uri = createDLMURI("/" + name);
        ensureURICreated(uri);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(confLocal).uri(uri).build();

        // use the pool
        DistributedLogManager dlm = namespace.openLog(name + "-pool");
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(1, segments.size());
        long ledgerId = segments.get(0).getLedgerId();
        LedgerHandle lh = ((BKDistributedLogNamespace) namespace).getReaderBKC()
                .get().openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32, confLocal.getBKDigestPW().getBytes(UTF_8));
        LedgerMetadata metadata = BookKeeperAccessor.getLedgerMetadata(lh);
        assertEquals(DistributedLogConfiguration.BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT, metadata.getEnsembleSize());
        lh.close();
        writer.close();
        dlm.close();

        // use customized configuration
        dlm = namespace.openLog(
                name + "-custom",
                Optional.<DistributedLogConfiguration>absent(),
                Optional.of(dynConf));
        writer = dlm.startAsyncLogSegmentNonPartitioned();
        FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
        segments = dlm.getLogSegments();
        assertEquals(1, segments.size());
        ledgerId = segments.get(0).getLedgerId();
        lh = ((BKDistributedLogNamespace) namespace).getReaderBKC()
                .get().openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32, confLocal.getBKDigestPW().getBytes(UTF_8));
        metadata = BookKeeperAccessor.getLedgerMetadata(lh);
        assertEquals(DistributedLogConfiguration.BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT - 1, metadata.getEnsembleSize());
        lh.close();
        writer.close();
        dlm.close();
        namespace.close();
    }
}
