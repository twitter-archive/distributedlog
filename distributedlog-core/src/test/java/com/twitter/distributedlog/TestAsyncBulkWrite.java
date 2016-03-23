package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.twitter.distributedlog.DLMTestUtil.validateFutureFailed;
import static com.twitter.distributedlog.DLMTestUtil.validateFutureSucceededAndGetResult;
import static com.twitter.distributedlog.LogRecordSet.MAX_LOGRECORD_SIZE;
import static com.twitter.distributedlog.LogRecordSet.MAX_LOGRECORDSET_SIZE;
import static org.junit.Assert.*;

/**
 * Test cases for bulk writes.
 */
public class TestAsyncBulkWrite extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestAsyncBulkWrite.class);

    @Rule
    public TestName runtime = new TestName();

    protected final DistributedLogConfiguration testConf;

    public TestAsyncBulkWrite() {
        this.testConf = new DistributedLogConfiguration();
        this.testConf.addConfiguration(conf);
        this.testConf.setReaderIdleErrorThresholdMillis(1200000);
    }

    /**
     * Test Case for partial failure in a bulk write.
     *
     * Write a batch: 10 good records + 1 too large record + 10 good records.
     *
     * Expected: first 10 good records succeed, the too-large-record will be rejected, while
     *           the last 10 good records will be cancelled because their previous write is rejected.
     */
    @Test(timeout = 60000)
    public void testAsyncBulkWritePartialFailureBufferFailure() throws Exception {
        String name = "distrlog-testAsyncBulkWritePartialFailure";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        final int goodRecs = 10;

        // Generate records: 10 good records, 1 too large record, 10 good records
        final List<LogRecord> records = DLMTestUtil.getLargeLogRecordInstanceList(1, goodRecs);
        records.add(DLMTestUtil.getLogRecordInstance(goodRecs, MAX_LOGRECORD_SIZE + 1));
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

    /**
     * Test Case for a total failure in a bulk write.
     *
     * Write 100 records as a batch. Inject failure on transmit and all records should be failed.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAsyncBulkWriteTotalFailureTransmitFailure() throws Exception {
        String name = "distrlog-testAsyncBulkWriteTotalFailureDueToTransmitFailure";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        final int batchSize = 100;
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

    /**
     * Test Case: There is no log segment rolling when there is partial failure in async bulk write.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAsyncBulkWriteNoLedgerRollWithPartialFailures() throws Exception {
        String name = "distrlog-testAsyncBulkWriteNoLedgerRollWithPartialFailures";
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
        DLSN dlsn = validateFutureSucceededAndGetResult(result);
        assertEquals(1, dlsn.getLogSegmentSequenceNo());

        // Write two more via bulk. Ledger doesn't roll because there's a partial failure.
        List<LogRecord> records = null;
        Future<List<Future<DLSN>>> futureResults = null;
        List<Future<DLSN>> results = null;
        records = new ArrayList<LogRecord>(2);
        records.add(DLMTestUtil.getLogRecordInstance(txid++, 2048));
        records.add(DLMTestUtil.getLogRecordInstance(txid++, MAX_LOGRECORD_SIZE + 1));
        futureResults = writer.writeBulk(records);
        results = validateFutureSucceededAndGetResult(futureResults);
        result = results.get(0);
        dlsn = validateFutureSucceededAndGetResult(result);
        assertEquals(1, dlsn.getLogSegmentSequenceNo());

        // Now writer is in a bad state.
        records = new ArrayList<LogRecord>(1);
        records.add(DLMTestUtil.getLogRecordInstance(txid++, 2048));
        futureResults = writer.writeBulk(records);
        validateFutureFailed(futureResults, WriteException.class);

        writer.closeAndComplete();
        dlm.close();
    }

    /**
     * Test Case: A large write batch will span records into multiple entries and ledgers.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSimpleAsyncBulkWriteSpanningEntryAndLedger() throws Exception {
        String name = "distrlog-testSimpleAsyncBulkWriteSpanningEntryAndLedger";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

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
        writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);

        writer.closeAndComplete();
        dlm.close();
    }

    /**
     * Test Case: A large write batch will span multiple packets.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAsyncBulkWriteSpanningPackets() throws Exception {
        String name = "distrlog-testAsyncBulkWriteSpanningPackets";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // First entry.
        int numTransmissions = 4;
        int recSize = 10*1024;
        int batchSize = (numTransmissions*MAX_LOGRECORDSET_SIZE+1)/recSize;
        long ledgerIndex = 1;
        long entryIndex = 0;
        long slotIndex = 0;
        long txIndex = 1;
        DLSN dlsn = checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);
        assertEquals(4, dlsn.getEntryId());
        assertEquals(1, dlsn.getLogSegmentSequenceNo());

        writer.closeAndComplete();
        dlm.close();
    }

    /**
     * Test Case: Test Transmit Failures when a large write batch spans multiple packets.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAsyncBulkWriteSpanningPacketsWithTransmitFailure() throws Exception {
        String name = "distrlog-testAsyncBulkWriteSpanningPacketsWithTransmitFailure";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(testConf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());

        // First entry.
        int numTransmissions = 4;
        int recSize = 10*1024;
        int batchSize = (numTransmissions*MAX_LOGRECORDSET_SIZE+1)/recSize;
        long ledgerIndex = 1;
        long entryIndex = 0;
        long slotIndex = 0;
        long txIndex = 1;

        DLSN dlsn = checkAllSucceeded(writer, batchSize, recSize, ledgerIndex, entryIndex, slotIndex, txIndex);
        assertEquals(4, dlsn.getEntryId());
        assertEquals(1, dlsn.getLogSegmentSequenceNo());

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

    private DLSN checkAllSucceeded(BKAsyncLogWriter writer,
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
            assertEquals(ledgerIndex, dlsn.getLogSegmentSequenceNo());
            assertEquals(slotIndex, dlsn.getSlotId());
            slotIndex++;
            prevEntryId = dlsn.getEntryId();
        }
        return lastDlsn;
    }

    private void checkAllSubmittedButFailed(BKAsyncLogWriter writer,
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
}

