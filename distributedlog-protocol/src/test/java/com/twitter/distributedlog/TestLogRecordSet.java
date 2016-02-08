package com.twitter.distributedlog;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.LogRecordSet.Reader;
import com.twitter.distributedlog.LogRecordSet.Writer;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.LogRecordSet.MAX_LOGRECORD_SIZE;
import static org.junit.Assert.*;

/**
 * Test Case of {@link LogRecordSet}
 */
public class TestLogRecordSet {

    @Test(timeout = 20000)
    public void testEmptyRecordSet() throws Exception {
        Writer writer = LogRecordSet.newLogRecordSet(
                "test-empty-record-set",
                1024,
                true,
                CompressionCodec.Type.NONE,
                NullStatsLogger.INSTANCE);
        assertEquals("zero bytes", 0, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        Buffer buffer = writer.getBuffer();
        LogRecordSet recordSet = LogRecordSet.newBuilder()
                .setData(buffer.getData(), 0, buffer.size())
                .setLogSegmentInfo(1L, 0L)
                .setEntryId(0L)
                .build();
        Reader reader = recordSet.reader();
        assertNull("Empty record set should return null",
                reader.nextRecord());
    }

    @Test(timeout = 20000)
    public void testWriteTooLongRecord() throws Exception {
        Writer writer = LogRecordSet.newLogRecordSet(
                "test-write-too-long-record",
                1024,
                false,
                CompressionCodec.Type.NONE,
                NullStatsLogger.INSTANCE);
        assertEquals("zero bytes", 0, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        LogRecord largeRecord = new LogRecord(1L, new byte[MAX_LOGRECORD_SIZE + 1]);
        try {
            writer.writeRecord(largeRecord, new Promise<DLSN>());
            fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("zero bytes", 0, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        Buffer buffer = writer.getBuffer();
        assertEquals("zero bytes", 0, buffer.size());
    }

    @Test(timeout = 20000)
    public void testWriteRecords() throws Exception {
        Writer writer = LogRecordSet.newLogRecordSet(
                "test-write-records",
                1024,
                true,
                CompressionCodec.Type.NONE,
                NullStatsLogger.INSTANCE);
        assertEquals("zero bytes", 0, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        List<Future<DLSN>> writePromiseList = Lists.newArrayList();
        // write first 5 records
        for (int i = 0; i < 5; i++) {
            LogRecord record = new LogRecord(i, ("record-" + i).getBytes(UTF_8));
            record.setPositionWithinLogSegment(i);
            Promise<DLSN> writePromise = new Promise<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 1) + " records", (i + 1), writer.getNumRecords());
        }

        // write large record
        LogRecord largeRecord = new LogRecord(1L, new byte[MAX_LOGRECORD_SIZE + 1]);
        try {
            writer.writeRecord(largeRecord, new Promise<DLSN>());
            fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("5 records", 5, writer.getNumRecords());

        // write another 5 records
        for (int i = 0; i < 5; i++) {
            LogRecord record = new LogRecord(i + 5, ("record-" + (i + 5)).getBytes(UTF_8));
            record.setPositionWithinLogSegment(i + 5);
            Promise<DLSN> writePromise = new Promise<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 6) + " records", (i + 6), writer.getNumRecords());
        }

        Buffer buffer = writer.getBuffer();

        // Test transmit complete
        writer.completeTransmit(1L, 1L);
        List<DLSN> writeResults = Await.result(Future.collect(writePromiseList));
        for (int i = 0; i < 10; i++) {
            assertEquals(new DLSN(1L, 1L, i), writeResults.get(i));
        }

        // Test reading from buffer
        LogRecordSet recordSet = LogRecordSet.newBuilder()
                .setData(buffer.getData(), 0, buffer.size())
                .setLogSegmentInfo(1L, 1L)
                .setEntryId(0L)
                .build();
        Reader reader = recordSet.reader();
        LogRecordWithDLSN record = reader.nextRecord();
        int numReads = 0;
        long expectedTxid = 0L;
        while (null != record) {
            assertEquals(expectedTxid, record.getTransactionId());
            assertEquals(expectedTxid, record.getSequenceId());
            assertEquals(new DLSN(1L, 0L, expectedTxid), record.getDlsn());
            ++numReads;
            ++expectedTxid;
            record = reader.nextRecord();
        }
        assertEquals(10, numReads);
    }

}
