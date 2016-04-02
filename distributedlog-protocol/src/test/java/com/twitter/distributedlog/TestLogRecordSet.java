package com.twitter.distributedlog;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.LogRecordSet.Reader;
import com.twitter.distributedlog.LogRecordSet.Writer;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.io.CompressionCodec.Type;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.LogRecord.*;
import static com.twitter.distributedlog.LogRecordSet.*;
import static org.junit.Assert.*;

/**
 * Test Case for {@link LogRecordSet}
 */
public class TestLogRecordSet {

    @Test(timeout = 60000)
    public void testEmptyRecordSet() throws Exception {
        Writer writer = LogRecordSet.newWriter(1024, Type.NONE);
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        ByteBuffer buffer = writer.getBuffer();
        assertEquals("zero user bytes", HEADER_LEN, buffer.remaining());

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        LogRecordWithDLSN record = new LogRecordWithDLSN(
                new DLSN(1L, 0L, 0L),
                1L,
                data,
                1L);
        record.setRecordSet();
        Reader reader = LogRecordSet.of(record);
        assertNull("Empty record set should return null",
                reader.nextRecord());
    }

    @Test(timeout = 60000)
    public void testWriteTooLongRecord() throws Exception {
        Writer writer = LogRecordSet.newWriter(1024, Type.NONE);
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        ByteBuffer dataBuf = ByteBuffer.allocate(MAX_LOGRECORD_SIZE + 1);
        try {
            writer.writeRecord(dataBuf, new Promise<DLSN>());
            fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());
        ByteBuffer buffer = writer.getBuffer();
        assertEquals("zero user bytes", HEADER_LEN, buffer.remaining());

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        LogRecordWithDLSN record = new LogRecordWithDLSN(
                new DLSN(1L, 0L, 0L),
                1L,
                data,
                1L);
        record.setRecordSet();
        Reader reader = LogRecordSet.of(record);
        assertNull("Empty record set should return null",
                reader.nextRecord());
    }

    @Test(timeout = 20000)
    public void testWriteRecordsNoneCompressed() throws Exception {
        testWriteRecords(Type.NONE);
    }

    @Test(timeout = 20000)
    public void testWriteRecordsLZ4Compressed() throws Exception {
        testWriteRecords(Type.LZ4);
    }

    void testWriteRecords(Type codec) throws Exception {
        Writer writer = LogRecordSet.newWriter(1024, codec);
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        List<Future<DLSN>> writePromiseList = Lists.newArrayList();
        /// write first 5 records
        for (int i = 0; i < 5; i++) {
            ByteBuffer record = ByteBuffer.wrap(("record-" + i).getBytes(UTF_8));
            Promise<DLSN> writePromise = new Promise<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 1) + " records", (i + 1), writer.getNumRecords());
        }
        ByteBuffer dataBuf = ByteBuffer.allocate(MAX_LOGRECORD_SIZE + 1);
        try {
            writer.writeRecord(dataBuf, new Promise<DLSN>());
            fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("5 records", 5, writer.getNumRecords());

        /// write another 5 records
        for (int i = 0; i < 5; i++) {
            ByteBuffer record = ByteBuffer.wrap(("record-" + (i + 5)).getBytes(UTF_8));
            Promise<DLSN> writePromise = new Promise<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 6) + " records", (i + 6), writer.getNumRecords());
        }

        ByteBuffer buffer = writer.getBuffer();
        assertEquals("10 records", 10, writer.getNumRecords());

        // Test transmit complete
        writer.completeTransmit(1L, 1L, 10L);
        List<DLSN> writeResults = Await.result(Future.collect(writePromiseList));
        for (int i = 0; i < 10; i++) {
            assertEquals(new DLSN(1L, 1L, 10L + i), writeResults.get(i));
        }

        // Test reading from buffer
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        LogRecordWithDLSN record = new LogRecordWithDLSN(
                new DLSN(1L, 1L, 10L),
                99L,
                data,
                999L);
        record.setPositionWithinLogSegment(888);
        record.setRecordSet();
        Reader reader = LogRecordSet.of(record);
        LogRecordWithDLSN readRecord = reader.nextRecord();
        int numReads = 0;
        while (null != readRecord) {
            assertEquals(new DLSN(1L, 1L, 10L + numReads), readRecord.getDlsn());
            assertEquals(99L, readRecord.getTransactionId());
            assertEquals(888 + numReads, readRecord.getPositionWithinLogSegment());
            assertEquals(999L, readRecord.getStartSequenceIdOfCurrentSegment());
            assertEquals(999L + 888 + numReads - 1, readRecord.getSequenceId());
            // read next
            ++numReads;
            readRecord = reader.nextRecord();
        }
        assertEquals(10, numReads);
    }

}
