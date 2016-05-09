/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.Entry.Reader;
import com.twitter.distributedlog.Entry.Writer;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.io.Buf;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static org.junit.Assert.*;

/**
 * Test Case of {@link Entry}
 */
public class TestEntry {

    @Test(timeout = 20000)
    public void testEmptyRecordSet() throws Exception {
        Writer writer = Entry.newEntry(
                "test-empty-record-set",
                1024,
                true,
                CompressionCodec.Type.NONE,
                NullStatsLogger.INSTANCE);
        assertEquals("zero bytes", 0, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        Buffer buffer = writer.getBuffer();
        Entry recordSet = Entry.newBuilder()
                .setData(buffer.getData(), 0, buffer.size())
                .setLogSegmentInfo(1L, 0L)
                .setEntryId(0L)
                .build();
        Reader reader = recordSet.reader();
        Assert.assertNull("Empty record set should return null",
                reader.nextRecord());
    }

    @Test(timeout = 20000)
    public void testWriteTooLongRecord() throws Exception {
        Writer writer = Entry.newEntry(
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
            Assert.fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("zero bytes", 0, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        Buffer buffer = writer.getBuffer();
        Assert.assertEquals("zero bytes", 0, buffer.size());
    }

    @Test(timeout = 20000)
    public void testWriteRecords() throws Exception {
        Writer writer = Entry.newEntry(
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
            Assert.fail("Should fail on writing large record");
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
            Assert.assertEquals(new DLSN(1L, 1L, i), writeResults.get(i));
        }

        // Test reading from buffer
        Entry recordSet = Entry.newBuilder()
                .setData(buffer.getData(), 0, buffer.size())
                .setLogSegmentInfo(1L, 1L)
                .setEntryId(0L)
                .build();
        Reader reader = recordSet.reader();
        LogRecordWithDLSN record = reader.nextRecord();
        int numReads = 0;
        long expectedTxid = 0L;
        while (null != record) {
            Assert.assertEquals(expectedTxid, record.getTransactionId());
            Assert.assertEquals(expectedTxid, record.getSequenceId());
            Assert.assertEquals(new DLSN(1L, 0L, expectedTxid), record.getDlsn());
            ++numReads;
            ++expectedTxid;
            record = reader.nextRecord();
        }
        Assert.assertEquals(10, numReads);
    }

    @Test(timeout = 20000)
    public void testWriteRecordSet() throws Exception {
        Writer writer = Entry.newEntry(
                "test-write-recordset",
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

        final LogRecordSet.Writer recordSetWriter = LogRecordSet.newWriter(1024, CompressionCodec.Type.NONE);
        List<Future<DLSN>> recordSetPromiseList = Lists.newArrayList();
        // write another 5 records as a batch
        for (int i = 0; i < 5; i++) {
            ByteBuffer record = ByteBuffer.wrap(("record-" + (i + 5)).getBytes(UTF_8));
            Promise<DLSN> writePromise = new Promise<DLSN>();
            recordSetWriter.writeRecord(record, writePromise);
            recordSetPromiseList.add(writePromise);
            assertEquals((i + 1) + " records", (i + 1), recordSetWriter.getNumRecords());
        }
        final ByteBuffer recordSetBuffer = recordSetWriter.getBuffer();
        byte[] data = new byte[recordSetBuffer.remaining()];
        recordSetBuffer.get(data);
        LogRecord setRecord = new LogRecord(5L, data);
        setRecord.setPositionWithinLogSegment(5);
        setRecord.setRecordSet();
        Promise<DLSN> writePromise = new Promise<DLSN>();
        writePromise.addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN dlsn) {
                recordSetWriter.completeTransmit(
                        dlsn.getLogSegmentSequenceNo(),
                        dlsn.getEntryId(),
                        dlsn.getSlotId());
            }

            @Override
            public void onFailure(Throwable cause) {
                recordSetWriter.abortTransmit(cause);
            }
        });
        writer.writeRecord(setRecord, writePromise);
        writePromiseList.add(writePromise);

        // write last 5 records
        for (int i = 0; i < 5; i++) {
            LogRecord record = new LogRecord(i + 10, ("record-" + (i + 10)).getBytes(UTF_8));
            record.setPositionWithinLogSegment(i + 10);
            writePromise = new Promise<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 11) + " records", (i + 11), writer.getNumRecords());
        }

        Buffer buffer = writer.getBuffer();

        // Test transmit complete
        writer.completeTransmit(1L, 1L);
        List<DLSN> writeResults = Await.result(Future.collect(writePromiseList));
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(new DLSN(1L, 1L, i), writeResults.get(i));
        }
        Assert.assertEquals(new DLSN(1L, 1L, 5), writeResults.get(5));
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(new DLSN(1L, 1L, (10 + i)), writeResults.get(6 + i));
        }
        List<DLSN> recordSetWriteResults = Await.result(Future.collect(recordSetPromiseList));
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(new DLSN(1L, 1L, (5 + i)), recordSetWriteResults.get(i));
        }

        // Test reading from buffer
        verifyReadResult(buffer, 1L, 1L, 1L, true,
                new DLSN(1L, 1L, 2L), 3, 5, 5,
                new DLSN(1L, 1L, 2L), 2L);
        verifyReadResult(buffer, 1L, 1L, 1L, true,
                new DLSN(1L, 1L, 7L), 0, 3, 5,
                new DLSN(1L, 1L, 7L), 7L);
        verifyReadResult(buffer, 1L, 1L, 1L, true,
                new DLSN(1L, 1L, 12L), 0, 0, 3,
                new DLSN(1L, 1L, 12L), 12L);
        verifyReadResult(buffer, 1L, 1L, 1L, false,
                new DLSN(1L, 1L, 2L), 3, 5, 5,
                new DLSN(1L, 1L, 2L), 2L);
        verifyReadResult(buffer, 1L, 1L, 1L, false,
                new DLSN(1L, 1L, 7L), 0, 3, 5,
                new DLSN(1L, 1L, 7L), 7L);
        verifyReadResult(buffer, 1L, 1L, 1L, false,
                new DLSN(1L, 1L, 12L), 0, 0, 3,
                new DLSN(1L, 1L, 12L), 12L);
    }

    void verifyReadResult(Buffer data,
                          long lssn, long entryId, long startSequenceId,
                          boolean deserializeRecordSet,
                          DLSN skipTo,
                          int firstNumRecords,
                          int secondNumRecords,
                          int lastNumRecords,
                          DLSN expectedDLSN,
                          long expectedTxId) throws Exception {
        Entry recordSet = Entry.newBuilder()
                .setData(data.getData(), 0, data.size())
                .setLogSegmentInfo(lssn, startSequenceId)
                .setEntryId(entryId)
                .deserializeRecordSet(deserializeRecordSet)
                .skipTo(skipTo)
                .build();
        Reader reader = recordSet.reader();

        LogRecordWithDLSN record;
        for (int i = 0; i < firstNumRecords; i++) { // first
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals(expectedDLSN, record.getDlsn());
            assertEquals(expectedTxId, record.getTransactionId());
            assertNotNull("record " + record + " payload is null",
                    record.getPayload());
            assertEquals("record-" + expectedTxId, new String(record.getPayload(), UTF_8));
            expectedDLSN = expectedDLSN.getNextDLSN();
            ++expectedTxId;
        }

        boolean verifyDeserializedRecords = true;
        if (firstNumRecords > 0) {
            verifyDeserializedRecords = deserializeRecordSet;
        }
        if (verifyDeserializedRecords) {
            long txIdOfRecordSet = 5;
            for (int i = 0; i < secondNumRecords; i++) {
                record = reader.nextRecord();
                assertNotNull(record);
                assertEquals(expectedDLSN, record.getDlsn());
                assertEquals(txIdOfRecordSet, record.getTransactionId());
                assertNotNull("record " + record + " payload is null",
                        record.getPayload());
                assertEquals("record-" + expectedTxId, new String(record.getPayload(), UTF_8));
                expectedDLSN = expectedDLSN.getNextDLSN();
                ++expectedTxId;
            }
        } else {
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals(expectedDLSN, record.getDlsn());
            assertEquals(expectedTxId, record.getTransactionId());
            for (int i = 0; i < secondNumRecords; i++) {
                expectedDLSN = expectedDLSN.getNextDLSN();
                ++expectedTxId;
            }
        }

        for (int i = 0; i < lastNumRecords; i++) {
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals(expectedDLSN, record.getDlsn());
            assertEquals(expectedTxId, record.getTransactionId());
            assertNotNull("record " + record + " payload is null",
                    record.getPayload());
            assertEquals("record-" + expectedTxId, new String(record.getPayload(), UTF_8));
            expectedDLSN = expectedDLSN.getNextDLSN();
            ++expectedTxId;
        }

    }


}
