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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper classes for reading the ops from an InputStream.
 * All ops derive from LogRecord and are only
 * instantiated from Reader#readOp()

 * Data type in brackets. Interpretation should be on the basis of data types and not individual
 * bytes to honor Endianness.
 *
 * LogRecord structure:
 * -------------------
 * Bytes 0 - 7                      : Metadata (Long)
 * Bytes 8 - 15                     : TxId (Long)
 * Bytes 16 - 19                    : Payload length (Integer)
 * Bytes 20 - 20+payload.length-1   : Payload (Byte[])
 *
 * Metadata: 8 Bytes (Long)
 * --------
 *
 * 0x 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
 *            |_____________| |_____|
 *                   |           |
 *               position      flags
 *
 * Flags: 2 Bytes (least significant)
 * -----
 * Bit  0      : If set, control record, else record with payload.
 * Bit  1      : If set, end of stream.
 * Bits 2 - 15 : Unused
 */
public class LogRecord {
    static final Logger LOG = LoggerFactory.getLogger(LogRecord.class);

    private long metadata;
    private long txid;
    private byte[] payload;

    static final long LOGRECORD_METADATA_FLAGS_MASK = 0xffff;
    static final long LOGRECORD_METADATA_POSITION_MASK = 0x0000ffffffff0000L;
    static final int LOGRECORD_METADATA_POSITION_SHIFT = 16;
    static final long LOGRECORD_METADATA_UNUSED_MASK = 0xffff000000000000L;


    // TODO: Replace with EnumSet
    static final long LOGRECORD_FLAGS_CONTROL_MESSAGE = 0x1;
    static final long LOGRECORD_FLAGS_END_OF_STREAM = 0x2;
    /**
     * This empty constructor can only be called from Reader#readOp.
     */
    protected LogRecord() {
        this.txid = 0;
        this.metadata = 0;
    }

    public LogRecord(long txid, byte[] payload) {
        this.txid = txid;
        this.payload = payload;
        this.metadata = 0;
    }

    public long getTransactionId() {
        return txid;
    }

    void setPositionWithinLogSegment(int positionWithinLogSegment) {
        assert(positionWithinLogSegment >= 0);
        metadata = metadata | (((long) positionWithinLogSegment) << LOGRECORD_METADATA_POSITION_SHIFT);
    }

    int getPositionWithinLogSegment() {
        long ret = (metadata & LOGRECORD_METADATA_POSITION_MASK) >> LOGRECORD_METADATA_POSITION_SHIFT;
        if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                (ret + " position should never exceed max integer value");
        }
        return (int) ret;
    }


    void setControl() {
        metadata = metadata | LOGRECORD_FLAGS_CONTROL_MESSAGE;
    }

    public boolean isControl() {
        return ((metadata & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    public static boolean isControl(long flags) {
        return ((flags & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    void setEndOfStream() {
        metadata = metadata | LOGRECORD_FLAGS_END_OF_STREAM;
    }

    boolean isEndOfStream() {
        return ((metadata & LOGRECORD_FLAGS_END_OF_STREAM) != 0);
    }

    public static boolean isEndOfStream(long flags) {
        return ((flags & LOGRECORD_FLAGS_END_OF_STREAM) != 0);
    }


    public byte[] getPayload() {
        return payload;
    }

    public InputStream getPayLoadInputStream() {
        return new ByteArrayInputStream(payload);
    }

    protected void readPayload(DataInputStream in, int logVersion) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new EOFException("Log Record is corrupt: Negative length " + length);
        }
        payload = new byte[length];
        in.readFully(payload);
    }

    private void writePayload(DataOutputStream out) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
    }

    protected void setTransactionId(long txid) {
        this.txid = txid;
    }

    protected void setMetadata(long metadata) {
        this.metadata = metadata;
    }

    private void writeToStream(DataOutputStream out) throws IOException {
        out.writeLong(metadata);
        out.writeLong(txid);
        writePayload(out);
    }

    /**
     * The size of the serialized log record, this is used to estimate how much will
     * be be appended to the in-memory buffer
     *
     * @return serialized size
     */
    int getPersistentSize() {
        // Flags + TxId + Payload-length + payload
        return 2 * (Long.SIZE / 8) + Integer.SIZE / 8 + payload.length;
    }

    /**
     * Class for writing log records
     */
    public static class Writer {
        private final DataOutputStream buf;

        public Writer(DataOutputStream out) {
            this.buf = out;
        }

        /**
         * Write an operation to the output stream
         *
         * @param record The operation to write
         * @throws IOException if an error occurs during writing.
         */
        public void writeOp(LogRecord record) throws IOException {
            record.writeToStream(buf);
        }

        public int getPendingBytes() {
            return buf.size();
        }
    }

    /**
     * This class is a package private class for reading log records
     * from the persistent
      */
    public static class Reader {
        private final RecordStream recordStream;
        private final DataInputStream in;
        private final int logVersion;
        private final long startSequenceId;
        private static final int SKIP_BUFFER_SIZE = 512;

        /**
         * Construct the reader
         *
         * @param in The stream to read from.
         */
        public Reader(RecordStream recordStream,
                      DataInputStream in,
                      int logVersion,
                      long startSequenceId) {
            this.logVersion = logVersion;
            this.recordStream = recordStream;
            this.in = in;
            this.startSequenceId = startSequenceId;
        }

        /**
         * Read an operation from the input stream.
         * <p/>
         * Note that the objects returned from this method may be re-used by future
         * calls to the same method.
         *
         * @return the operation read from the stream, or null at the end of the file
         * @throws IOException on error.
         */
        public LogRecordWithDLSN readOp() throws IOException {
            try {
                long metadata = in.readLong();
                // Reading the first 8 bytes positions the record stream on the correct log record
                // By this time all components of the DLSN are valid so this is where we shoud
                // retrieve the currentDLSN and advance to the next
                // Given that there are 20 bytes following the read position of the previous call
                // to readLong, we should not have moved ahead in the stream.
                LogRecordWithDLSN nextRecordInStream = new LogRecordWithDLSN(recordStream.getCurrentPosition(), startSequenceId);
                nextRecordInStream.setMetadata(metadata);
                recordStream.advanceToNextRecord();
                nextRecordInStream.setTransactionId(in.readLong());
                nextRecordInStream.readPayload(in, logVersion);
                if (LOG.isTraceEnabled()) {
                    if (nextRecordInStream.isControl()) {
                        LOG.trace("Reading {} Control DLSN {}", recordStream.getName(), nextRecordInStream.getDlsn());
                    } else {
                        LOG.trace("Reading {} Valid DLSN {}", recordStream.getName(), nextRecordInStream.getDlsn());
                    }
                }
                return nextRecordInStream;
            } catch (EOFException eof) {
                // Expected

            }
            return null;
        }

        public boolean skipTo(long txId) throws IOException {
            return skipTo(txId, null);
        }

        public boolean skipTo(DLSN dlsn) throws IOException {
            return skipTo(null, dlsn);
        }

        private boolean skipTo(Long txId, DLSN dlsn) throws IOException {
            LOG.debug("SkipTo");
            byte[] skipBuffer = null;
            boolean found = false;
            while (true) {
                in.mark(DistributedLogConstants.INPUTSTREAM_MARK_LIMIT);
                try {
                    in.readLong();
                    if ((null != dlsn) && (recordStream.getCurrentPosition().compareTo(dlsn) >=0)) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Found position {} beyond {}", recordStream.getCurrentPosition(), dlsn);
                        }
                        in.reset();
                        found = true;
                        break;
                    }
                    long currTxId = in.readLong();
                    if ((null != txId) && (currTxId >= txId)) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Found position {} beyond {}", currTxId, txId);
                        }
                        in.reset();
                        found = true;
                        break;
                    }
                    int length = in.readInt();
                    if (length < 0) {
                        // We should never really see this as we only write complete entries to
                        // BK and BK client has logic to detect torn writes (through checksum)
                        LOG.info("Encountered Record with negative length at TxId: {}", currTxId);
                        break;
                    }
                    if (null == skipBuffer) {
                        skipBuffer = new byte[SKIP_BUFFER_SIZE];
                    }
                    int read = 0;
                    while (read < length) {
                        int bytesToRead = Math.min(length - read, SKIP_BUFFER_SIZE);
                        in.readFully(skipBuffer, 0 , bytesToRead);
                        read += bytesToRead;
                    }
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Skipped Record with TxId {} DLSN {}", currTxId, recordStream.getCurrentPosition());
                    }
                    recordStream.advanceToNextRecord();
                } catch (EOFException eof) {
                    LOG.debug("Skip encountered end of file Exception", eof);
                    break;
                }
            }
            return found;
        }
    }
}
