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
 */
public class LogRecord {
    static final Logger LOG = LoggerFactory.getLogger(LogRecord.class);

    private long flags;
    private long txid;
    private byte[] payload;

    // TODO: Replace with EnumSet
    static final long LOGRECORD_FLAGS_CONTROL_MESSAGE = 0x1;
    static final long LOGRECORD_FLAGS_END_OF_STREAM = 0x2;
    /**
     * This empty constructor can only be called from Reader#readOp.
     */
    protected LogRecord() {
        this.txid = 0;
        this.flags = 0;
    }

    public LogRecord(long txid, byte[] payload) {
        this.txid = txid;
        this.payload = payload;
        this.flags = 0;
    }

    public long getTransactionId() {
        return txid;
    }

    void setControl() {
        flags = flags | LOGRECORD_FLAGS_CONTROL_MESSAGE;
    }

    boolean isControl() {
        return ((flags & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    public static boolean isControl(long flags) {
        return ((flags & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    void setEndOfStream() {
        flags = flags | LOGRECORD_FLAGS_END_OF_STREAM;
    }

    boolean isEndOfStream() {
        return ((flags & LOGRECORD_FLAGS_END_OF_STREAM) != 0);
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

    protected void setFlags(long flags) {
        this.flags = flags;
    }

    private void writeToStream(DataOutputStream out) throws IOException {
        out.writeLong(flags);
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
        return 2 * (Long.SIZE) + Integer.SIZE + payload.length;
    }

    /**
     * Class for writing log records
     */
    static class Writer {
        private final DataOutputBuffer buf;

        Writer(DataOutputBuffer out) {
            this.buf = out;
        }

        /**
         * Write an operation to the output stream
         *
         * @param record The operation to write
         * @throws IOException if an error occurs during writing.
         */
        void writeOp(LogRecord record) throws IOException {
            record.writeToStream(buf);
        }

        int getPendingBytes() {
            return buf.getLength();
        }
    }

    /**
     * This class is a package private class for reading log records
     * from the persistent
      */
    static class Reader {
        private final RecordStream recordStream;
        private final DataInputStream in;
        private final int logVersion;
        private static final int SKIP_BUFFER_SIZE = 512;

        /**
         * Construct the reader
         *
         * @param in The stream to read from.
         */
        public Reader(RecordStream recordStream, DataInputStream in, int logVersion) {
            this.logVersion = logVersion;
            this.recordStream = recordStream;
            this.in = in;
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
                LogRecordWithDLSN nextRecordInStream = new LogRecordWithDLSN();
                nextRecordInStream.setFlags(in.readLong());
                // Reading the first 8 bytes positions the record stream on the correct log record
                // By this time all components of the DLSN are valid so this is where we shoud
                // retrieve the currentDLSN and advance to the next
                // Given that there are 20 bytes following the read position of the previous call
                // to readLong, we should not have moved ahead in the stream.
                DLSN dlsn = recordStream.getCurrentPosition();
                nextRecordInStream.setDlsn(dlsn);

                recordStream.advanceToNextRecord();
                nextRecordInStream.setTransactionId(in.readLong());
                nextRecordInStream.readPayload(in, logVersion);
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