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

/**
 * Helper classes for reading the ops from an InputStream.
 * All ops derive from LogRecord and are only
 * instantiated from Reader#readOp()
 */
public class LogRecord {
    long flags;
    long txid;
    byte[] payload;

    // TODO: Replace with EnumSet
    public static final long LOGRECORD_FLAGS_CONTROL_MESSAGE = 0x1;
    public static final long LOGRECORD_FLAGS_END_OF_STREAM = 0x2;
    /**
     * This empty constructor can only be called from Reader#readOp.
     */
    private LogRecord() {
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

    public void setControl() {
        flags = flags | LOGRECORD_FLAGS_CONTROL_MESSAGE;
    }

    public boolean isControl() {
        return ((flags & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    public static boolean isControl(long flags) {
        return ((flags & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    public void setEndOfStream() {
        flags = flags | LOGRECORD_FLAGS_END_OF_STREAM;
    }

    public boolean isEndOfStream() {
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

    private void readPayload(DataInputStream in, int logVersion) throws IOException {
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

    private void setTransactionId(long txid) {
        this.txid = txid;
    }

    private void setFlags(long flags) {
        this.flags = flags;
    }

    /**
     * Class for writing log records
     */
    public static class Writer {
        private final DataOutputBuffer buf;

        public Writer(DataOutputBuffer out) {
            this.buf = out;
        }

        /**
         * Write an operation to the output stream
         *
         * @param record The operation to write
         * @throws IOException if an error occurs during writing.
         */
        public void writeOp(LogRecord record) throws IOException {
            buf.writeLong(record.flags);
            buf.writeLong(record.txid);
            record.writePayload(buf);
        }
    }

    /**
     * Class for reading editlog ops from a stream
     */
    public static class Reader {
        private final DataInputStream in;
        private final int logVersion;
        private static final int SKIP_BUFFER_SIZE = 512;

        /**
         * Construct the reader
         *
         * @param in The stream to read from.
         */
        public Reader(DataInputStream in, int logVersion) {
            this.logVersion = logVersion;
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
        public LogRecord readOp() throws IOException {
            in.mark(1);
            try {
                LogRecord nextRecordInStream = new LogRecord();
                nextRecordInStream.setFlags(in.readLong());
                nextRecordInStream.setTransactionId(in.readLong());
                nextRecordInStream.readPayload(in, logVersion);
                return nextRecordInStream;
            } catch (EOFException eof) {
                // Expected

            }
            return null;
        }

        public boolean skipTo(long txId) throws IOException {
            byte[] skipBuffer = null;
            boolean found = false;
            while (true) {
                in.mark(24);
                try {
                    in.readLong();
                    if (in.readLong() >= txId) {
                        in.reset();
                        found = true;
                        break;
                    }
                    int length = in.readInt();
                    if (length < 0) {
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
                } catch (EOFException eof) {
                    break;
                }
            }
            return found;
        }
    }
}