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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log record is the basic element in a log.
 *
 * <p>A log is a sequence of log records. Each log record is a sequence of bytes.
 * Log records are written sequentially into a stream, and will be assigned with
 * an unique system generated sequence number {@link DLSN} (distributedlog sequence
 * number). Besides {@link DLSN}, application can assign its own sequence number
 * while constructing log records. The application defined sequence number is called
 * <code>TransactionID</code> (<i>txid</i>). Either {@link DLSN} or <code>TransactionId</code>
 * could be used to position readers to start from specific log records.
 *
 * <h3>User Record</h3>
 *
 * User records are the records written by applications and read by applications. They
 * are constructed via {@link #LogRecord(long, byte[])} by applications and appended to
 * logs by writers. And they would be deserialized from bytes by the readers and return
 * to applications.
 *
 * <h3>Control Record</h3>
 *
 * Control records are special records that written by distributedlog. They are invisible
 * to applications. They could be treated as <i>commit requests</i> as what people could find
 * in distributed consensus algorithms, since they are usually written by distributedlog to
 * commit application written records. <i>Commit</i> means making application written records
 * visible to readers to achieve consistent views among them.
 * <p>
 * They are named as 'Control Records' for controlling visibility of application written records.
 * <p>
 * The transaction id of 'Control Records' are assigned by distributedlog by inheriting from last
 * written user records. So we could indicate what user records that a control record is committing
 * by looking at its transaction id.
 *
 * <h4>EndOfStream Record</h4>
 *
 * <code>EoS</code>(EndOfStream) is a special control record that would be written by a writer
 * to seal a log. After a <i>EoS</i> record is written to a log, no writers could append any record
 * after that and readers will get {@link com.twitter.distributedlog.exceptions.EndOfStreamException}
 * when they reach EoS.
 * <p>TransactionID of EoS is <code>Long.MAX_VALUE</code>.
 *
 * <h3>Serialization & Deserialization</h3>
 *
 * Data type in brackets. Interpretation should be on the basis of data types and not individual
 * bytes to honor Endianness.
 * <p>
 * <pre>
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
 * </pre>
 *
 * <h3>Sequence Numbers</h3>
 *
 * A record is associated with three types of sequence numbers. They are generated
 * and used for different purposes. Check {@link LogRecordWithDLSN} for more details.
 *
 * @see LogRecordWithDLSN
 */
public class LogRecord {
    static final Logger LOG = LoggerFactory.getLogger(LogRecord.class);

    private static final int INPUTSTREAM_MARK_LIMIT = 16;

    static final long LOGRECORD_METADATA_FLAGS_MASK = 0xffff;
    static final long LOGRECORD_METADATA_POSITION_MASK = 0x0000ffffffff0000L;
    static final int LOGRECORD_METADATA_POSITION_SHIFT = 16;
    static final long LOGRECORD_METADATA_UNUSED_MASK = 0xffff000000000000L;

    // TODO: Replace with EnumSet
    static final long LOGRECORD_FLAGS_CONTROL_MESSAGE = 0x1;
    static final long LOGRECORD_FLAGS_END_OF_STREAM = 0x2;

    private long metadata;
    private long txid;
    private byte[] payload;

    /**
     * Construct an uninitialized log record.
     * <p>
     * NOTE: only deserializer should call this constructor.
     */
    protected LogRecord() {
        this.txid = 0;
        this.metadata = 0;
    }

    /**
     * Construct a log record with <i>TransactionId</i> and payload.
     * <p>Usually writer would construct the log record for writing.
     *
     * @param txid
     *          application defined transaction id.
     * @param payload
     *          record data
     */
    public LogRecord(long txid, byte[] payload) {
        this.txid = txid;
        this.payload = payload;
        this.metadata = 0;
    }

    //
    // Accessors
    //

    /**
     * Return application defined transaction id.
     *
     * @return transacton id.
     */
    public long getTransactionId() {
        return txid;
    }

    /**
     * Set application defined transaction id.
     *
     * @param txid application defined transaction id.
     */
    protected void setTransactionId(long txid) {
        this.txid = txid;
    }

    /**
     * Return the payload of this log record.
     *
     * @return payload of this log record.
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * Return the payload as an {@link InputStream}.
     *
     * @return payload as input stream
     */
    public InputStream getPayLoadInputStream() {
        return new ByteArrayInputStream(payload);
    }

    //
    // Metadata & Flags
    //

    protected void setMetadata(long metadata) {
        this.metadata = metadata;
    }

    void setPositionWithinLogSegment(int positionWithinLogSegment) {
        assert(positionWithinLogSegment >= 0);
        metadata = metadata | (((long) positionWithinLogSegment) << LOGRECORD_METADATA_POSITION_SHIFT);
    }

    public int getPositionWithinLogSegment() {
        long ret = (metadata & LOGRECORD_METADATA_POSITION_MASK) >> LOGRECORD_METADATA_POSITION_SHIFT;
        if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                (ret + " position should never exceed max integer value");
        }
        return (int) ret;
    }

    @VisibleForTesting
    public void setControl() {
        metadata = metadata | LOGRECORD_FLAGS_CONTROL_MESSAGE;
    }

    /**
     * Check if the record is a control record.
     *
     * @return true if the record is a control record, otherwise false.
     */
    public boolean isControl() {
        return ((metadata & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    /**
     * Check flags to see if it indicates a control record.
     *
     * @param flags record flags
     * @return true if the record is a control record, otherwise false.
     */
    public static boolean isControl(long flags) {
        return ((flags & LOGRECORD_FLAGS_CONTROL_MESSAGE) != 0);
    }

    /**
     * Set the record as <code>EoS</code> mark.
     *
     * @see #isEndOfStream()
     */
    void setEndOfStream() {
        metadata = metadata | LOGRECORD_FLAGS_END_OF_STREAM;
    }

    /**
     * Check if the record is a <code>EoS</code> mark.
     * <p><code>EoS</code> mark is a special record that writer would
     * add to seal a log. after <code>Eos</code> mark is written,
     * writers can't write any more records and readers will get
     * {@link com.twitter.distributedlog.exceptions.EndOfStreamException}
     * when they reach <code>EoS</code>.
     *
     * @return true
     */
    boolean isEndOfStream() {
        return ((metadata & LOGRECORD_FLAGS_END_OF_STREAM) != 0);
    }

    //
    // Serialization & Deserialization
    //

    protected void readPayload(DataInputStream in) throws IOException {
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
        private final long startSequenceId;
        private static final int SKIP_BUFFER_SIZE = 512;

        /**
         * Construct the reader
         *
         * @param in The stream to read from.
         */
        public Reader(RecordStream recordStream,
                      DataInputStream in,
                      long startSequenceId) {
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
                nextRecordInStream.readPayload(in);
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

        public boolean skipTo(long txId, boolean skipControl) throws IOException {
            return skipTo(txId, null, skipControl);
        }

        public boolean skipTo(DLSN dlsn) throws IOException {
            return skipTo(null, dlsn, false);
        }

        private boolean skipTo(Long txId, DLSN dlsn, boolean skipControl) throws IOException {
            LOG.debug("SkipTo");
            byte[] skipBuffer = null;
            boolean found = false;
            while (true) {
                in.mark(INPUTSTREAM_MARK_LIMIT);
                try {
                    long flags = in.readLong();
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
                        if (!skipControl || !isControl(flags)) {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Found position {} beyond {}", currTxId, txId);
                            }
                            in.reset();
                            found = true;
                            break;
                        }
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
