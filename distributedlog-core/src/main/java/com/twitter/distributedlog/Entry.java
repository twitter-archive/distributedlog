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
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A set of {@link LogRecord}s.
 */
public class Entry {

    /**
     * Create a new log record set.
     *
     * @param logName
     *          name of the log
     * @param initialBufferSize
     *          initial buffer size
     * @param envelopeBeforeTransmit
     *          if envelope the buffer before transmit
     * @param codec
     *          compression codec
     * @param statsLogger
     *          stats logger to receive stats
     * @return writer to build a log record set.
     */
    public static Writer newEntry(
            String logName,
            int initialBufferSize,
            boolean envelopeBeforeTransmit,
            CompressionCodec.Type codec,
            StatsLogger statsLogger) {
        return new EnvelopedEntryWriter(
                logName,
                initialBufferSize,
                envelopeBeforeTransmit,
                codec,
                statsLogger);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Build the record set object.
     */
    public static class Builder {

        private long logSegmentSequenceNumber = -1;
        private long entryId = -1;
        private long startSequenceId = Long.MIN_VALUE;
        private boolean envelopeEntry = true;
        // input stream
        private InputStream in = null;
        // or bytes array
        private byte[] data = null;
        private int offset = -1;
        private int length = -1;
        private Optional<Long> txidToSkipTo = Optional.absent();
        private Optional<DLSN> dlsnToSkipTo = Optional.absent();
        private boolean deserializeRecordSet = true;

        private Builder() {}

        /**
         * Reset the builder.
         *
         * @return builder
         */
        public Builder reset() {
            logSegmentSequenceNumber = -1;
            entryId = -1;
            startSequenceId = Long.MIN_VALUE;
            envelopeEntry = true;
            // input stream
            in = null;
            // or bytes array
            data = null;
            offset = -1;
            length = -1;
            txidToSkipTo = Optional.absent();
            dlsnToSkipTo = Optional.absent();
            return this;
        }

        /**
         * Set the segment info of the log segment that this record
         * set belongs to.
         *
         * @param lssn
         *          log segment sequence number
         * @param startSequenceId
         *          start sequence id of this log segment
         * @return builder
         */
        public Builder setLogSegmentInfo(long lssn, long startSequenceId) {
            this.logSegmentSequenceNumber = lssn;
            this.startSequenceId = startSequenceId;
            return this;
        }

        /**
         * Set the entry id of this log record set.
         *
         * @param entryId
         *          entry id assigned for this log record set.
         * @return builder
         */
        public Builder setEntryId(long entryId) {
            this.entryId = entryId;
            return this;
        }

        /**
         * Set whether this record set is enveloped or not.
         *
         * @param enabled
         *          flag indicates whether this record set is enveloped or not.
         * @return builder
         */
        public Builder setEnvelopeEntry(boolean enabled) {
            this.envelopeEntry = enabled;
            return this;
        }

        /**
         * Set the serialized bytes data of this record set.
         *
         * @param data
         *          serialized bytes data of this record set.
         * @param offset
         *          offset of the bytes data
         * @param length
         *          length of the bytes data
         * @return builder
         */
        public Builder setData(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            return this;
        }

        /**
         * Set the input stream of the serialized bytes data of this record set.
         *
         * @param in
         *          input stream
         * @return builder
         */
        public Builder setInputStream(InputStream in) {
            this.in = in;
            return this;
        }

        /**
         * Set the record set starts from <code>dlsn</code>.
         *
         * @param dlsn
         *          dlsn to skip to
         * @return builder
         */
        public Builder skipTo(@Nullable DLSN dlsn) {
            this.dlsnToSkipTo = Optional.fromNullable(dlsn);
            return this;
        }

        /**
         * Set the record set starts from <code>txid</code>.
         *
         * @param txid
         *          txid to skip to
         * @return builder
         */
        public Builder skipTo(long txid) {
            this.txidToSkipTo = Optional.of(txid);
            return this;
        }

        /**
         * Enable/disable deserialize record set.
         *
         * @param enabled
         *          flag to enable/disable dserialize record set.
         * @return builder
         */
        public Builder deserializeRecordSet(boolean enabled) {
            this.deserializeRecordSet = enabled;
            return this;
        }

        public Entry build() {
            Preconditions.checkNotNull(data, "Serialized data isn't provided");
            Preconditions.checkArgument(offset >= 0 && length >= 0
                    && (offset + length) <= data.length,
                    "Invalid offset or length of serialized data");
            return new Entry(
                    logSegmentSequenceNumber,
                    entryId,
                    startSequenceId,
                    envelopeEntry,
                    deserializeRecordSet,
                    data,
                    offset,
                    length,
                    txidToSkipTo,
                    dlsnToSkipTo);
        }

        public Entry.Reader buildReader() throws IOException {
            Preconditions.checkArgument(data != null || in != null,
                    "Serialized data or input stream isn't provided");
            InputStream in;
            if (null != this.in) {
                in = this.in;
            } else {
                Preconditions.checkArgument(offset >= 0 && length >= 0
                                && (offset + length) <= data.length,
                        "Invalid offset or length of serialized data");
                in = new ByteArrayInputStream(data, offset, length);
            }
            return new EnvelopedEntryReader(
                    logSegmentSequenceNumber,
                    entryId,
                    startSequenceId,
                    in,
                    envelopeEntry,
                    deserializeRecordSet,
                    NullStatsLogger.INSTANCE);
        }

    }

    private final long logSegmentSequenceNumber;
    private final long entryId;
    private final long startSequenceId;
    private final boolean envelopedEntry;
    private final boolean deserializeRecordSet;
    private final byte[] data;
    private final int offset;
    private final int length;
    private final Optional<Long> txidToSkipTo;
    private final Optional<DLSN> dlsnToSkipTo;

    private Entry(long logSegmentSequenceNumber,
                  long entryId,
                  long startSequenceId,
                  boolean envelopedEntry,
                  boolean deserializeRecordSet,
                  byte[] data,
                  int offset,
                  int length,
                  Optional<Long> txidToSkipTo,
                  Optional<DLSN> dlsnToSkipTo) {
        this.logSegmentSequenceNumber = logSegmentSequenceNumber;
        this.entryId = entryId;
        this.startSequenceId = startSequenceId;
        this.envelopedEntry = envelopedEntry;
        this.deserializeRecordSet = deserializeRecordSet;
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.txidToSkipTo = txidToSkipTo;
        this.dlsnToSkipTo = dlsnToSkipTo;
    }

    /**
     * Get raw data of this record set.
     *
     * @return raw data representation of this record set.
     */
    public byte[] getRawData() {
        return data;
    }

    /**
     * Create reader to iterate over this record set.
     *
     * @return reader to iterate over this record set.
     * @throws IOException if the record set is invalid record set.
     */
    public Reader reader() throws IOException {
        InputStream in = new ByteArrayInputStream(data, offset, length);
        Reader reader = new EnvelopedEntryReader(
                logSegmentSequenceNumber,
                entryId,
                startSequenceId,
                in,
                envelopedEntry,
                deserializeRecordSet,
                NullStatsLogger.INSTANCE);
        if (txidToSkipTo.isPresent()) {
            reader.skipTo(txidToSkipTo.get());
        }
        if (dlsnToSkipTo.isPresent()) {
            reader.skipTo(dlsnToSkipTo.get());
        }
        return reader;
    }

    /**
     * Writer to append {@link LogRecord}s to {@link Entry}.
     */
    public interface Writer extends EntryBuffer {

        /**
         * Write a {@link LogRecord} to this record set.
         *
         * @param record
         *          record to write
         * @param transmitPromise
         *          callback for transmit result. the promise is only
         *          satisfied when this record set is transmitted.
         * @throws LogRecordTooLongException if the record is too long
         * @throws WriteException when encountered exception writing the record
         */
        void writeRecord(LogRecord record, Promise<DLSN> transmitPromise)
                throws LogRecordTooLongException, WriteException;

        /**
         * Reset the writer to write records.
         */
        void reset();

    }

    /**
     * Reader to read {@link LogRecord}s from this record set.
     */
    public interface Reader {

        /**
         * Read next log record from this record set.
         *
         * @return next log record from this record set.
         */
        LogRecordWithDLSN nextRecord() throws IOException;

        /**
         * Skip the reader to the record whose transaction id is <code>txId</code>.
         *
         * @param txId
         *          transaction id to skip to.
         * @return true if skip succeeds, otherwise false.
         * @throws IOException
         */
        boolean skipTo(long txId) throws IOException;

        /**
         * Skip the reader to the record whose DLSN is <code>dlsn</code>.
         *
         * @param dlsn
         *          DLSN to skip to.
         * @return true if skip succeeds, otherwise false.
         * @throws IOException
         */
        boolean skipTo(DLSN dlsn) throws IOException;

    }

}
