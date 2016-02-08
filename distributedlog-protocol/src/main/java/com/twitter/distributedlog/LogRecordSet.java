package com.twitter.distributedlog;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.CompressionCodec;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A set of {@link LogRecord}s.
 */
public class LogRecordSet {

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
    public static Writer newLogRecordSet(
            String logName,
            int initialBufferSize,
            boolean envelopeBeforeTransmit,
            CompressionCodec.Type codec,
            StatsLogger statsLogger) {
        return new EnvelopedRecordSetWriter(
                logName,
                initialBufferSize,
                envelopeBeforeTransmit,
                codec,
                statsLogger);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Allow 4K overhead for metadata within the max transmission size
    public static final int MAX_LOGRECORD_SIZE = 1024 * 1024 - 8 * 1024; //1MB - 8KB
    // Allow 4K overhead for transmission overhead
    public static final int MAX_LOGRECORDSET_SIZE = 1024 * 1024 - 4 * 1024; //1MB - 4KB

    /**
     * Build the record set object.
     */
    public static class Builder {

        private long logSegmentSequenceNumber = -1;
        private long entryId = -1;
        private long startSequenceId = Long.MIN_VALUE;
        private boolean envelopeEntry = true;
        private byte[] data = null;
        private int offset = -1;
        private int length = -1;
        private Optional<Long> txidToSkipTo = Optional.absent();
        private Optional<DLSN> dlsnToSkipTo = Optional.absent();

        private Builder() {}

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

        public LogRecordSet build() {
            Preconditions.checkNotNull(data, "Serialized data isn't provided");
            Preconditions.checkArgument(offset >= 0 && length >= 0
                    && (offset + length) <= data.length,
                    "Invalid offset or length of serialized data");
            return new LogRecordSet(
                    logSegmentSequenceNumber,
                    entryId,
                    startSequenceId,
                    envelopeEntry,
                    data,
                    offset,
                    length,
                    txidToSkipTo,
                    dlsnToSkipTo);
        }

    }

    private final long logSegmentSequenceNumber;
    private final long entryId;
    private final long startSequenceId;
    private final boolean envelopedEntry;
    private final byte[] data;
    private final int offset;
    private final int length;
    private final Optional<Long> txidToSkipTo;
    private final Optional<DLSN> dlsnToSkipTo;

    private LogRecordSet(long logSegmentSequenceNumber,
                         long entryId,
                         long startSequenceId,
                         boolean envelopedEntry,
                         byte[] data,
                         int offset,
                         int length,
                         Optional<Long> txidToSkipTo,
                         Optional<DLSN> dlsnToSkipTo) {
        this.logSegmentSequenceNumber = logSegmentSequenceNumber;
        this.entryId = entryId;
        this.startSequenceId = startSequenceId;
        this.envelopedEntry = envelopedEntry;
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
        Reader reader = new EnvelopedRecordSetReader(
                logSegmentSequenceNumber,
                entryId,
                startSequenceId,
                in,
                envelopedEntry,
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
     * Writer to append {@link LogRecord}s to {@link LogRecordSet}.
     */
    public interface Writer {

        /**
         * Write a {@link LogRecord} to this record set.
         *
         * @param record
         *          record to write
         * @throws LogRecordTooLongException if the record is too long
         * @throws WriteException when encountered exception writing the record
         */
        void writeRecord(LogRecord record)
                throws LogRecordTooLongException, WriteException;

        /**
         * Return the number of bytes written so far.
         *
         * @return the number of bytes written so far.
         */
        int getNumBytes();

        /**
         * Return the number of records written so far.
         * @return
         */
        int getNumRecords();

        /**
         * Return serialized data.
         *
         * @return serialized data.
         */
        Buffer serialize() throws InvalidEnvelopedEntryException, IOException;

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
