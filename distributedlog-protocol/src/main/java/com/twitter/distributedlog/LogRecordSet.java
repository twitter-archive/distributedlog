package com.twitter.distributedlog;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A set of {@link LogRecord}s.
 * <pre>
 * Structure:
 * Bytes 0  -  4                : Metadata (version + flags)
 * Bytes 4  - 7                 : Number of Records
 * Bytes 8  - 11                : Original Payload Length
 * Bytes 12 - 15                : Actual Payload Length
 * Bytes 16 - 16+length-1       : Payload
 * ------------------------------------------------------
 *
 * Metadata: Version and Flags // 32 Bits
 * --------------------------------------
 * 0 ... 0 0 0 0 0
 *             |_|
 *              |
 *    Compression Codec: // 2 Bits (Least significant)
 *    -----------------
 *    00        : No Compression
 *    01        : LZ4 Compression
 *    10        : Unused
 *    11        : Unused
 *
 * </pre>
 */
public class LogRecordSet {

    static final OpStatsLogger NullOpStatsLogger =
            NullStatsLogger.INSTANCE.getOpStatsLogger("");

    static final int HEADER_LEN =
            4 /* Metadata */
          + 4 /* Count */
          + 8 /* Lengths */
            ;

    // Version
    static final int VERSION = 0x1000;

    // Metadata
    static final int METADATA_VERSION_MASK = 0xf000;
    static final int METADATA_COMPRESSION_MASK = 0x3;

    // Compression Codec
    static final int COMPRESSION_CODEC_NONE = 0x0;
    static final int COMPRESSION_CODEC_LZ4 = 0X1;

    public static int numRecords(LogRecord record) throws IOException {
        Preconditions.checkArgument(record.isRecordSet(),
                "record is not a recordset");
        byte[] data = record.getPayload();
        return numRecords(data);
    }

    public static int numRecords(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int metadata = buffer.getInt();
        int version = (metadata & METADATA_VERSION_MASK);
        if (version != VERSION) {
            throw new IOException(String.format("Version mismatch while reading. Received: %d," +
                    " Required: %d", version, VERSION));
        }
        return buffer.getInt();
    }

    public static Writer newWriter(int initialBufferSize,
                                   CompressionCodec.Type codec) {
        return new EnvelopedRecordSetWriter(initialBufferSize, codec);
    }

    public static Reader of(LogRecordWithDLSN record) throws IOException {
        Preconditions.checkArgument(record.isRecordSet(),
                "record is not a recordset");
        byte[] data = record.getPayload();
        DLSN dlsn = record.getDlsn();
        int startPosition = record.getPositionWithinLogSegment();
        long startSequenceId = record.getStartSequenceIdOfCurrentSegment();

        return new EnvelopedRecordSetReader(
                dlsn.getLogSegmentSequenceNo(),
                dlsn.getEntryId(),
                record.getTransactionId(),
                dlsn.getSlotId(),
                startPosition,
                startSequenceId,
                new ByteArrayInputStream(data));
    }

    /**
     * Writer to append {@link LogRecord}s to {@link LogRecordSet}.
     */
    public static interface Writer extends LogRecordSetBuffer {

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
        void writeRecord(ByteBuffer record, Promise<DLSN> transmitPromise)
                throws LogRecordTooLongException, WriteException;

        /**
         * Complete transmit.
         *
         * @param lssn log segment sequence number
         * @param entryId entry id
         * @param startSlotId start slot id
         */
        void completeTransmit(long lssn, long entryId, long startSlotId);

        /**
         * Abort transmit.
         *
         * @param reason reason to abort.
         */
        void abortTransmit(Throwable reason);

    }

    /**
     * Reader to read {@link LogRecord}s from this record set.
     */
    public static interface Reader {

        /**
         * Read next log record from this record set.
         *
         * @return next log record from this record set.
         */
        LogRecordWithDLSN nextRecord() throws IOException;

    }

}
