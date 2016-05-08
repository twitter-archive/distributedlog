package com.twitter.distributedlog;

import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.io.CompressionUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.twitter.distributedlog.LogRecordSet.*;

/**
 * Record reader to read records from an enveloped entry buffer.
 */
class EnvelopedRecordSetReader implements LogRecordSet.Reader {

    private final long logSegmentSeqNo;
    private final long entryId;
    private final long transactionId;
    private final long startSequenceId;
    private int numRecords;
    private final ByteBuffer reader;

    // slot id
    private long slotId;
    private int position;

    EnvelopedRecordSetReader(long logSegmentSeqNo,
                             long entryId,
                             long transactionId,
                             long startSlotId,
                             int startPositionWithinLogSegment,
                             long startSequenceId,
                             InputStream in)
            throws IOException {
        this.logSegmentSeqNo = logSegmentSeqNo;
        this.entryId = entryId;
        this.transactionId = transactionId;
        this.slotId = startSlotId;
        this.position = startPositionWithinLogSegment;
        this.startSequenceId = startSequenceId;

        // read data
        DataInputStream src = new DataInputStream(in);
        int metadata = src.readInt();
        int version = metadata & METADATA_VERSION_MASK;
        if (version != VERSION) {
            throw new IOException(String.format("Version mismatch while reading. Received: %d," +
                    " Required: %d", version, VERSION));
        }
        int codecCode = metadata & METADATA_COMPRESSION_MASK;
        this.numRecords = src.readInt();
        int originDataLen = src.readInt();
        int actualDataLen = src.readInt();
        byte[] compressedData = new byte[actualDataLen];
        src.readFully(compressedData);

        if (COMPRESSION_CODEC_LZ4 == codecCode) {
            CompressionCodec codec = CompressionUtils.getCompressionCodec(CompressionCodec.Type.LZ4);
            byte[] decompressedData = codec.decompress(compressedData, 0, actualDataLen,
                    originDataLen, NullOpStatsLogger);
            this.reader = ByteBuffer.wrap(decompressedData);
        } else {
            if (originDataLen != actualDataLen) {
                throw new IOException("Inconsistent data length found for a non-compressed record set : original = "
                        + originDataLen + ", actual = " + actualDataLen);
            }
            this.reader = ByteBuffer.wrap(compressedData);
        }
    }

    @Override
    public LogRecordWithDLSN nextRecord() throws IOException {
        if (numRecords <= 0) {
            return null;
        }

        int recordLen = reader.getInt();
        byte[] recordData = new byte[recordLen];
        reader.get(recordData);
        DLSN dlsn = new DLSN(logSegmentSeqNo, entryId, slotId);

        LogRecordWithDLSN record =
                new LogRecordWithDLSN(dlsn, startSequenceId);
        record.setPositionWithinLogSegment(position);
        record.setTransactionId(transactionId);
        record.setPayload(recordData);

        ++slotId;
        ++position;
        --numRecords;

        return record;
    }

}
