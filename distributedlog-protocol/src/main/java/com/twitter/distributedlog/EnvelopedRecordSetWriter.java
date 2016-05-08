package com.twitter.distributedlog;

import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.io.CompressionUtils;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.util.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedList;
import java.util.List;

import static com.twitter.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static com.twitter.distributedlog.LogRecordSet.*;

/**
 * {@link Buffer} based log record set writer.
 */
class EnvelopedRecordSetWriter implements LogRecordSet.Writer {

    static final Logger logger = LoggerFactory.getLogger(EnvelopedRecordSetWriter.class);

    private final Buffer buffer;
    private final DataOutputStream writer;
    private final WritableByteChannel writeChannel;
    private final List<Promise<DLSN>> promiseList;
    private final CompressionCodec.Type codec;
    private final int codecCode;
    private int count = 0;
    private ByteBuffer recordSetBuffer = null;

    EnvelopedRecordSetWriter(int initialBufferSize,
                             CompressionCodec.Type codec) {
        this.buffer = new Buffer(Math.max(initialBufferSize, HEADER_LEN));
        this.promiseList = new LinkedList<Promise<DLSN>>();
        this.codec = codec;
        switch (codec) {
            case LZ4:
                this.codecCode = COMPRESSION_CODEC_LZ4;
                break;
            default:
                this.codecCode = COMPRESSION_CODEC_NONE;
                break;
        }
        this.writer = new DataOutputStream(buffer);
        try {
            this.writer.writeInt((VERSION & METADATA_VERSION_MASK)
                    | (codecCode & METADATA_COMPRESSION_MASK));
            this.writer.writeInt(0); // count
            this.writer.writeInt(0); // original len
            this.writer.writeInt(0); // actual len
        } catch (IOException e) {
            logger.warn("Failed to serialize the header to an enveloped record set", e);
        }
        this.writeChannel = Channels.newChannel(writer);
    }

    synchronized List<Promise<DLSN>> getPromiseList() {
        return promiseList;
    }

    @Override
    public synchronized void writeRecord(ByteBuffer record,
                                         Promise<DLSN> transmitPromise)
            throws LogRecordTooLongException, WriteException {
        int logRecordSize = record.remaining();
        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(
                    "Log Record of size " + logRecordSize + " written when only "
                            + MAX_LOGRECORD_SIZE + " is allowed");
        }
        try {
            writer.writeInt(record.remaining());
            writeChannel.write(record);
            ++count;
            promiseList.add(transmitPromise);
        } catch (IOException e) {
            logger.error("Failed to append record to record set", e);
            throw new WriteException("", "Failed to append record to record set");
        }
    }

    private synchronized void satisfyPromises(long lssn, long entryId, long startSlotId) {
        long nextSlotId = startSlotId;
        for (Promise<DLSN> promise : promiseList) {
            promise.setValue(new DLSN(lssn, entryId, nextSlotId));
            nextSlotId++;
        }
        promiseList.clear();
    }

    private synchronized void cancelPromises(Throwable reason) {
        for (Promise<DLSN> promise : promiseList) {
            promise.setException(reason);
        }
        promiseList.clear();
    }

    @Override
    public int getNumBytes() {
        return buffer.size();
    }

    @Override
    public synchronized int getNumRecords() {
        return count;
    }

    @Override
    public synchronized ByteBuffer getBuffer() {
        if (null == recordSetBuffer) {
            recordSetBuffer = createBuffer();
        }
        return recordSetBuffer.duplicate();
    }

    ByteBuffer createBuffer() {
        byte[] data = buffer.getData();
        int dataOffset = HEADER_LEN;
        int dataLen = buffer.size() - HEADER_LEN;

        if (COMPRESSION_CODEC_LZ4 != codecCode) {
            ByteBuffer recordSetBuffer = ByteBuffer.wrap(data, 0, buffer.size());
            // update count
            recordSetBuffer.putInt(4, count);
            // update data len
            recordSetBuffer.putInt(8, dataLen);
            recordSetBuffer.putInt(12, dataLen);
            return recordSetBuffer;
        }

        // compression

        CompressionCodec compressor =
                    CompressionUtils.getCompressionCodec(codec);
        byte[] compressed =
                compressor.compress(data, dataOffset, dataLen, NullOpStatsLogger);

        ByteBuffer recordSetBuffer;
        if (compressed.length > dataLen) {
            byte[] newData = new byte[HEADER_LEN + compressed.length];
            System.arraycopy(data, 0, newData, 0, HEADER_LEN + dataLen);
            recordSetBuffer = ByteBuffer.wrap(newData);
        } else {
            recordSetBuffer = ByteBuffer.wrap(data);
        }
        // version
        recordSetBuffer.position(4);
        // update count
        recordSetBuffer.putInt(count);
        // update data len
        recordSetBuffer.putInt(dataLen);
        recordSetBuffer.putInt(compressed.length);
        recordSetBuffer.put(compressed);
        recordSetBuffer.flip();
        return recordSetBuffer;
    }

    @Override
    public void completeTransmit(long lssn, long entryId, long startSlotId) {
        satisfyPromises(lssn, entryId, startSlotId);
    }

    @Override
    public void abortTransmit(Throwable reason) {
        cancelPromises(reason);
    }
}
