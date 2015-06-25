package com.twitter.distributedlog.logsegment;

import com.google.common.annotations.Beta;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.io.Abortable;
import com.twitter.util.Future;

import java.io.Closeable;
import java.io.IOException;

/**
 * An interface class to write log records into a log segment.
 */
@Beta
public interface LogSegmentWriter extends Closeable, Abortable {

    /**
     * Write a log record to a log segment.
     *
     * @param record single log record
     * @return a future representing write result. A {@link DLSN} is returned if write succeeds,
     *         otherwise, exceptions are returned.
     * @throws com.twitter.distributedlog.exceptions.LogRecordTooLongException if log record is too long
     * @throws com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException on invalid enveloped entry
     * @throws com.twitter.distributedlog.LockingException if failed to acquire lock for the writer
     * @throws com.twitter.distributedlog.BKTransmitException if failed to transmit data to bk
     * @throws com.twitter.distributedlog.exceptions.WriteException if failed to write to bk
     */
    public Future<DLSN> asyncWrite(LogRecord record);

    /**
     * This isn't a simple synchronous version of {@code asyncWrite}. It has different semantic.
     * This method only writes data to the buffer and flushes buffer if needed.
     *
     * TODO: we should remove this method. when we rewrite synchronous writer based on asynchronous writer,
     *       since this is the semantic needed to be provided in higher level but just calling write & flush.
     *
     * @param record single log record
     * @throws IOException when tried to flush the buffer.
     * @see {@link LogSegmentWriter#asyncWrite}
     */
    public void write(LogRecord record) throws IOException;

    /**
     * All data that has been written to the stream so far will be sent to
     * persistent storage.
     * The transmission is asynchronous and new data can be still written to the
     * stream while flushing is performed.
     */
    public long setReadyToFlush() throws IOException;

    /**
     * Flush and sync all data that is ready to be flush
     * {@link #setReadyToFlush()} into underlying persistent store.
     * @throws java.io.IOException
     */
    public long flushAndSync() throws IOException;

}
