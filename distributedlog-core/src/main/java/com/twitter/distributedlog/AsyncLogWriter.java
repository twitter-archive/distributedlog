package com.twitter.distributedlog;

import com.twitter.util.Future;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface AsyncLogWriter extends Closeable {
    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     * @return A Future which contains a DLSN if the record was successfully written
     * or an exception if the write fails
     */
    public Future<DLSN> write(LogRecord record);

    /**
     * Write log records to the stream in bulk. Each future in the list represents the result of
     * one write operation. The size of the result list is equal to the size of the input list.
     * Buffers are written in order, and the list of result futures has the same order.
     *
     * @param record set of log records
     * @return A Future which contains a list of Future DLSNs if the record was successfully written
     * or an exception if the operation fails.
     */
    public Future<List<Future<DLSN>>> writeBulk(List<LogRecord> record);

    /**
     * Truncate the log until <i>dlsn</i>.
     *
     * @param dlsn
     *          dlsn to truncate until.
     * @return A Future indicates whether the operation succeeds or not, or an exception
     * if the truncation fails.
     */
    public Future<Boolean> truncate(DLSN dlsn);

    /**
     * Abort the writer.
     */
    public void abort() throws IOException;

    /**
     * Get the name of the stream this writer writes data to
     */
    public String getStreamName();
}
