package com.twitter.distributedlog;

import com.twitter.util.Future;

import java.io.Closeable;
import java.io.IOException;

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
     * *TEMP HACK*
     * Get the name of the stream this writer writes data to
     */
    public String getStreamName();
}
