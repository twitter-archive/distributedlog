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
}
