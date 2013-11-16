package com.twitter.distributedlog;

import com.twitter.util.Future;

import java.io.Closeable;
import java.io.IOException;

public interface AsyncLogWriter extends Closeable {
    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     * @throws java.io.IOException
     */
    public Future<DLSN> write(LogRecord record) throws IOException;
}
