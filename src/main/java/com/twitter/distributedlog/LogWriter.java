package com.twitter.distributedlog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/*
* A generic interface class to support writing log records into
* a persistent distributed log.
*/
public interface LogWriter extends Closeable {
    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     * @throws IOException
     */
    public void write(LogRecord record) throws IOException;

    /**
     * Write a list of log records to the stream.
     *
     * @param record list of records
     * @throws IOException
     */
    public int writeBulk(List<LogRecord> records) throws IOException;

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
     * @throws IOException
     */
    public long flushAndSync() throws IOException;
}
