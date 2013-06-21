package com.twitter.distributedlog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PartitionAwareLogWriter {

    /**
     * Close the journal.
     * @throws IOException if the log stream can't be closed,
     */
    public void close() throws IOException;

    /**
     * Write log records to the stream.
     *
     * @param record - the log record to be generated
     * @param partition – the partition to which this log record should be written
     * @throws IOException
     */
    public void write(LogRecord record, PartitionId partition)
        throws IOException;

    /**
     * Write log records to the stream.
     *
     * @param records – a map with a list of log records for one or more partitions
     * @throws IOException
     */
    public int writeBulk(Map<PartitionId, List<LogRecord>> records)
        throws IOException;

    /**
     * All data that has been written to the stream so far will be flushed.
     * New data can be still written to the stream while flush is ongoing.
     */
    public long setReadyToFlush() throws IOException;

    /**
     * Flush and sync all data that is ready to be flush
     * {@link #setReadyToFlush()} into underlying persistent store.
     *
     * This API is optional as the writer implements a policy for automatically syncing
     * the log records in the buffer. The buffered edits can be flushed when the buffer
     * becomes full or a certain period of time is elapsed.
     *
     * @throws IOException
     */
    public long flushAndSync() throws IOException;
}
