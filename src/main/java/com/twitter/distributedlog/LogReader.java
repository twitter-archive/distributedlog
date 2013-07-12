package com.twitter.distributedlog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface LogReader extends Closeable {
    /**
     * Close the stream.
     * @throws IOException if an error occurred while closing
     */
    public void close() throws IOException;

    /**
     * Read the next log record from the stream
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public LogRecord readNext(boolean shouldBlock) throws IOException;

    /**
     * Read the next numLogRec log records from the stream
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public List<LogRecord> readBulk(boolean shouldBlock, int numLogRecords) throws IOException;

    /**
     * Read the last transaction id
     * @return the last transaction Id that was successfully returned or consumed by the reader
     */
    public long getLastTxId();
}
