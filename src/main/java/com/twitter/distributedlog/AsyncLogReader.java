package com.twitter.distributedlog;

import com.twitter.util.Future;
import java.io.IOException;
import java.util.List;

public interface AsyncLogReader {
    /**
     * Read the next log record from the stream
     *
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public Future<LogRecordWithDLSN> readNext(boolean shouldBlock) throws IOException;

    /**
     * Close the stream.
     *
     * @throws java.io.IOException if an error occurred while closing
     */
    public void close() throws IOException;
}
