package com.twitter.distributedlog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface LogReader extends Closeable {

    /**
     * Close the stream.
     *
     * @throws IOException if an error occurred while closing
     */
    public void close() throws IOException;

    /**
     * Read the next log record from the stream
     *
     * @param nonBlocking should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public LogRecordWithDLSN readNext(boolean nonBlocking) throws IOException;

    /**
     * Read the next numLogRec log records from the stream
     *
     * @param nonBlocking should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @param numLogRecords
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public List<LogRecordWithDLSN> readBulk(boolean nonBlocking, int numLogRecords) throws IOException;
}
