package com.twitter.distributedlog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface LogReader extends Closeable {

    public interface ReaderNotification {
        void notifyNextRecordAvailable();
    }
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
    public LogRecord readNext(boolean nonBlocking) throws IOException;

    /**
     * Read the next numLogRec log records from the stream
     *
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public List<LogRecord> readBulk(boolean shouldBlock, int numLogRecords) throws IOException;

    /**
     * Register for notifications of changes to background reader when using
     * non blocking semantics
     *
     * @param notification Implementation of the ReaderNotification interface whose methods
     * are called when new data is available or when the reader errors out
     */
    public void registerNotification(ReaderNotification notification);

    /**
     * Read the last transaction id
     * @return the last transaction Id that was successfully returned or consumed by the reader
     */
    @Deprecated
    public long getLastTxId();
}
