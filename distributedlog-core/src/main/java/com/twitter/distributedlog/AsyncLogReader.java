package com.twitter.distributedlog;

import com.twitter.util.Future;

import java.io.Closeable;
import java.util.List;

public interface AsyncLogReader extends Closeable {

    /**
     * Get stream name that the reader reads from.
     *
     * @return stream name.
     */
    public String getStreamName();

    /**
     * Read the next record from the log stream
     *
     * @return A promise that when satisfied will contain the Log Record with its DLSN.
     */
    public Future<LogRecordWithDLSN> readNext();

    /**
     * Read next <i>numEntries</i> entries. The future is only satisfied with non-empty list
     * of entries. It doesn't block until returning exact <i>numEntries</i>. It is a best effort
     * call.
     *
     * @param numEntries
     *          num entries
     * @return A promise that when satisfied will contain a non-empty list of records with their DLSN.
     */
    public Future<List<LogRecordWithDLSN>> readBulk(int numEntries);
}
