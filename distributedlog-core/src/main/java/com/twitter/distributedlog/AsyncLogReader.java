package com.twitter.distributedlog;

import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.util.Future;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface AsyncLogReader extends AsyncCloseable {

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

    /**
     * Read next <i>numEntries</i> entries in a given <i>waitTime</i>.
     * <p>
     * The future is satisfied when either reads <i>numEntries</i> entries or reaches <i>waitTime</i>.
     * The only exception is if there isn't any new entries written within <i>waitTime</i>, it would
     * wait until new entries are available.
     *
     * @param numEntries
     *          max entries to return
     * @param waitTime
     *          maximum wait time if there are entries already for read
     * @param timeUnit
     *          wait time unit
     * @return A promise that when satisfied will contain a non-empty list of records with their DLSN.
     */
    public Future<List<LogRecordWithDLSN>> readBulk(int numEntries, long waitTime, TimeUnit timeUnit);
}
