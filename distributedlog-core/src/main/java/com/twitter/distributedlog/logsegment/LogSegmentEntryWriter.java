package com.twitter.distributedlog.logsegment;

import com.google.common.annotations.Beta;
import com.twitter.distributedlog.util.Sizable;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;

/**
 * An interface class to write the enveloped entry (serialized bytes of
 * {@link com.twitter.distributedlog.LogRecordSet} into the log segment.
 *
 * <p>It is typically used by {@link LogSegmentWriter}.
 *
 * @see LogSegmentWriter
 *
 * TODO: The interface is leveraging bookkeeper's callback and status code now
 *       Consider making it more generic.
 */
@Beta
public interface LogSegmentEntryWriter extends Sizable {

    /**
     * Get the log segment id.
     *
     * @return log segment id.
     */
    long getLogSegmentId();

    /**
     * Close the entry writer.
     *
     * @throws BKException
     * @throws InterruptedException
     */
    void close() throws BKException, InterruptedException;

    /**
     * Async add entry to the log segment.
     * <p>The implementation semantic follows
     * {@link org.apache.bookkeeper.client.LedgerHandle#asyncAddEntry(
     * byte[], int, int, AsyncCallback.AddCallback, Object)}
     *
     * @param data
     *          data to add
     * @param offset
     *          offset in the data
     * @param length
     *          length of the data
     * @param callback
     *          callback
     * @param ctx
     *          ctx
     * @see org.apache.bookkeeper.client.LedgerHandle#asyncAddEntry(
     * byte[], int, int, AsyncCallback.AddCallback, Object)
     */
    void asyncAddEntry(byte[] data, int offset, int length,
                       AsyncCallback.AddCallback callback, Object ctx);
}