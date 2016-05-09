package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecordSetBuffer;
import com.twitter.util.Future;

import java.nio.ByteBuffer;
import java.util.List;

public interface DistributedLogClient {
    /**
     * Write <i>data</i> to a given <i>stream</i>.
     *
     * @param stream
     *          Stream Name.
     * @param data
     *          Data to write.
     * @return a future representing a sequence id returned for this write.
     */
    Future<DLSN> write(String stream, ByteBuffer data);

    /**
     * Write record set to a given <i>stream</i>.
     * <p>The record set is built from {@link com.twitter.distributedlog.LogRecordSet.Writer}
     *
     * @param stream stream to write to
     * @param recordSet record set
     */
    Future<DLSN> writeRecordSet(String stream, LogRecordSetBuffer recordSet);

    /**
     * Write <i>data</i> in bulk to a given <i>stream</i>. Return a list of
     * Future dlsns, one for each submitted buffer. In the event of a partial
     * failure--ex. some specific buffer write fails, all subsequent writes
     * will also fail.
     *
     * @param stream
     *          Stream Name.
     * @param data
     *          Data to write.
     * @return a list of futures, one for each submitted buffer.
     */
    List<Future<DLSN>> writeBulk(String stream, List<ByteBuffer> data);

    /**
     * Truncate the stream to a given <i>dlsn</i>.
     *
     * @param stream
     *          Stream Name.
     * @param dlsn
     *          DLSN to truncate until.
     * @return a future representing the truncation.
     */
    Future<Boolean> truncate(String stream, DLSN dlsn);

    /**
     * Release the ownership of a stream <i>stream</i>.
     *
     * @param stream
     *          Stream Name to release.
     * @return a future representing the release operation.
     */
    Future<Void> release(String stream);

    /**
     * Delete a given stream <i>stream</i>.
     *
     * @param stream
     *          Stream Name to delete.
     * @return a future representing the delete operation.
     */
    Future<Void> delete(String stream);

    /**
     * Create a stream with name <i>stream</i>.
     *
     * @param stream
     *          Stream Name to create.
     * @return a future representing the create operation.
     */
    Future<Void> create(String stream);

    /**
     * Close the client.
     */
    void close();
}
