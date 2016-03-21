package com.twitter.distributedlog;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;

import java.io.IOException;
import java.util.List;

class BKSyncLogWriter extends BKAbstractLogWriter implements LogWriter {

    public BKSyncLogWriter(DistributedLogConfiguration conf,
                           DynamicDistributedLogConfiguration dynConf,
                           BKDistributedLogManager bkdlm) {
        super(conf, dynConf, bkdlm);
    }
    /**
     * Write log records to the stream.
     *
     * @param record operation
     */
    @Override
    public void write(LogRecord record) throws IOException {
        getLedgerWriter(record.getTransactionId(), false).write(record);
    }

    /**
     * Write edits logs operation to the stream.
     *
     * @param records list of records
     */
    @Override
    @Deprecated
    public int writeBulk(List<LogRecord> records) throws IOException {
        return getLedgerWriter(records.get(0).getTransactionId(), false).writeBulk(records);
    }

    /**
     * Flushes all the data up to this point,
     * adds the end of stream marker and marks the stream
     * as read-only in the metadata. No appends to the
     * stream will be allowed after this point
     */
    @Override
    public void markEndOfStream() throws IOException {
        getLedgerWriter(DistributedLogConstants.MAX_TXID, true).markEndOfStream();
        closeAndComplete();
    }

    /**
     * Close the stream without necessarily flushing immediately.
     * This may be called if the stream is in error such as after a
     * previous write or close threw an exception.
     */
    @Override
    public void abort() throws IOException {
        super.abort();
    }
}
