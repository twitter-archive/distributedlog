package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


public class BKUnPartitionedSyncLogWriter extends BKUnPartitionedLogWriterBase implements LogWriter {

    public BKUnPartitionedSyncLogWriter(DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) throws IOException {
        super(conf, bkdlm);
    }
    /**
     * Write log records to the stream.
     *
     * @param record operation
     */
    @Override
    public void write(LogRecord record) throws IOException {
        if ((record.getTransactionId() < 0) ||
            (record.getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

        getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, record.getTransactionId()).write(record);
    }

    /**
     * Write edits logs operation to the stream.
     *
     * @param record list of records
     */
    @Override
    public int writeBulk(List<LogRecord> records) throws IOException {
        if ((records.get(0).getTransactionId() < 0) ||
            (records.get(0).getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

        return getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, records.get(0).getTransactionId()).writeBulk(records);
    }

    /**
     * Flushes all the data up to this point,
     * adds the end of stream marker and marks the stream
     * as read-only in the metadata. No appends to the
     * stream will be allowed after this point
     */
    @Override
    public void markEndOfStream() throws IOException {
        getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM,
            DistributedLogConstants.MAX_TXID).markEndOfStream();
        closeAndComplete();
    }
}
