package com.twitter.distributedlog;

import com.twitter.util.Future;
import java.io.IOException;


public class BKUnPartitionedAsyncLogWriter extends BKUnPartitionedLogWriterBase implements AsyncLogWriter {

    public BKUnPartitionedAsyncLogWriter(DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) throws IOException {
        super(conf, bkdlm);
    }

    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     */
    @Override
    public Future<DLSN> write(LogRecord record) throws IOException {
        if ((record.getTransactionId() < 0) ||
            (record.getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

        return getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, record.getTransactionId()).write(record);
    }
}
