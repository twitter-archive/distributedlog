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
        return getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, record.getTransactionId()).write(record);
    }
}
