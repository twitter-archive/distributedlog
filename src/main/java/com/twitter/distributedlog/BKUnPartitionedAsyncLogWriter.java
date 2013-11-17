package com.twitter.distributedlog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Future;

import java.io.IOException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BKUnPartitionedAsyncLogWriter extends BKUnPartitionedLogWriterBase implements AsyncLogWriter {

    private final ExecutorService executorService;
    private final ExecutorServiceFuturePool futurePool;


    public BKUnPartitionedAsyncLogWriter(DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) throws IOException {
        super(conf, bkdlm);
        executorService = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("BKALW-" + bkdlm.name + "-executor-%d").build());
        futurePool = new ExecutorServiceFuturePool(executorService);
    }

    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     */
    @Override
    public Future<DLSN> write(final LogRecord record) throws IOException {
        if ((record.getTransactionId() < 0) ||
            (record.getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

        return futurePool.apply(new ExceptionalFunction0<BKPerStreamLogWriter>() {
            public BKPerStreamLogWriter applyE() throws IOException {
                return getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, record.getTransactionId());
            }
        }).flatMap(new ExceptionalFunction<BKPerStreamLogWriter, Future<DLSN>>() {
            public Future<DLSN> applyE(BKPerStreamLogWriter w) throws IOException {
                return w.write(record);
            }
        });
    }

    private void closeAndCompleteSync() throws IOException {
        super.closeAndComplete();
    }

    @Override
    public void closeAndComplete() throws IOException {
        futurePool.apply(new ExceptionalFunction0<Integer>() {
            public Integer applyE() throws IOException {
                closeAndCompleteSync();
                return 0;
            }
        }).get();
    }
}
