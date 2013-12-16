package com.twitter.distributedlog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.distributedlog.exceptions.MetadataException;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;

import java.io.IOException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BKUnPartitionedAsyncLogWriter extends BKUnPartitionedLogWriterBase implements AsyncLogWriter {

    static class TruncationFunction extends ExceptionalFunction<BKLogPartitionWriteHandler, Future<Boolean>>
            implements BookkeeperInternalCallbacks.GenericCallback<Void> {

        private final DLSN dlsn;
        private final Promise<Boolean> promise = new Promise<Boolean>();

        TruncationFunction(DLSN dlsn) {
            this.dlsn = dlsn;
        }

        @Override
        public Future<Boolean> applyE(BKLogPartitionWriteHandler handler) throws Throwable {
            if (DLSN.InvalidDLSN == dlsn) {
                promise.setValue(false);
                return promise;
            }
            handler.purgeLogsOlderThanDLSN(dlsn, this);
            return promise;
        }

        @Override
        public void operationComplete(int rc, Void result) {
            if (BKException.Code.OK == rc) {
                promise.setValue(true);
            } else {
                promise.setException(new MetadataException("Error on purging logs before " + dlsn,
                        BKException.create(rc)));
            }
        }
    }

    private final FuturePool futurePool;

    public BKUnPartitionedAsyncLogWriter(DistributedLogConfiguration conf,
                                         BKDistributedLogManager bkdlm,
                                         FuturePool futurePool) throws IOException {
        super(conf, bkdlm);
        this.futurePool = futurePool;
    }

    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     */
    @Override
    public Future<DLSN> write(final LogRecord record) {
        return futurePool.apply(new ExceptionalFunction0<BKPerStreamLogWriter>() {
            public BKPerStreamLogWriter applyE() throws IOException {
                return getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, record.getTransactionId(), 1);
            }
        }).flatMap(new ExceptionalFunction<BKPerStreamLogWriter, Future<DLSN>>() {
            public Future<DLSN> applyE(BKPerStreamLogWriter w) throws IOException {
                return w.asyncWrite(record);
            }
        });
    }

    @Override
    public Future<Boolean> truncate(final DLSN dlsn) {
        return futurePool.apply(new ExceptionalFunction0<BKLogPartitionWriteHandler>() {
            @Override
            public BKLogPartitionWriteHandler applyE() throws Throwable {
                return getWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM, false);
            }
        }).flatMap(new TruncationFunction(dlsn));
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
