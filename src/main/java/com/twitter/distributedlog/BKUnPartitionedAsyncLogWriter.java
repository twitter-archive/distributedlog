package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.MetadataException;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import scala.runtime.AbstractFunction0;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

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

    private final FuturePool orderedFuturePool;

    public BKUnPartitionedAsyncLogWriter(DistributedLogConfiguration conf,
                                         BKDistributedLogManager bkdlm,
                                         FuturePool orderedFuturePool,
                                         ExecutorService metadataExecutor) throws IOException {
        super(conf, bkdlm);
        this.orderedFuturePool = orderedFuturePool;
        this.createAndCacheWriteHandler(conf.getUnpartitionedStreamName(), orderedFuturePool, metadataExecutor);
    }

    BKUnPartitionedAsyncLogWriter recover() throws IOException {
        BKLogPartitionWriteHandler writeHandler =
                this.getWriteLedgerHandler(conf.getUnpartitionedStreamName(), false);
        writeHandler.recoverIncompleteLogSegments();
        return this;
    }

    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     */
    @Override
    public Future<DLSN> write(final LogRecord record) {
        final AtomicReference<BKPerStreamLogWriter> writerRef =
                new AtomicReference<BKPerStreamLogWriter>(null);
        return orderedFuturePool.apply(new ExceptionalFunction0<BKPerStreamLogWriter>() {
            public BKPerStreamLogWriter applyE() throws IOException {
                BKPerStreamLogWriter writer = getLedgerWriter(conf.getUnpartitionedStreamName());
                if (null == writer) {
                    writer = rollLogSegmentIfNecessary(null, conf.getUnpartitionedStreamName(),
                                                       record.getTransactionId());
                }
                return writer;
            }
        }).flatMap(new Function<BKPerStreamLogWriter, Future<DLSN>>() {
            public Future<DLSN> apply(BKPerStreamLogWriter w) {
                writerRef.set(w);
                return w.asyncWrite(record);
            }
        }).ensure(new AbstractFunction0() {
            @Override
            public Object apply() {
                BKPerStreamLogWriter writer = writerRef.get();
                if (null != writer) {
                    try {
                        rollLogSegmentIfNecessary(writer, conf.getUnpartitionedStreamName(),
                                                  record.getTransactionId());
                    } catch (IOException e) {
                        // it is hard to handle exceptions in an ensure block. but it is probaly ok
                        // here. since if the failure happened during rolling log segment, it doesn't
                        // put the newer log segment in caching map. so next write request could still
                        // using the existing log segment. it is actually good for handling zookeeper
                        // exceptions during ledger rolling.
                        LOG.warn("Failed to roll log segment for {}, but it is OK right now. Next write request will try rolling : ",
                                 BKUnPartitionedAsyncLogWriter.super.bkDistributedLogManager.name, e);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public Future<Boolean> truncate(final DLSN dlsn) {
        return orderedFuturePool.apply(new ExceptionalFunction0<BKLogPartitionWriteHandler>() {
            @Override
            public BKLogPartitionWriteHandler applyE() throws Throwable {
                return getWriteLedgerHandler(conf.getUnpartitionedStreamName(), false);
            }
        }).flatMap(new TruncationFunction(dlsn));
    }

    @Override
    public void closeAndComplete() throws IOException {
        // Insert a request to future pool to wait until all writes are completed.
        orderedFuturePool.apply(new ExceptionalFunction0<Integer>() {
            public Integer applyE() throws IOException {
                return 0;
            }
        }).get();
        super.closeAndComplete();
    }

    @Override
    public void abort() throws IOException {
        super.abort();
    }

    /**
     * *TEMP HACK*
     * Get the name of the stream this writer writes data to
     */
    @Override
    public String getStreamName() {
        return bkDistributedLogManager.getName();
    }

    @Override
    public String toString() {
        return String.format("AsyncLogWriter:%s", getStreamName());
    }
}
