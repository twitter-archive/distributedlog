package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.exceptions.MetadataException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Future$;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

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

    // records pending for roll log segment
    class PendingLogRecord implements FutureEventListener<DLSN> {

        final LogRecord record;
        final Promise<DLSN> promise;

        PendingLogRecord(LogRecord record) {
            this.record = record;
            this.promise = new Promise<DLSN>();
        }

        @Override
        public void onSuccess(DLSN value) {
            promise.setValue(value);
        }

        @Override
        public void onFailure(Throwable cause) {
            promise.setException(cause);
            encounteredError = true;
        }
    }

    /**
     * Last pending record in current log segment. After it is satisified, it would
     * roll log segment.
     *
     * This implementation is based on the assumption that all future satisified in same
     * order future pool.
     */
    class LastPendingLogRecord extends PendingLogRecord {

        LastPendingLogRecord(LogRecord record) {
            super(record);
        }

        @Override
        public void onSuccess(DLSN value) {
            super.onSuccess(value);
            // roll log segment and issue all pending requests.
            rollLogSegmentAndIssuePendingRequests(record);
        }

        @Override
        public void onFailure(Throwable cause) {
            super.onFailure(cause);
            // error out pending requests.
            errorOutPendingRequests(cause);
        }
    }

    private final FuturePool orderedFuturePool;
    private LinkedList<PendingLogRecord> pendingRequests = null;
    private boolean encounteredError = false;
    private boolean shouldPendRequest = false;

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
     * Write a log record as control record. The method will be used by Monitor Service to enforce a new inprogress segment.
     *
     * @param record
     *          log record
     * @return future of the write
     */
    public Future<DLSN> writeControlRecord(final LogRecord record) {
        record.setControl();
        return write(record);
    }

    private BKPerStreamLogWriter getPerStreamLogWriter(LogRecord record, boolean bestEffort,
                                                       boolean rollLog) throws IOException {
        if (encounteredError) {
            throw new WriteException(bkDistributedLogManager.getName(), "writer has been closed due to error.");
        }

        BKPerStreamLogWriter writer = getLedgerWriter(conf.getUnpartitionedStreamName());
        if (null == writer || rollLog) {
            writer = rollLogSegmentIfNecessary(writer, conf.getUnpartitionedStreamName(),
                                               record.getTransactionId(), bestEffort);
        }
        return writer;
    }

    // for ordering guarantee, we shouldn't send requests to next log segments until
    // previous log segment is done.
    private Future<DLSN> asyncWrite(BKPerStreamLogWriter w, LogRecord record) {
        if (shouldPendRequest) {
            PendingLogRecord pendingLogRecord = new PendingLogRecord(record);
            pendingRequests.add(pendingLogRecord);
            return pendingLogRecord.promise;
        }

        boolean shouldRollLog;
        try {
            shouldRollLog = shouldShartNewSegment(w, conf.getUnpartitionedStreamName());
        } catch (IOException ioe) {
            shouldRollLog = false;
        }
        if (shouldRollLog) {
            pendingRequests = new LinkedList<PendingLogRecord>();
            shouldPendRequest = true;
            // insert a last record, so when it called back, we will trigger a log segment rolling
            LastPendingLogRecord lastLogRecordInCurrentSegment = new LastPendingLogRecord(record);
            w.asyncWrite(record).addEventListener(lastLogRecordInCurrentSegment);
            return lastLogRecordInCurrentSegment.promise;
        } else {
            return w.asyncWrite(record);
        }
    }

    private void rollLogSegmentAndIssuePendingRequests(LogRecord record) {
        try {
            BKPerStreamLogWriter writer = getPerStreamLogWriter(record, true, true);
            shouldPendRequest = false;
            // issue pending requests
            LinkedList<PendingLogRecord> requestsToIssue = pendingRequests;
            pendingRequests = null;
            for (PendingLogRecord pendingLogRecord : requestsToIssue) {
                writer.asyncWrite(pendingLogRecord.record)
                        .addEventListener(pendingLogRecord);
            }
        } catch (IOException ioe) {
            encounteredError = true;
            errorOutPendingRequests(ioe);
        }
    }

    private void errorOutPendingRequests(Throwable cause) {
        LinkedList<PendingLogRecord> requestsToErrorOut = pendingRequests;
        pendingRequests = null;
        for (PendingLogRecord pendingLogRecord : requestsToErrorOut) {
            pendingLogRecord.promise.setException(cause);
        }
    }

    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     */
    @Override
    public Future<DLSN> write(final LogRecord record) {
        // IMPORTANT: Continuations (flatMap, map, etc.) applied to a completed future are NOT guaranteed 
        // to run inline/synchronously. For example if the current thread is already running some 
        // continuation, any new applied continuations will be run only after the current continuation 
        // completes. Thus it is NOT safe to replace the single flattened future pool block below with 
        // the flatMap alternative, "futurePool { getWriter } flatMap { asyncWrite }".
        return Future$.MODULE$.flatten(orderedFuturePool.apply(new ExceptionalFunction0<Future<DLSN>>() {
            public Future<DLSN> applyE() throws IOException {
                BKPerStreamLogWriter w = getPerStreamLogWriter(record, false, false);
                return asyncWrite(w, record);
            }
        }));
    }

    @VisibleForTesting
    Future<Void> nop() {
        return orderedFuturePool.apply(new ExceptionalFunction0<Void>() {
            @Override
            public Void applyE() throws Throwable {
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
        nop().get();
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
