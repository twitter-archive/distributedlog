package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Future$;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class BKUnPartitionedAsyncLogWriter extends BKUnPartitionedLogWriterBase implements AsyncLogWriter {

    static class TruncationFunction extends ExceptionalFunction<BKLogPartitionWriteHandler, Future<Boolean>> {

        private final DLSN dlsn;

        TruncationFunction(DLSN dlsn) {
            this.dlsn = dlsn;
        }

        @Override
        public Future<Boolean> applyE(BKLogPartitionWriteHandler handler) throws Throwable {
            if (DLSN.InvalidDLSN == dlsn) {
                return Future.value(false);
            }
            return handler.purgeLogsOlderThanDLSN(dlsn).map(new Function<List<LogSegmentLedgerMetadata>, Boolean>() {
                @Override
                public Boolean apply(List<LogSegmentLedgerMetadata> logSegments) {
                    return true;
                }
            });
        }

    }

    // Records pending for roll log segment.
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

    // Roll the log segment and apply pending results on success.
    class LogRollingListener implements FutureEventListener<Void> {

        LogRecord lastLogRecord;

        LogRollingListener(LogRecord lastLogRecord) {
            this.lastLogRecord = lastLogRecord;
        }

        @Override
        public void onSuccess(Void value) {
            rollLogSegmentAndIssuePendingRequests(lastLogRecord);
        }

        @Override
        public void onFailure(Throwable cause) {
            encounteredError = true;
            errorOutPendingRequests(cause);
        }
    }

    private final FuturePool orderedFuturePool;
    private LinkedList<PendingLogRecord> pendingRequests = null;
    private boolean encounteredError = false;
    private boolean queueingRequests = false;

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
        // hold the lock for the handler across the lifecycle of log writer, so we don't need
        // to release underlying lock when rolling or completing log segments, which would reduce
        // the possibility of ownership change during rolling / completing log segments.
        writeHandler.lockHandler().recoverIncompleteLogSegments();
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
            throw new WriteException(bkDistributedLogManager.getStreamName(), "writer has been closed due to error.");
        }

        BKPerStreamLogWriter writer = getPerStreamLogWriterNoWait(record, bestEffort, rollLog);
        if (null == writer || rollLog) {
            writer = rollLogSegmentIfNecessary(writer, conf.getUnpartitionedStreamName(),
                                               record.getTransactionId(), bestEffort);
        }
        return writer;
    }

    private BKPerStreamLogWriter getPerStreamLogWriterNoWait(LogRecord record, boolean bestEffort,
                                                             boolean rollLog) throws IOException {
        if (encounteredError) {
            throw new WriteException(bkDistributedLogManager.getStreamName(), "writer has been closed due to error.");
        }

        return getLedgerWriter(conf.getUnpartitionedStreamName());
    }

    Future<DLSN> queueRequest(LogRecord record) {
        PendingLogRecord pendingLogRecord = new PendingLogRecord(record);
        pendingRequests.add(pendingLogRecord);
        return pendingLogRecord.promise;
    }

    List<Future<DLSN>> queueRequests(List<LogRecord> records) {
        List<Future<DLSN>> pendingResults = new ArrayList<Future<DLSN>>(records.size());
        for (LogRecord record : records) {
            pendingResults.add(queueRequest(record));
        }
        return pendingResults;
    }

    boolean shouldRollLog(BKPerStreamLogWriter w) {
        try {
            return shouldStartNewSegment(w, conf.getUnpartitionedStreamName());
        } catch (IOException ioe) {
            return false;
        }
    }

    void startQueueingRequests() {
        assert(null == pendingRequests && false == queueingRequests);
        pendingRequests = new LinkedList<PendingLogRecord>();
        queueingRequests = true;
    }

    // for ordering guarantee, we shouldn't send requests to next log segments until
    // previous log segment is done.
    private Future<DLSN> asyncWrite(BKPerStreamLogWriter w, LogRecord record) {
        Future<DLSN> result = null;
        if (queueingRequests) {
            result = queueRequest(record);
        } else if (shouldRollLog(w)) {
            // insert a last record, so when it called back, we will trigger a log segment rolling
            startQueueingRequests();
            LastPendingLogRecord lastLogRecordInCurrentSegment = new LastPendingLogRecord(record);
            w.asyncWrite(record).addEventListener(lastLogRecordInCurrentSegment);
            result = lastLogRecordInCurrentSegment.promise;
        } else {
            result = w.asyncWrite(record);
        }
        return result;
    }

    private List<Future<DLSN>> asyncWriteBulk(BKPerStreamLogWriter w, List<LogRecord> records) {
        List<Future<DLSN>> results = null;
        if (queueingRequests) {
            results = queueRequests(records);
        } else if (shouldRollLog(w)) {
            startQueueingRequests();
            LogRollingListener logRollingListener = new LogRollingListener(records.get(records.size()-1));
            results = w.asyncWriteBulk(records);
            Future$.MODULE$.join(results).voided().addEventListener(logRollingListener);
        } else {
            results = w.asyncWriteBulk(records);
        }
        return results;
    }

    private void rollLogSegmentAndIssuePendingRequests(LogRecord record) {
        try {
            BKPerStreamLogWriter writer = getPerStreamLogWriter(record, true, true);
            queueingRequests = false;
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

    /**
     * Write many log records to the stream. The return type here is unfortunate but its a direct result
     * of having to combine FuturePool and the asyncWriteBulk method which returns a future as well. The
     * problem is the List that asyncWriteBulk returns can't be materialized until getPerStreamLogWriter
     * completes, so it has to be wrapped in a future itself.
     *
     * @param record single log record
     */
    @Override
    public Future<List<Future<DLSN>>> writeBulk(final List<LogRecord> records) {
        return orderedFuturePool.apply(new ExceptionalFunction0<List<Future<DLSN>>>() {
            public List<Future<DLSN>> applyE() throws IOException {
                LogRecord firstRecord = records.get(0);
                BKPerStreamLogWriter w = getPerStreamLogWriter(firstRecord, false, false);
                return asyncWriteBulk(w, records);
            }
        });
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
        return bkDistributedLogManager.getStreamName();
    }

    @Override
    public String toString() {
        return String.format("AsyncLogWriter:%s", getStreamName());
    }
}
