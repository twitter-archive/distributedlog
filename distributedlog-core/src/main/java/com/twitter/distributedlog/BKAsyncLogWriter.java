package com.twitter.distributedlog;

import com.google.common.base.Stopwatch;
import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.feature.CoreFeatureKeys;
import com.twitter.distributedlog.stats.OpStatsListener;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;
import com.twitter.util.Futures;
import com.twitter.util.Promise;
import com.twitter.util.Try;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class BKAsyncLogWriter extends BKAbstractLogWriter implements AsyncLogWriter {

    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogWriter.class);

    static class TruncationFunction extends ExceptionalFunction<BKLogPartitionWriteHandler, Future<Boolean>> {

        private final DLSN dlsn;

        TruncationFunction(DLSN dlsn) {
            this.dlsn = dlsn;
        }

        @Override
        public Future<Boolean> applyE(BKLogPartitionWriteHandler handler) throws Throwable {
            if (DLSN.InvalidDLSN == dlsn) {
                Promise<Boolean> promise = new Promise<Boolean>();
                promise.setValue(false);
                return promise;
            }
            return handler.setLogsOlderThanDLSNTruncatedAsync(dlsn);
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
            errorOutPendingRequestsAndWriter(cause);
        }
    }

    private final FuturePool orderedFuturePool;
    private LinkedList<PendingLogRecord> pendingRequests = null;
    private volatile boolean encounteredError = false;
    private boolean queueingRequests = false;
    private long lastTxId = DistributedLogConstants.INVALID_TXID;

    private final StatsLogger statsLogger;
    private final OpStatsLogger writeOpStatsLogger;
    private final OpStatsLogger writeQueueOpStatsLogger;
    private final OpStatsLogger bulkWriteOpStatsLogger;
    private final OpStatsLogger bulkWriteQueueOpStatsLogger;
    private final OpStatsLogger getWriterOpStatsLogger;

    private final Feature disableLogSegmentRollingFeature;

    public BKAsyncLogWriter(DistributedLogConfiguration conf,
                            DynamicDistributedLogConfiguration dynConf,
                            BKDistributedLogManager bkdlm,
                            FuturePool orderedFuturePool,
                            FeatureProvider featureProvider,
                            StatsLogger dlmStatsLogger) throws IOException {
        super(conf, dynConf, bkdlm);
        this.orderedFuturePool = orderedFuturePool;
        this.createAndCacheWriteHandler(conf.getUnpartitionedStreamName(), orderedFuturePool);

        // features
        disableLogSegmentRollingFeature = featureProvider.getFeature(CoreFeatureKeys.DISABLE_LOGSEGMENT_ROLLING.name().toLowerCase());

        // stats
        this.statsLogger = dlmStatsLogger.scope("log_writer");
        this.writeOpStatsLogger = statsLogger.getOpStatsLogger("write");
        this.writeQueueOpStatsLogger = statsLogger.getOpStatsLogger("write/queued");
        this.bulkWriteOpStatsLogger = statsLogger.getOpStatsLogger("bulk_write");
        this.bulkWriteQueueOpStatsLogger = statsLogger.getOpStatsLogger("bulk_write/queued");
        this.getWriterOpStatsLogger = statsLogger.getOpStatsLogger("get_writer");
    }

    private synchronized void setLastTxId(long txId) {
        lastTxId = Math.max(lastTxId, txId);
    }

    @Override
    public synchronized long getLastTxId() {
        return lastTxId;
    }

    @VisibleForTesting
    FuturePool getOrderedFuturePool() {
        return orderedFuturePool;
    }

    BKAsyncLogWriter recover() throws IOException {
        BKLogPartitionWriteHandler writeHandler =
                this.getWriteLedgerHandler(conf.getUnpartitionedStreamName());
        // hold the lock for the handler across the lifecycle of log writer, so we don't need
        // to release underlying lock when rolling or completing log segments, which would reduce
        // the possibility of ownership change during rolling / completing log segments.
        writeHandler.lockHandler();
        boolean success = false;
        try {
            setLastTxId(writeHandler.recoverIncompleteLogSegments());
            success = true;
            return this;
        } finally {
            if (!success) {
                writeHandler.unlockHandler();
            }
        }
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

    private BKLogSegmentWriter getCachedPerStreamLogWriter() throws WriteException {
        if (encounteredError) {
            throw new WriteException(bkDistributedLogManager.getStreamName(),
                    "writer has been closed due to error.");
        }
        return getCachedLogWriter();
    }

    private BKLogSegmentWriter getPerStreamLogWriter(LogRecord record, boolean bestEffort,
                                                       boolean rollLog) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            if (encounteredError) {
                throw new WriteException(bkDistributedLogManager.getStreamName(), "writer has been closed due to error.");
            }
            BKLogSegmentWriter writer = getLedgerWriter(conf.getUnpartitionedStreamName());
            if (null == writer || rollLog) {
                writer = rollLogSegmentIfNecessary(writer, conf.getUnpartitionedStreamName(),
                                                   record.getTransactionId(), bestEffort, false);
            }
            success = true;
            return writer;
        } finally {
            if (success) {
                getWriterOpStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            } else {
                getWriterOpStatsLogger.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            }
        }
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

    boolean shouldRollLog(BKLogSegmentWriter w) {
        try {
            return !disableLogSegmentRollingFeature.isAvailable() &&
                    shouldStartNewSegment(w, conf.getUnpartitionedStreamName());
        } catch (IOException ioe) {
            return false;
        }
    }

    void startQueueingRequests() {
        assert(null == pendingRequests && false == queueingRequests);
        pendingRequests = new LinkedList<PendingLogRecord>();
        queueingRequests = true;
    }

    private Future<DLSN> asyncWrite(LogRecord record) throws IOException {
        return asyncWrite(record, true /* flush after write */);
    }

    private Future<DLSN> asyncWrite(LogRecord record, boolean flush) throws IOException {
        BKLogSegmentWriter w = getPerStreamLogWriter(record, false, false);
        return asyncWrite(w, record, flush);
    }

    // for ordering guarantee, we shouldn't send requests to next log segments until
    // previous log segment is done.
    private synchronized Future<DLSN> asyncWrite(BKLogSegmentWriter writer,
                                                 final LogRecord record,
                                                 boolean flush) throws IOException {
        // The passed in writer may be stale since we acquire the writer outside of sync
        // lock. If we recently rolled and the new writer is cached, use that instead.
        Future<DLSN> result = null;
        BKLogSegmentWriter w = getCachedPerStreamLogWriter();
        if (null == w) {
            w = writer;
        }
        if (queueingRequests) {
            result = queueRequest(record);
        } else if (shouldRollLog(w)) {
            // insert a last record, so when it called back, we will trigger a log segment rolling
            startQueueingRequests();
            LastPendingLogRecord lastLogRecordInCurrentSegment = new LastPendingLogRecord(record);
            w.asyncWrite(record, true).addEventListener(lastLogRecordInCurrentSegment);
            result = lastLogRecordInCurrentSegment.promise;
        } else {
            result = w.asyncWrite(record, flush);
        }
        return result.onSuccess(new AbstractFunction1<DLSN, BoxedUnit>() {
            @Override
            public BoxedUnit apply(DLSN dlsn) {
                setLastTxId(record.getTransactionId());
                return BoxedUnit.UNIT;
            }
        });
    }

    private List<Future<DLSN>> asyncWriteBulk(List<LogRecord> records) throws IOException {
        final ArrayList<Future<DLSN>> results = new ArrayList<Future<DLSN>>(records.size());
        Iterator<LogRecord> iterator = records.iterator();
        while (iterator.hasNext()) {
            LogRecord record = iterator.next();
            Future<DLSN> future = asyncWrite(record, !iterator.hasNext());
            results.add(future);

            // Abort early if an individual write has already failed.
            Option<Try<DLSN>> result = future.poll();
            if (result.isDefined() && result.get().isThrow()) {
                break;
            }
        }
        if (records.size() > results.size()) {
            appendCancelledFutures(results, records.size() - results.size());
        }
        return results;
    }

    private void appendCancelledFutures(List<Future<DLSN>> futures, int numToAdd) {
        final WriteCancelledException cre =
            new WriteCancelledException(getStreamName());
        for (int i = 0; i < numToAdd; i++) {
            Future<DLSN> cancelledFuture = Future.exception(cre);
            futures.add(cancelledFuture);
        }
    }

    private void rollLogSegmentAndIssuePendingRequests(LogRecord record) {
        try {
            BKLogSegmentWriter writer = getPerStreamLogWriter(record, true, true);
            synchronized (this) {
                for (PendingLogRecord pendingLogRecord : pendingRequests) {

                    FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_LogWriterIssuePending);

                    writer.asyncWrite(pendingLogRecord.record, true /* flush after write */)
                            .addEventListener(pendingLogRecord);
                }
                queueingRequests = false;
                pendingRequests = null;
            }
        } catch (IOException ioe) {
            errorOutPendingRequestsAndWriter(ioe);
        }
    }

    @VisibleForTesting
    void errorOutPendingRequests(Throwable cause, boolean errorOutWriter) {
        final List<PendingLogRecord> pendingLogRecords;
        synchronized (this) {
            pendingLogRecords = pendingRequests;
            encounteredError = errorOutWriter;
            pendingRequests = null;
            queueingRequests = false;
        }

        // After erroring out the writer above, no more requests
        // will be enqueued to pendingRequests
        for (PendingLogRecord pendingLogRecord : pendingLogRecords) {
            pendingLogRecord.promise.setException(cause);
        }
    }

    void errorOutPendingRequestsAndWriter(Throwable cause) {
        errorOutPendingRequests(cause, true /* error out writer */);
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
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return Futures.flatten(orderedFuturePool.apply(new ExceptionalFunction0<Future<DLSN>>() {
            @Override
            public Future<DLSN> applyE() throws IOException {
                writeQueueOpStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                return asyncWrite(record);
            }

            @Override
            public String toString() {
                return String.format("LogWrite(Stream=%s)", getStreamName());
            }
        })).addEventListener(new OpStatsListener<DLSN>(writeOpStatsLogger, stopwatch));
    }

    /**
     * Write many log records to the stream. The return type here is unfortunate but its a direct result
     * of having to combine FuturePool and the asyncWriteBulk method which returns a future as well. The
     * problem is the List that asyncWriteBulk returns can't be materialized until getPerStreamLogWriter
     * completes, so it has to be wrapped in a future itself.
     *
     * @param records list of records
     */
    @Override
    public Future<List<Future<DLSN>>> writeBulk(final List<LogRecord> records) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return orderedFuturePool.apply(new ExceptionalFunction0<List<Future<DLSN>>>() {
            @Override
            public List<Future<DLSN>> applyE() throws IOException {
                bulkWriteQueueOpStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                return asyncWriteBulk(records);
            }

            @Override
            public String toString() {
                return String.format("BulkLogWrite(Stream=%s)", getStreamName());
            }
        }).addEventListener(new OpStatsListener<List<Future<DLSN>>>(bulkWriteOpStatsLogger, stopwatch));
    }

    @VisibleForTesting
    Future<Void> nop() {
        return orderedFuturePool.apply(new ExceptionalFunction0<Void>() {
            @Override
            public Void applyE() throws Throwable {
                return null;
            }

            @Override
            public String toString() {
                return String.format("LogNop(Stream=%s)", getStreamName());
            }
        });
    }

    @Override
    public Future<Boolean> truncate(final DLSN dlsn) {
        return orderedFuturePool.apply(new ExceptionalFunction0<BKLogPartitionWriteHandler>() {
            @Override
            public BKLogPartitionWriteHandler applyE() throws Throwable {
                return getWriteLedgerHandler(conf.getUnpartitionedStreamName());
            }
            @Override
            public String toString() {
                return String.format("Truncate(Stream=%s, DLSN=%s)", getStreamName(), dlsn);
            }
        }).flatMap(new TruncationFunction(dlsn));
    }

    // Ordered sync operation. Calling fsync outside of the ordered future pool may result in
    // fsync happening out of program order. For certain applications this is a problem.
    Future<Long> flushAndSyncAll() {
        return orderedFuturePool.apply(new ExceptionalFunction0<Long>() {
            @Override
            public Long applyE() throws Throwable {
                setReadyToFlush();
                return flushAndSync();
            }

            @Override
            public String toString() {
                return String.format("FlushAndSyncAll(Stream=%s)", getStreamName());
            }
        });
    }

    @Override
    public void closeAndComplete() throws IOException {
        // Insert a request to future pool to wait until all writes are completed.
        FutureUtils.result(nop());
        super.closeAndComplete();
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
