/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import com.google.common.base.Stopwatch;
import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.StreamNotReadyException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.feature.CoreFeatureKeys;
import com.twitter.distributedlog.stats.OpStatsListener;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Try;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * BookKeeper based {@link AsyncLogWriter} implementation.
 *
 * <h3>Metrics</h3>
 * All the metrics are exposed under `log_writer`.
 * <ul>
 * <li> `log_writer/write`: opstats. latency characteristics about the time that write operations spent.
 * <li> `log_writer/bulk_write`: opstats. latency characteristics about the time that bulk_write
 * operations spent.
 * are pending in the queue for long time due to log segment rolling.
 * <li> `log_writer/get_writer`: opstats. the time spent on getting the writer. it could spike when there
 * is log segment rolling happened during getting the writer. it is a good stat to look into when the latency
 * is caused by queuing time.
 * <li> `log_writer/pending_request_dispatch`: counter. the number of queued operations that are dispatched
 * after log segment is rolled. it is an metric on measuring how many operations has been queued because of
 * log segment rolling.
 * </ul>
 * See {@link BKLogSegmentWriter} for segment writer stats.
 */
public class BKAsyncLogWriter extends BKAbstractLogWriter implements AsyncLogWriter {

    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogWriter.class);

    static Function1<List<LogSegmentMetadata>, Boolean> TruncationResultConverter =
            new AbstractFunction1<List<LogSegmentMetadata>, Boolean>() {
                @Override
                public Boolean apply(List<LogSegmentMetadata> segments) {
                    return true;
                }
            };

    // Records pending for roll log segment.
    class PendingLogRecord implements FutureEventListener<DLSN> {

        final LogRecord record;
        final Promise<DLSN> promise;
        final boolean flush;

        PendingLogRecord(LogRecord record, boolean flush) {
            this.record = record;
            this.promise = new Promise<DLSN>();
            this.flush = flush;
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

        LastPendingLogRecord(LogRecord record, boolean flush) {
            super(record, flush);
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

    private final boolean streamFailFast;
    private final boolean disableRollOnSegmentError;
    private LinkedList<PendingLogRecord> pendingRequests = null;
    private volatile boolean encounteredError = false;
    private Promise<BKLogSegmentWriter> rollingFuture = null;
    private long lastTxId = DistributedLogConstants.INVALID_TXID;

    private final StatsLogger statsLogger;
    private final OpStatsLogger writeOpStatsLogger;
    private final OpStatsLogger markEndOfStreamOpStatsLogger;
    private final OpStatsLogger bulkWriteOpStatsLogger;
    private final OpStatsLogger getWriterOpStatsLogger;
    private final Counter pendingRequestDispatch;

    private final Feature disableLogSegmentRollingFeature;

    BKAsyncLogWriter(DistributedLogConfiguration conf,
                     DynamicDistributedLogConfiguration dynConf,
                     BKDistributedLogManager bkdlm,
                     BKLogWriteHandler writeHandler, /** log writer owns the handler **/
                     FeatureProvider featureProvider,
                     StatsLogger dlmStatsLogger) {
        super(conf, dynConf, bkdlm);
        this.writeHandler = writeHandler;
        this.streamFailFast = conf.getFailFastOnStreamNotReady();
        this.disableRollOnSegmentError = conf.getDisableRollingOnLogSegmentError();

        // features
        disableLogSegmentRollingFeature = featureProvider.getFeature(CoreFeatureKeys.DISABLE_LOGSEGMENT_ROLLING.name().toLowerCase());
        // stats
        this.statsLogger = dlmStatsLogger.scope("log_writer");
        this.writeOpStatsLogger = statsLogger.getOpStatsLogger("write");
        this.markEndOfStreamOpStatsLogger = statsLogger.getOpStatsLogger("mark_end_of_stream");
        this.bulkWriteOpStatsLogger = statsLogger.getOpStatsLogger("bulk_write");
        this.getWriterOpStatsLogger = statsLogger.getOpStatsLogger("get_writer");
        this.pendingRequestDispatch = statsLogger.getCounter("pending_request_dispatch");
    }

    @VisibleForTesting
    synchronized void setLastTxId(long txId) {
        lastTxId = Math.max(lastTxId, txId);
    }

    @Override
    public synchronized long getLastTxId() {
        return lastTxId;
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

    private BKLogSegmentWriter getCachedLogSegmentWriter() throws WriteException {
        if (encounteredError) {
            throw new WriteException(bkDistributedLogManager.getStreamName(),
                    "writer has been closed due to error.");
        }
        BKLogSegmentWriter segmentWriter = getCachedLogWriter();
        if (null != segmentWriter
                && segmentWriter.isLogSegmentInError()
                && !disableRollOnSegmentError) {
            return null;
        } else {
            return segmentWriter;
        }
    }

    private Future<BKLogSegmentWriter> getLogSegmentWriter(long firstTxid,
                                                           boolean bestEffort,
                                                           boolean rollLog,
                                                           boolean allowMaxTxID) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        return doGetLogSegmentWriter(firstTxid, bestEffort, rollLog, allowMaxTxID)
                .addEventListener(new OpStatsListener<BKLogSegmentWriter>(getWriterOpStatsLogger, stopwatch));
    }

    private Future<BKLogSegmentWriter> doGetLogSegmentWriter(final long firstTxid,
                                                             final boolean bestEffort,
                                                             final boolean rollLog,
                                                             final boolean allowMaxTxID) {
        if (encounteredError) {
            return Future.exception(new WriteException(bkDistributedLogManager.getStreamName(),
                    "writer has been closed due to error."));
        }
        Future<BKLogSegmentWriter> writerFuture = asyncGetLedgerWriter(!disableRollOnSegmentError);
        if (null == writerFuture) {
            return rollLogSegmentIfNecessary(null, firstTxid, bestEffort, allowMaxTxID);
        } else if (rollLog) {
            return writerFuture.flatMap(new AbstractFunction1<BKLogSegmentWriter, Future<BKLogSegmentWriter>>() {
                @Override
                public Future<BKLogSegmentWriter> apply(BKLogSegmentWriter writer) {
                    return rollLogSegmentIfNecessary(writer, firstTxid, bestEffort, allowMaxTxID);
                }
            });
        } else {
            return writerFuture;
        }
    }

    /**
     * We write end of stream marker by writing a record with MAX_TXID, so we need to allow using
     * max txid when rolling for this case only.
     */
    private Future<BKLogSegmentWriter> getLogSegmentWriterForEndOfStream() {
        return getLogSegmentWriter(DistributedLogConstants.MAX_TXID,
                                     false /* bestEffort */,
                                     false /* roll log */,
                                     true /* allow max txid */);
    }

    private Future<BKLogSegmentWriter> getLogSegmentWriter(long firstTxid,
                                                           boolean bestEffort,
                                                           boolean rollLog) {
        return getLogSegmentWriter(firstTxid, bestEffort, rollLog, false /* allow max txid */);
    }

    Future<DLSN> queueRequest(LogRecord record, boolean flush) {
        PendingLogRecord pendingLogRecord = new PendingLogRecord(record, flush);
        pendingRequests.add(pendingLogRecord);
        return pendingLogRecord.promise;
    }

    boolean shouldRollLog(BKLogSegmentWriter w) {
        try {
            return null == w ||
                    (!disableLogSegmentRollingFeature.isAvailable() &&
                    shouldStartNewSegment(w));
        } catch (IOException ioe) {
            return false;
        }
    }

    void startQueueingRequests() {
        assert(null == pendingRequests && null == rollingFuture);
        pendingRequests = new LinkedList<PendingLogRecord>();
        rollingFuture = new Promise<BKLogSegmentWriter>();
    }

    // for ordering guarantee, we shouldn't send requests to next log segments until
    // previous log segment is done.
    private synchronized Future<DLSN> asyncWrite(final LogRecord record,
                                                 boolean flush) {
        // The passed in writer may be stale since we acquire the writer outside of sync
        // lock. If we recently rolled and the new writer is cached, use that instead.
        Future<DLSN> result = null;
        BKLogSegmentWriter w;
        try {
            w = getCachedLogSegmentWriter();
        } catch (WriteException we) {
            return Future.exception(we);
        }
        if (null != rollingFuture) {
            if (streamFailFast) {
                result = Future.exception(new StreamNotReadyException("Rolling log segment"));
            } else {
                result = queueRequest(record, flush);
            }
        } else if (shouldRollLog(w)) {
            // insert a last record, so when it called back, we will trigger a log segment rolling
            startQueueingRequests();
            if (null != w) {
                LastPendingLogRecord lastLogRecordInCurrentSegment = new LastPendingLogRecord(record, flush);
                w.asyncWrite(record, true).addEventListener(lastLogRecordInCurrentSegment);
                result = lastLogRecordInCurrentSegment.promise;
            } else { // no log segment yet. roll the log segment and issue pending requests.
                result = queueRequest(record, flush);
                rollLogSegmentAndIssuePendingRequests(record);
            }
        } else {
            result = w.asyncWrite(record, flush);
        }
        // use map here rather than onSuccess because we want lastTxId to be updated before
        // satisfying the future
        return result.map(new AbstractFunction1<DLSN, DLSN>() {
            @Override
            public DLSN apply(DLSN dlsn) {
                setLastTxId(record.getTransactionId());
                return dlsn;
            }
        });
    }

    private List<Future<DLSN>> asyncWriteBulk(List<LogRecord> records) {
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
        getLogSegmentWriter(record.getTransactionId(), true, true)
                .addEventListener(new FutureEventListener<BKLogSegmentWriter>() {
            @Override
            public void onSuccess(BKLogSegmentWriter writer) {
                try {
                    synchronized (BKAsyncLogWriter.this) {
                        for (PendingLogRecord pendingLogRecord : pendingRequests) {
                            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_LogWriterIssuePending);
                            writer.asyncWrite(pendingLogRecord.record, pendingLogRecord.flush)
                                    .addEventListener(pendingLogRecord);
                        }
                        if (null != rollingFuture) {
                            FutureUtils.setValue(rollingFuture, writer);
                        }
                        rollingFuture = null;
                        pendingRequestDispatch.add(pendingRequests.size());
                        pendingRequests = null;
                    }
                } catch (IOException ioe) {
                    errorOutPendingRequestsAndWriter(ioe);
                }
            }
            @Override
            public void onFailure(Throwable cause) {
                errorOutPendingRequestsAndWriter(cause);
            }
        });
    }

    @VisibleForTesting
    void errorOutPendingRequests(Throwable cause, boolean errorOutWriter) {
        final List<PendingLogRecord> pendingRequestsSnapshot;
        synchronized (this) {
            pendingRequestsSnapshot = pendingRequests;
            encounteredError = errorOutWriter;
            pendingRequests = null;
            if (null != rollingFuture) {
                FutureUtils.setException(rollingFuture, cause);
            }
            rollingFuture = null;
        }

        pendingRequestDispatch.add(pendingRequestsSnapshot.size());

        // After erroring out the writer above, no more requests
        // will be enqueued to pendingRequests
        for (PendingLogRecord pendingLogRecord : pendingRequestsSnapshot) {
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
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return asyncWrite(record, true)
                .addEventListener(new OpStatsListener<DLSN>(writeOpStatsLogger, stopwatch));
    }

    /**
     * Write many log records to the stream. The return type here is unfortunate but its a direct result
     * of having to combine FuturePool and the asyncWriteBulk method which returns a future as well. The
     * problem is the List that asyncWriteBulk returns can't be materialized until getLogSegmentWriter
     * completes, so it has to be wrapped in a future itself.
     *
     * @param records list of records
     */
    @Override
    public Future<List<Future<DLSN>>> writeBulk(final List<LogRecord> records) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return Future.value(asyncWriteBulk(records))
                .addEventListener(new OpStatsListener<List<Future<DLSN>>>(bulkWriteOpStatsLogger, stopwatch));
    }

    @Override
    public Future<Boolean> truncate(final DLSN dlsn) {
        if (DLSN.InvalidDLSN == dlsn) {
            return Future.value(false);
        }
        BKLogWriteHandler writeHandler;
        try {
            writeHandler = getWriteHandler();
        } catch (IOException e) {
            return Future.exception(e);
        }
        return writeHandler.setLogSegmentsOlderThanDLSNTruncated(dlsn).map(TruncationResultConverter);
    }

    Future<Long> flushAndCommit() {
        Future<BKLogSegmentWriter> writerFuture;
        synchronized (this) {
            if (null != this.rollingFuture) {
                writerFuture = this.rollingFuture;
            } else {
                writerFuture = getCachedLogWriterFuture();
            }
        }
        if (null == writerFuture) {
            return Future.value(lastTxId);
        }
        return writerFuture.flatMap(new AbstractFunction1<BKLogSegmentWriter, Future<Long>>() {
            @Override
            public Future<Long> apply(BKLogSegmentWriter writer) {
                return writer.flushAndCommit();
            }
        });
    }

    Future<Long> markEndOfStream() {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        Future<BKLogSegmentWriter> logSegmentWriterFuture;
        synchronized (this) {
            logSegmentWriterFuture = this.rollingFuture;
        }
        if (null == logSegmentWriterFuture) {
            logSegmentWriterFuture = getLogSegmentWriterForEndOfStream();
        }

        return logSegmentWriterFuture.flatMap(new AbstractFunction1<BKLogSegmentWriter, Future<Long>>() {
            @Override
            public Future<Long> apply(BKLogSegmentWriter w) {
                return w.markEndOfStream();
            }
        }).addEventListener(new OpStatsListener<Long>(markEndOfStreamOpStatsLogger, stopwatch));
    }

    @Override
    protected Future<Void> asyncCloseAndComplete() {
        Future<BKLogSegmentWriter> logSegmentWriterFuture;
        synchronized (this) {
            logSegmentWriterFuture = this.rollingFuture;
        }

        if (null == logSegmentWriterFuture) {
            return super.asyncCloseAndComplete();
        } else {
            return logSegmentWriterFuture.flatMap(new AbstractFunction1<BKLogSegmentWriter, Future<Void>>() {
                @Override
                public Future<Void> apply(BKLogSegmentWriter segmentWriter) {
                    return BKAsyncLogWriter.super.asyncCloseAndComplete();
                }
            });
        }
    }

    @Override
    void closeAndComplete() throws IOException {
        FutureUtils.result(asyncCloseAndComplete());
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
