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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import com.twitter.distributedlog.feature.CoreFeatureKeys;
import com.twitter.distributedlog.injector.FailureInjector;
import com.twitter.distributedlog.injector.RandomDelayFailureInjector;
import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.io.CompressionUtils;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.logsegment.LogSegmentEntryWriter;
import com.twitter.distributedlog.logsegment.LogSegmentWriter;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.stats.OpStatsListener;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.SafeQueueingFuturePool;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import com.twitter.distributedlog.util.Sizable;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static com.twitter.distributedlog.LogRecord.MAX_LOGRECORDSET_SIZE;

/**
 * BookKeeper Based Log Segment Writer.
 *
 * Multiple log records are packed into a single bookkeeper
 * entry before sending it over the network. The fact that the log record entries
 * are complete in the bookkeeper entries means that each bookkeeper log entry
 * can be read as a complete edit log. This is useful for reading, as we don't
 * need to read through the entire log segment to get the last written entry.
 *
 * <h3>Metrics</h3>
 *
 * <ul>
 * <li> flush/periodic/{success,miss}: counters for periodic flushes.
 * <li> data/{success,miss}: counters for data transmits.
 * <li> transmit/packetsize: opstats. characteristics of packet size for transmits.
 * <li> control/success: counter of success transmit of control records
 * <li> seg_writer/write: opstats. latency characteristics of write operations in segment writer.
 * <li> seg_writer/add_complete/{callback,queued,deferred}: opstats. latency components of add completions.
 * <li> seg_writer/pendings: counter. the number of records pending by the segment writers.
 * <li> transmit/outstanding/requests: per stream gauge. the number of outstanding transmits each stream.
 * </ul>
 */
class BKLogSegmentWriter implements LogSegmentWriter, AddCallback, Runnable, Sizable {
    static final Logger LOG = LoggerFactory.getLogger(BKLogSegmentWriter.class);

    private final String fullyQualifiedLogSegment;
    private final String streamName;
    private final int logSegmentMetadataVersion;
    private BKTransmitPacket packetPrevious;
    private Entry.Writer recordSetWriter;
    private final AtomicInteger outstandingTransmits;
    private final int transmissionThreshold;
    protected final LogSegmentEntryWriter entryWriter;
    private final CompressionCodec.Type compressionType;
    private final ReentrantLock transmitLock = new ReentrantLock();
    private final AtomicInteger transmitResult
        = new AtomicInteger(BKException.Code.OK);
    private final DistributedLock lock;
    private final boolean isDurableWriteEnabled;
    private DLSN lastDLSN = DLSN.InvalidDLSN;
    private final long startTxId;
    private long lastTxId = DistributedLogConstants.INVALID_TXID;
    private long lastTxIdAcknowledged = DistributedLogConstants.INVALID_TXID;
    private long outstandingBytes = 0;
    private long numFlushesSinceRestart = 0;
    private long numBytes = 0;
    private long lastEntryId = Long.MIN_VALUE;

    // Indicates whether there are writes that have been successfully transmitted that would need
    // a control record to be transmitted to make them visible to the readers by updating the last
    // add confirmed
    volatile private boolean controlFlushNeeded = false;
    private boolean immediateFlushEnabled = false;
    private int minDelayBetweenImmediateFlushMs = 0;
    private Stopwatch lastTransmit;
    private boolean streamEnded = false;
    private ScheduledFuture<?> periodicFlushSchedule = null;
    final private AtomicReference<ScheduledFuture<?>> transmitSchedFutureRef = new AtomicReference<ScheduledFuture<?>>(null);
    final private AtomicReference<ScheduledFuture<?>> immFlushSchedFutureRef = new AtomicReference<ScheduledFuture<?>>(null);
    final private AtomicReference<Exception> scheduledFlushException = new AtomicReference<Exception>(null);
    private boolean enforceLock = true;
    private Promise<Void> closeFuture = null;
    private final boolean enableRecordCounts;
    private int positionWithinLogSegment = 0;
    private final long logSegmentSequenceNumber;
    // Used only for values that *could* change (e.g. buffer size etc.)
    private final DistributedLogConfiguration conf;
    private final OrderedScheduler scheduler;

    // stats
    private final StatsLogger envelopeStatsLogger;
    private final Counter transmitDataSuccesses;
    private final Counter transmitDataMisses;
    private final OpStatsLogger transmitDataPacketSize;
    private final Counter transmitControlSuccesses;
    private final Counter pFlushSuccesses;
    private final Counter pFlushMisses;
    private final OpStatsLogger writeTime;
    private final OpStatsLogger addCompleteTime;
    private final OpStatsLogger addCompleteQueuedTime;
    private final OpStatsLogger addCompleteDeferredTime;
    private final Counter pendingWrites;

    // add complete processing
    private final SafeQueueingFuturePool<Void> addCompleteFuturePool;

    // Functions
    private final AbstractFunction1<Integer, Future<Long>> GET_LAST_TXID_ACKNOWLEDGED_AFTER_TRANSMIT_FUNC =
            new AbstractFunction1<Integer, Future<Long>>() {
                @Override
                public Future<Long> apply(Integer transmitRc) {
                    if (BKException.Code.OK == transmitRc) {
                        return Future.value(getLastTxIdAcknowledged());
                    } else {
                        return Future.exception(new BKTransmitException("Failed to transmit entry", transmitRc));
                    }
                }
            };
    final AbstractFunction1<Long, Future<Long>> COMMIT_AFTER_FLUSH_FUNC =
            new AbstractFunction1<Long, Future<Long>>() {
                @Override
                public Future<Long> apply(Long lastAckedTxId) {
                    return commit();
                }
            };

    private final AlertStatsLogger alertStatsLogger;
    private final WriteLimiter writeLimiter;
    private final FailureInjector writeDelayInjector;

    /**
     * Construct an edit log output stream which writes to a ledger.
     */
    protected BKLogSegmentWriter(String streamName,
                                 String logSegmentName,
                                 DistributedLogConfiguration conf,
                                 int logSegmentMetadataVersion,
                                 LogSegmentEntryWriter entryWriter,
                                 DistributedLock lock, /** the lock needs to be acquired **/
                                 long startTxId,
                                 long logSegmentSequenceNumber,
                                 OrderedScheduler scheduler,
                                 StatsLogger statsLogger,
                                 StatsLogger perLogStatsLogger,
                                 AlertStatsLogger alertStatsLogger,
                                 PermitLimiter globalWriteLimiter,
                                 FeatureProvider featureProvider,
                                 DynamicDistributedLogConfiguration dynConf)
        throws IOException {
        super();

        // set up a write limiter
        PermitLimiter streamWriteLimiter = null;
        if (conf.getPerWriterOutstandingWriteLimit() < 0) {
            streamWriteLimiter = PermitLimiter.NULL_PERMIT_LIMITER;
        } else {
            Feature disableWriteLimitFeature = featureProvider.getFeature(
                CoreFeatureKeys.DISABLE_WRITE_LIMIT.name().toLowerCase());
            streamWriteLimiter = new SimplePermitLimiter(
                conf.getOutstandingWriteLimitDarkmode(),
                conf.getPerWriterOutstandingWriteLimit(),
                statsLogger.scope("streamWriteLimiter"),
                false,
                disableWriteLimitFeature);
        }
        this.writeLimiter = new WriteLimiter(streamName, streamWriteLimiter, globalWriteLimiter);
        this.alertStatsLogger = alertStatsLogger;
        this.envelopeStatsLogger = BroadCastStatsLogger.masterslave(statsLogger, perLogStatsLogger);

        StatsLogger flushStatsLogger = statsLogger.scope("flush");
        StatsLogger pFlushStatsLogger = flushStatsLogger.scope("periodic");
        pFlushSuccesses = pFlushStatsLogger.getCounter("success");
        pFlushMisses = pFlushStatsLogger.getCounter("miss");

        // transmit
        StatsLogger transmitDataStatsLogger = statsLogger.scope("data");
        transmitDataSuccesses = transmitDataStatsLogger.getCounter("success");
        transmitDataMisses = transmitDataStatsLogger.getCounter("miss");
        StatsLogger transmitStatsLogger = statsLogger.scope("transmit");
        transmitDataPacketSize =  transmitStatsLogger.getOpStatsLogger("packetsize");
        StatsLogger transmitControlStatsLogger = statsLogger.scope("control");
        transmitControlSuccesses = transmitControlStatsLogger.getCounter("success");
        StatsLogger segWriterStatsLogger = statsLogger.scope("seg_writer");
        writeTime = segWriterStatsLogger.getOpStatsLogger("write");
        addCompleteTime = segWriterStatsLogger.scope("add_complete").getOpStatsLogger("callback");
        addCompleteQueuedTime = segWriterStatsLogger.scope("add_complete").getOpStatsLogger("queued");
        addCompleteDeferredTime = segWriterStatsLogger.scope("add_complete").getOpStatsLogger("deferred");
        pendingWrites = segWriterStatsLogger.getCounter("pending");

        // outstanding transmit requests
        StatsLogger transmitOutstandingLogger = perLogStatsLogger.scope("transmit").scope("outstanding");
        transmitOutstandingLogger.registerGauge("requests", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }
            @Override
            public Number getSample() {
                return outstandingTransmits.get();
            }
        });

        outstandingTransmits = new AtomicInteger(0);
        this.fullyQualifiedLogSegment = streamName + ":" + logSegmentName;
        this.streamName = streamName;
        this.logSegmentMetadataVersion = logSegmentMetadataVersion;
        this.entryWriter = entryWriter;
        this.lock = lock;
        this.lock.checkOwnershipAndReacquire();

        final int configuredTransmissionThreshold = dynConf.getOutputBufferSize();
        if (configuredTransmissionThreshold > MAX_LOGRECORDSET_SIZE) {
            LOG.warn("Setting output buffer size {} greater than max transmission size {} for log segment {}",
                new Object[] {configuredTransmissionThreshold, MAX_LOGRECORDSET_SIZE, fullyQualifiedLogSegment});
            this.transmissionThreshold = MAX_LOGRECORDSET_SIZE;
        } else {
            this.transmissionThreshold = configuredTransmissionThreshold;
        }
        this.compressionType = CompressionUtils.stringToType(conf.getCompressionType());

        this.logSegmentSequenceNumber = logSegmentSequenceNumber;
        this.recordSetWriter = Entry.newEntry(
                streamName,
                Math.max(transmissionThreshold, 1024),
                envelopeBeforeTransmit(),
                compressionType,
                envelopeStatsLogger);
        this.packetPrevious = null;
        this.startTxId = startTxId;
        this.lastTxId = startTxId;
        this.lastTxIdAcknowledged = startTxId;
        this.enableRecordCounts = conf.getEnableRecordCounts();
        this.immediateFlushEnabled = conf.getImmediateFlushEnabled();
        this.isDurableWriteEnabled = dynConf.isDurableWriteEnabled();
        this.scheduler = scheduler;

        // Failure injection
        if (conf.getEIInjectWriteDelay()) {
            this.writeDelayInjector = new RandomDelayFailureInjector(dynConf);
        } else {
            this.writeDelayInjector = FailureInjector.NULL;
        }

        // If we are transmitting immediately (threshold == 0) and if immediate
        // flush is enabled, we don't need the periodic flush task
        final int configuredPeriodicFlushFrequency = dynConf.getPeriodicFlushFrequencyMilliSeconds();
        if (!immediateFlushEnabled || (0 != this.transmissionThreshold)) {
            int periodicFlushFrequency = configuredPeriodicFlushFrequency;
            if (periodicFlushFrequency > 0 && scheduler != null) {
                periodicFlushSchedule = scheduler.scheduleAtFixedRate(this,
                        periodicFlushFrequency/2, periodicFlushFrequency/2, TimeUnit.MILLISECONDS);
            }
        } else {
            // Min delay heuristic applies only when immediate flush is enabled
            // and transmission threshold is zero
            minDelayBetweenImmediateFlushMs = conf.getMinDelayBetweenImmediateFlushMs();
        }

        this.conf = conf;
        if (null != scheduler) {
            this.addCompleteFuturePool = new SafeQueueingFuturePool<Void>(scheduler.getFuturePool(streamName));
        } else {
            this.addCompleteFuturePool = null;
        }
        assert(!this.immediateFlushEnabled || (null != this.scheduler));
        this.lastTransmit = Stopwatch.createStarted();
    }

    String getFullyQualifiedLogSegment() {
        return fullyQualifiedLogSegment;
    }

    @VisibleForTesting
    DistributedLock getLock() {
        return this.lock;
    }

    @VisibleForTesting
    FuturePool getFuturePool() {
        if (null == scheduler) {
            return null;
        }
        return scheduler.getFuturePool(streamName);
    }

    @VisibleForTesting
    void setTransmitResult(int rc) {
        transmitResult.set(rc);
    }

    @VisibleForTesting
    protected final LogSegmentEntryWriter getEntryWriter() {
        return this.entryWriter;
    }

    @Override
    public long getLogSegmentId() {
        return this.entryWriter.getLogSegmentId();
    }

    protected final long getLogSegmentSequenceNumber() {
        return logSegmentSequenceNumber;
    }

    /**
     * Get the start tx id of the log segment.
     *
     * @return start tx id of the log segment.
     */
    protected final long getStartTxId() {
        return startTxId;
    }

    /**
     * Get the last tx id that has been written to the log segment buffer but not committed yet.
     *
     * @return last tx id that has been written to the log segment buffer but not committed yet.
     * @see #getLastTxIdAcknowledged()
     */
    synchronized long getLastTxId() {
        return lastTxId;
    }

    /**
     * Get the last tx id that has been acknowledged.
     *
     * @return last tx id that has been acknowledged.
     * @see #getLastTxId()
     */
    synchronized long getLastTxIdAcknowledged() {
        return lastTxIdAcknowledged;
    }

    /**
     * Get the position-within-logsemgnet of the last written log record.
     *
     * @return position-within-logsegment of the last written log record.
     */
    int getPositionWithinLogSegment() {
        return positionWithinLogSegment;
    }

    @VisibleForTesting
    long getLastEntryId() {
        return lastEntryId;
    }

    /**
     * Get the last dlsn of the last acknowledged record.
     *
     * @return last dlsn of the last acknowledged record.
     */
    synchronized DLSN getLastDLSN() {
        return lastDLSN;
    }

    @Override
    public long size() {
        return entryWriter.size();
    }

    private synchronized int getAverageTransmitSize() {
        if (numFlushesSinceRestart > 0) {
            long ret = numBytes/numFlushesSinceRestart;

            if (ret < Integer.MIN_VALUE || ret > Integer.MAX_VALUE) {
                throw new IllegalArgumentException
                    (ret + " transmit size should never exceed max transmit size");
            }
            return (int) ret;
        }

        return 0;
    }

    private Entry.Writer newRecordSetWriter() {
        return Entry.newEntry(
                streamName,
                Math.max(transmissionThreshold, getAverageTransmitSize()),
                envelopeBeforeTransmit(),
                compressionType,
                envelopeStatsLogger);
    }

    private boolean envelopeBeforeTransmit() {
        return LogSegmentMetadata.supportsEnvelopedEntries(logSegmentMetadataVersion);
    }

    @Override
    public Future<Void> asyncClose() {
        return closeInternal(false);
    }

    @Override
    public Future<Void> asyncAbort() {
        return closeInternal(true);
    }

    private void flushAddCompletes() {
        if (null != addCompleteFuturePool) {
            addCompleteFuturePool.close();
        }
    }

    private synchronized void abortPacket(BKTransmitPacket packet) {
        long numRecords = 0;
        if (null != packet) {
            EntryBuffer recordSet = packet.getRecordSet();
            numRecords = recordSet.getNumRecords();
            int rc = transmitResult.get();
            if (BKException.Code.OK == rc) {
                rc = BKException.Code.InterruptedException;
            }
            Throwable reason = new WriteCancelledException(streamName, FutureUtils.transmitException(rc));
            recordSet.abortTransmit(reason);
        }
        LOG.info("Stream {} aborted {} writes", fullyQualifiedLogSegment, numRecords);
    }

    private synchronized long getWritesPendingTransmit() {
        if (null != recordSetWriter) {
            return recordSetWriter.getNumRecords();
        } else {
            return 0;
        }
    }

    private synchronized long getPendingAddCompleteCount() {
        if (null != addCompleteFuturePool) {
            return addCompleteFuturePool.size();
        } else {
            return 0;
        }
    }

    private Future<Void> closeInternal(boolean abort) {
        Promise<Void> closePromise;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closePromise = closeFuture = new Promise<Void>();
        }

        AtomicReference<Throwable> throwExc = new AtomicReference<Throwable>(null);
        closeInternal(abort, throwExc, closePromise);
        return closePromise;
    }

    private void closeInternal(final boolean abort,
                               final AtomicReference<Throwable> throwExc,
                               final Promise<Void> closePromise) {
        // Cancel the periodic flush schedule first
        // The task is allowed to exit gracefully
        if (null != periodicFlushSchedule) {
            // we don't need to care about the cancel result here. if the periodicl flush task couldn't
            // be cancelled, it means that it is doing flushing. So following flushes would be synchronized
            // to wait until background flush completed.
            if (!periodicFlushSchedule.cancel(false)) {
                LOG.info("Periodic flush for log segment {} isn't cancelled.", getFullyQualifiedLogSegment());
            }
        }

        // If it is a normal close and the stream isn't in an error state, we attempt to flush any buffered data
        if (!abort && !isLogSegmentInError()) {
            this.enforceLock = false;
            LOG.info("Flushing before closing log segment {}", getFullyQualifiedLogSegment());
            flushAndCommit().addEventListener(new FutureEventListener<Long>() {
                @Override
                public void onSuccess(Long value) {
                    abortTransmitPacketOnClose(abort, throwExc, closePromise);
                }

                @Override
                public void onFailure(Throwable cause) {
                    throwExc.set(cause);
                    abortTransmitPacketOnClose(abort, throwExc, closePromise);
                }
            });
        } else {
            abortTransmitPacketOnClose(abort, throwExc, closePromise);
        }

    }

    private void abortTransmitPacketOnClose(final boolean abort,
                                            final AtomicReference<Throwable> throwExc,
                                            final Promise<Void> closePromise) {
        LOG.info("Closing BKPerStreamLogWriter (abort={}) for {} :" +
                        " lastDLSN = {} outstandingTransmits = {} writesPendingTransmit = {} addCompletesPending = {}",
                new Object[]{abort, fullyQualifiedLogSegment, getLastDLSN(),
                        outstandingTransmits.get(), getWritesPendingTransmit(), getPendingAddCompleteCount()});

        // Save the current packet to reset, leave a new empty packet to avoid a race with
        // addCompleteDeferredProcessing.
        final BKTransmitPacket packetPreviousSaved;
        final BKTransmitPacket packetCurrentSaved;
        synchronized (this) {
            packetPreviousSaved = packetPrevious;
            packetCurrentSaved = new BKTransmitPacket(recordSetWriter);
            recordSetWriter = newRecordSetWriter();
        }

        // Once the last packet been transmitted, apply any remaining promises asynchronously
        // to avoid blocking close if bk client is slow for some reason.
        if (null != packetPreviousSaved) {
            packetPreviousSaved.addTransmitCompleteListener(new FutureEventListener<Integer>() {
                @Override
                public void onSuccess(Integer transmitResult) {
                    flushAddCompletes();
                    abortPacket(packetCurrentSaved);
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.error("Unexpected error on transmit completion ", cause);
                }
            });
        } else {
            // In this case there are no pending add completes, but we still need to abort the
            // current packet.
            abortPacket(packetCurrentSaved);
        }
        closeLedgerOnClose(abort, throwExc, closePromise);
    }

    private void closeLedgerOnClose(final boolean abort,
                                    final AtomicReference<Throwable> throwExc,
                                    final Promise<Void> closePromise) {
        // close the log segment if it isn't in error state, so all the outstanding addEntry(s) will callback.
        if (null == throwExc.get() && !isLogSegmentInError()) {
            // Synchronous closing the ledger handle, if we couldn't close a ledger handle successfully.
            // we should throw the exception to #closeToFinalize, so it would fail completing a log segment.
            entryWriter.asyncClose(new CloseCallback() {
                @Override
                public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                    if (BKException.Code.OK != rc && BKException.Code.LedgerClosedException != rc) {
                        if (!abort) {
                            throwExc.set(new IOException("Failed to close ledger for " + fullyQualifiedLogSegment + " : " +
                                    BKException.getMessage(rc)));
                        }
                    }
                    completeClosePromise(abort, throwExc, closePromise);
                }
            }, null);
        } else {
            completeClosePromise(abort, throwExc, closePromise);
        }
    }

    private void completeClosePromise(final boolean abort,
                                      final AtomicReference<Throwable> throwExc,
                                      final Promise<Void> closePromise) {
        // If add entry failed because of closing ledger above, we don't need to fail the close operation
        if (!abort && null == throwExc.get() && shouldFailCompleteLogSegment()) {
            throwExc.set(new BKTransmitException("Closing an errored stream : ", transmitResult.get()));
        }

        if (null == throwExc.get()) {
            FutureUtils.setValue(closePromise, null);
        } else {
            FutureUtils.setException(closePromise, throwExc.get());
        }
    }

    @Override
    synchronized public void write(LogRecord record) throws IOException {
        writeUserRecord(record);
        flushIfNeeded();
    }

    @Override
    synchronized public Future<DLSN> asyncWrite(LogRecord record) {
        return asyncWrite(record, true);
    }

    synchronized public Future<DLSN> asyncWrite(LogRecord record, boolean flush) {
        Future<DLSN> result = null;
        try {
            if (record.isControl()) {
                // we don't pack control records with user records together
                // so transmit current output buffer if possible
                try {
                    transmit();
                } catch (IOException ioe) {
                    return Future.exception(new WriteCancelledException(fullyQualifiedLogSegment, ioe));
                }
                result = writeControlLogRecord(record);
                transmit();
            } else {
                result = writeUserRecord(record);
                if (!isDurableWriteEnabled) {
                    // we have no idea about the DLSN if durability is turned off.
                    result = Future.value(DLSN.InvalidDLSN);
                }
                if (flush) {
                    flushIfNeeded();
                }
            }
        } catch (IOException ioe) {
            // We may incorrectly report transmit failure here, but only if we happened to hit
            // packet/xmit size limit conditions AND fail flush above, which should happen rarely
            if (null != result) {
                LOG.error("Overriding first result with flush failure {}", result);
            }
            result = Future.exception(ioe);

            // Flush to ensure any prev. writes with flush=false are flushed despite failure.
            flushIfNeededNoThrow();
        }
        return result;
    }

    synchronized private Future<DLSN> writeUserRecord(LogRecord record) throws IOException {
        if (null != closeFuture) {
            throw new WriteException(fullyQualifiedLogSegment, BKException.getMessage(BKException.Code.LedgerClosedException));
        }

        if (BKException.Code.OK != transmitResult.get()) {
            // Failfast if the stream already encountered error with safe retry on the client
            throw new WriteException(fullyQualifiedLogSegment, BKException.getMessage(transmitResult.get()));
        }

        if (streamEnded) {
            throw new EndOfStreamException("Writing to a stream after it has been marked as completed");
        }

        if ((record.getTransactionId() < 0) ||
            (record.getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new TransactionIdOutOfOrderException(record.getTransactionId());
        }

        // Inject write delay if configured to do so
        writeDelayInjector.inject();

        // Will check write rate limits and throw if exceeded.
        writeLimiter.acquire();
        pendingWrites.inc();

        // The count represents the number of user records up to the
        // current record
        // Increment the record count only when writing a user log record
        // Internally generated log records don't increment the count
        // writeInternal will always set a count regardless of whether it was
        // incremented or not.
        Future<DLSN> future = null;
        try {
            // increment the position for the record to write
            // if the record is failed to write, it would be decremented.
            positionWithinLogSegment++;
            int numRecords = 1;
            if (record.isRecordSet()) {
                numRecords = LogRecordSet.numRecords(record);
            }
            future = writeInternal(record);
            // after the record (record set) is written, the position should be
            // moved for {numRecords}, but since we already moved the record by 1
            // so advance the position for other {numRecords - 1}.
            positionWithinLogSegment += (numRecords - 1);
        } catch (IOException ex) {
            writeLimiter.release();
            pendingWrites.dec();
            positionWithinLogSegment--;
            throw ex;
        }

        // Track outstanding requests and return the future.
        return future.ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                pendingWrites.dec();
                writeLimiter.release();
                return null;
            }
        });
    }

    boolean isLogSegmentInError() {
        return (transmitResult.get() != BKException.Code.OK);
    }

    boolean shouldFailCompleteLogSegment() {
        return (transmitResult.get() != BKException.Code.OK) &&
                (transmitResult.get() != BKException.Code.LedgerClosedException);
    }

    synchronized public Future<DLSN> writeInternal(LogRecord record)
            throws LogRecordTooLongException, LockingException, BKTransmitException,
                   WriteException, InvalidEnvelopedEntryException {
        int logRecordSize = record.getPersistentSize();

        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(String.format(
                    "Log Record of size %d written when only %d is allowed",
                    logRecordSize, MAX_LOGRECORD_SIZE));
        }

        // If we will exceed the max number of bytes allowed per entry
        // initiate a transmit before accepting the new log record
        if ((recordSetWriter.getNumBytes() + logRecordSize) > MAX_LOGRECORDSET_SIZE) {
            checkStateAndTransmit();
        }

        checkWriteLock();

        if (enableRecordCounts) {
            // Set the count here. The caller would appropriately increment it
            // if this log record is to be counted
            record.setPositionWithinLogSegment(positionWithinLogSegment);
        }

        Promise<DLSN> writePromise = new Promise<DLSN>();
        writePromise.addEventListener(new OpStatsListener<DLSN>(writeTime));
        recordSetWriter.writeRecord(record, writePromise);

        if (record.getTransactionId() < lastTxId) {
            LOG.info("Log Segment {} TxId decreased Last: {} Record: {}",
                    new Object[] {fullyQualifiedLogSegment, lastTxId, record.getTransactionId()});
        }
        if (!record.isControl()) {
            // only update last tx id for user records
            lastTxId = record.getTransactionId();
            outstandingBytes += (20 + record.getPayload().length);
        }
        return writePromise;
    }

    synchronized private Future<DLSN> writeControlLogRecord()
            throws BKTransmitException, WriteException, InvalidEnvelopedEntryException,
                   LockingException, LogRecordTooLongException {
        LogRecord controlRec = new LogRecord(lastTxId, DistributedLogConstants.CONTROL_RECORD_CONTENT);
        controlRec.setControl();
        return writeControlLogRecord(controlRec);
    }

    synchronized private Future<DLSN> writeControlLogRecord(LogRecord record)
            throws BKTransmitException, WriteException, InvalidEnvelopedEntryException,
                   LockingException, LogRecordTooLongException {
        return writeInternal(record);
    }

    /**
     * We write a special log record that marks the end of the stream. Since this is the last
     * log record in the stream, it is marked with MAX_TXID. MAX_TXID also has the useful
     * side-effect of disallowing future startLogSegment calls through the MaxTxID check
     *
     * @throws IOException
     */
    synchronized private void writeEndOfStreamMarker() throws IOException {
        LogRecord endOfStreamRec = new LogRecord(DistributedLogConstants.MAX_TXID, "endOfStream".getBytes(UTF_8));
        endOfStreamRec.setEndOfStream();
        writeInternal(endOfStreamRec);
    }

    /**
     * Flushes all the data up to this point,
     * adds the end of stream marker and marks the stream
     * as read-only in the metadata. No appends to the
     * stream will be allowed after this point
     */
    public Future<Long> markEndOfStream() {
        synchronized (this) {
            try {
                writeEndOfStreamMarker();
            } catch (IOException e) {
                return Future.exception(e);
            }
            streamEnded = true;
        }
        return flushAndCommit();
    }

    /**
     * Write bulk of records.
     *
     * (TODO: moved this method to log writer level)
     *
     * @param records list of records to write
     * @return number of records that has been written
     * @throws IOException when there is I/O errors during writing records.
     */
    synchronized public int writeBulk(List<LogRecord> records) throws IOException {
        int numRecords = 0;
        for (LogRecord r : records) {
            write(r);
            numRecords++;
        }
        return numRecords;
    }

    private void checkStateBeforeTransmit() throws WriteException {
        try {
            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_TransmitBeforeAddEntry);
        } catch (IOException e) {
            throw new WriteException(streamName, "Fail transmit before adding entries");
        }
    }

    /**
     * Transmit the output buffer data to the backend.
     *
     * @return last txn id that already acknowledged
     * @throws BKTransmitException if the segment writer is already in error state
     * @throws LockingException if the segment writer lost lock before transmit
     * @throws WriteException if failed to create the envelope for the data to transmit
     * @throws InvalidEnvelopedEntryException when built an invalid enveloped entry
     */
    synchronized void checkStateAndTransmit()
            throws BKTransmitException, WriteException, InvalidEnvelopedEntryException, LockingException {
        checkStateBeforeTransmit();
        transmit();
    }

    @Override
    public synchronized Future<Long> flush() {
        try {
            checkStateBeforeTransmit();
        } catch (WriteException e) {
            return Future.exception(e);
        }

        Future<Integer> transmitFuture;
        try {
            transmitFuture = transmit();
        } catch (BKTransmitException e) {
            return Future.exception(e);
        } catch (LockingException e) {
            return Future.exception(e);
        } catch (WriteException e) {
            return Future.exception(e);
        } catch (InvalidEnvelopedEntryException e) {
            return Future.exception(e);
        }

        if (null == transmitFuture) {
            if (null != packetPrevious) {
                transmitFuture = packetPrevious.getTransmitFuture();
            }  else {
                return Future.value(getLastTxIdAcknowledged());
            }
        }

        return transmitFuture.flatMap(GET_LAST_TXID_ACKNOWLEDGED_AFTER_TRANSMIT_FUNC);
    }

    @Override
    public synchronized Future<Long> commit() {
        // we don't pack control records with user records together
        // so transmit current output buffer if possible
        Future<Integer> transmitFuture;
        try {
            try {
                transmitFuture = transmit();
            } catch (IOException ioe) {
                return Future.exception(ioe);
            }
            if (null == transmitFuture) {
                writeControlLogRecord();
                return flush();
            }
        } catch (IOException ioe) {
            return Future.exception(ioe);
        }
        return transmitFuture.flatMap(GET_LAST_TXID_ACKNOWLEDGED_AFTER_TRANSMIT_FUNC);
    }

    Future<Long> flushAndCommit() {
        return flush().flatMap(COMMIT_AFTER_FLUSH_FUNC);
    }

    void flushIfNeededNoThrow() {
        try {
            flushIfNeeded();
        } catch (IOException ioe) {
            LOG.error("Encountered exception while flushing log records to stream {}",
                fullyQualifiedLogSegment, ioe);
        }
    }

    void scheduleFlushWithDelayIfNeeded(final Callable<?> callable,
                                        final AtomicReference<ScheduledFuture<?>> scheduledFutureRef) {
        final long delayMs = Math.max(0, minDelayBetweenImmediateFlushMs - lastTransmit.elapsed(TimeUnit.MILLISECONDS));
        final ScheduledFuture<?> scheduledFuture = scheduledFutureRef.get();
        if ((null == scheduledFuture) || scheduledFuture.isDone()) {
            scheduledFutureRef.set(scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized(this) {
                        scheduledFutureRef.set(null);
                        try {
                            callable.call();

                            // Flush was successful or wasn't needed, the exception should be unset.
                            scheduledFlushException.set(null);
                        } catch (Exception exc) {
                            scheduledFlushException.set(exc);
                            LOG.error("Delayed flush failed", exc);
                        }
                    }
                }
            }, delayMs, TimeUnit.MILLISECONDS));
        }
    }

    // Based on transmit buffer size, immediate flush, etc., should we flush the current
    // packet now.
    void flushIfNeeded() throws BKTransmitException, WriteException, InvalidEnvelopedEntryException,
            LockingException, FlushException {
        if (outstandingBytes > transmissionThreshold) {
            // If flush delay is disabled, flush immediately, else schedule appropriately.
            if (0 == minDelayBetweenImmediateFlushMs) {
                checkStateAndTransmit();
            } else {
                scheduleFlushWithDelayIfNeeded(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        checkStateAndTransmit();
                        return null;
                    }
                }, transmitSchedFutureRef);

                // Timing here is not very important--the last flush failed and we should
                // indicate this to the caller. The next flush may succeed and unset the
                // scheduledFlushException in which case the next write will succeed (if the caller
                // hasn't already closed the writer).
                if (scheduledFlushException.get() != null) {
                    throw new FlushException("Last flush encountered an error while writing data to the backend",
                        getLastTxId(), getLastTxIdAcknowledged(), scheduledFlushException.get());
                }
            }
        }
    }

    private void checkWriteLock() throws LockingException {
        try {
            if (FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_WriteInternalLostLock)) {
                throw new LockingException("/failpoint/lockpath", "failpoint is simulating a lost lock"
                        + getFullyQualifiedLogSegment());
            }
        } catch (IOException e) {
            throw new LockingException("/failpoint/lockpath", "failpoint is simulating a lost lock for "
                    + getFullyQualifiedLogSegment());
        }
        if (enforceLock) {
            lock.checkOwnershipAndReacquire();
        }
    }

    /**
     * Transmit the current buffer to bookkeeper.
     * Synchronised at the class. #write() and #setReadyToFlush()
     * are never called at the same time.
     *
     * NOTE: This method should only throw known exceptions so that we don't accidentally
     *       add new code that throws in an inappropriate place.
     *
     * @return a transmit future for caller to wait for transmit result if we transmit successfully,
     *         null if no data to transmit
     * @throws BKTransmitException if the segment writer is already in error state
     * @throws LockingException if the segment writer lost lock before transmit
     * @throws WriteException if failed to create the envelope for the data to transmit
     * @throws InvalidEnvelopedEntryException when built an invalid enveloped entry
     */
    private Future<Integer> transmit()
        throws BKTransmitException, LockingException, WriteException, InvalidEnvelopedEntryException {
        EntryBuffer recordSetToTransmit;
        transmitLock.lock();
        try {
            synchronized (this) {
                checkWriteLock();
                // If transmitResult is anything other than BKException.Code.OK, it means that the
                // stream has encountered an error and cannot be written to.
                if (!transmitResult.compareAndSet(BKException.Code.OK,
                                                  BKException.Code.OK)) {
                    LOG.error("Log Segment {} Trying to write to an errored stream; Error is {}",
                              fullyQualifiedLogSegment,
                              BKException.getMessage(transmitResult.get()));
                    throw new BKTransmitException("Trying to write to an errored stream;"
                                                          + " Error code : (" + transmitResult.get()
                                                          + ") " + BKException.getMessage(transmitResult.get()), transmitResult.get());
                }

                if (recordSetWriter.getNumRecords() == 0) {
                    // Control flushes always have at least the control record to flush
                    transmitDataMisses.inc();
                    return null;
                }

                recordSetToTransmit = recordSetWriter;
                recordSetWriter = newRecordSetWriter();
                outstandingBytes = 0;

                if (recordSetToTransmit.hasUserRecords()) {
                    numBytes += recordSetToTransmit.getNumBytes();
                    numFlushesSinceRestart++;
                }
            }

            Buffer toSend;
            try {
                toSend = recordSetToTransmit.getBuffer();
                FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_TransmitFailGetBuffer);
            } catch (IOException e) {
                if (e instanceof InvalidEnvelopedEntryException) {
                    alertStatsLogger.raise("Invalid enveloped entry for segment {} : ", fullyQualifiedLogSegment, e);
                }
                LOG.error("Exception while enveloping entries for segment: {}",
                          new Object[] {fullyQualifiedLogSegment}, e);
                // If a write fails here, we need to set the transmit result to an error so that
                // no future writes go through and violate ordering guarantees.
                transmitResult.set(BKException.Code.WriteException);
                if (e instanceof InvalidEnvelopedEntryException) {
                    alertStatsLogger.raise("Invalid enveloped entry for segment {} : ", fullyQualifiedLogSegment, e);
                    throw (InvalidEnvelopedEntryException) e;
                } else {
                    throw new WriteException(streamName, "Envelope Error");
                }
            }

            synchronized (this) {
                BKTransmitPacket packet = new BKTransmitPacket(recordSetToTransmit);
                packetPrevious = packet;
                entryWriter.asyncAddEntry(toSend.getData(), 0, toSend.size(),
                                          this, packet);

                if (recordSetToTransmit.hasUserRecords()) {
                    transmitDataSuccesses.inc();
                } else {
                    transmitControlSuccesses.inc();
                }

                lastTransmit.reset().start();
                outstandingTransmits.incrementAndGet();
                controlFlushNeeded = false;
                return packet.getTransmitFuture();
            }
        } finally {
            transmitLock.unlock();
        }
    }

    /**
     *  Checks if there is any data to transmit so that the periodic flush
     *  task can determine if there is anything it needs to do
     */
    synchronized private boolean haveDataToTransmit() {
        if (!transmitResult.compareAndSet(BKException.Code.OK, BKException.Code.OK)) {
            // Even if there is data it cannot be transmitted, so effectively nothing to send
            return false;
        }

        return (recordSetWriter.getNumRecords() > 0);
    }

    @Override
    public void addComplete(final int rc, LedgerHandle handle,
                            final long entryId, final Object ctx) {
        final AtomicReference<Integer> effectiveRC = new AtomicReference<Integer>(rc);
        try {
            if (FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_TransmitComplete)) {
                effectiveRC.set(BKException.Code.UnexpectedConditionException);
            }
        } catch (Exception exc) {
            effectiveRC.set(BKException.Code.UnexpectedConditionException);
        }

        // Sanity check to make sure we're receiving these callbacks in order.
        if (entryId > -1 && lastEntryId >= entryId) {
            LOG.error("Log segment {} saw out of order entry {} lastEntryId {}",
                new Object[] {fullyQualifiedLogSegment, entryId, lastEntryId});
        }
        lastEntryId = entryId;

        assert (ctx instanceof BKTransmitPacket);
        final BKTransmitPacket transmitPacket = (BKTransmitPacket) ctx;

        // Time from transmit until receipt of addComplete callback
        addCompleteTime.registerSuccessfulEvent(TimeUnit.MICROSECONDS.convert(
            System.nanoTime() - transmitPacket.getTransmitTime(), TimeUnit.NANOSECONDS));

        if (BKException.Code.OK == rc) {
            EntryBuffer recordSet = transmitPacket.getRecordSet();
            if (recordSet.hasUserRecords()) {
                synchronized (this) {
                    lastTxIdAcknowledged = Math.max(lastTxIdAcknowledged, recordSet.getMaxTxId());
                }
            }
        }

        if (null != addCompleteFuturePool) {
            final Stopwatch queuedTime = Stopwatch.createStarted();
            addCompleteFuturePool.apply(new Function0<Void>() {
                public Void apply() {
                    final Stopwatch deferredTime = Stopwatch.createStarted();
                    addCompleteQueuedTime.registerSuccessfulEvent(queuedTime.elapsed(TimeUnit.MICROSECONDS));
                    addCompleteDeferredProcessing(transmitPacket, entryId, effectiveRC.get());
                    addCompleteDeferredTime.registerSuccessfulEvent(deferredTime.elapsed(TimeUnit.MICROSECONDS));
                    return null;
                }
                @Override
                public String toString() {
                    return String.format("AddComplete(Stream=%s, entryId=%d, rc=%d)",
                            fullyQualifiedLogSegment, entryId, rc);
                }
            }).addEventListener(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void done) {
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.error("addComplete processing failed for {} entry {} lastTxId {} rc {} with error",
                        new Object[] {fullyQualifiedLogSegment, entryId, transmitPacket.getRecordSet().getMaxTxId(), rc, cause});
                }
            });
            // Race condition if we notify before the addComplete is enqueued.
            transmitPacket.notifyTransmitComplete(effectiveRC.get());
            outstandingTransmits.getAndDecrement();
        } else {
            // Notify transmit complete must be called before deferred processing in the
            // sync case since otherwise callbacks in deferred processing may deadlock.
            transmitPacket.notifyTransmitComplete(effectiveRC.get());
            outstandingTransmits.getAndDecrement();
            addCompleteDeferredProcessing(transmitPacket, entryId, effectiveRC.get());
        }
    }

    private void addCompleteDeferredProcessing(final BKTransmitPacket transmitPacket,
                                               final long entryId,
                                               final int rc) {
        boolean cancelPendingPromises = false;
        EntryBuffer recordSet = transmitPacket.getRecordSet();
        synchronized (this) {
            if (transmitResult.compareAndSet(BKException.Code.OK, rc)) {
                // If this is the first time we are setting an error code in the transmitResult then
                // we must cancel pending promises; once this error has been set, more records will not
                // be enqueued; they will be failed with WriteException
                cancelPendingPromises = (BKException.Code.OK != rc);
            } else {
                LOG.warn("Log segment {} entryId {}: Tried to set transmit result to ({}) but is already ({})",
                    new Object[] {fullyQualifiedLogSegment, entryId, rc, transmitResult.get()});
            }

            if (transmitResult.get() != BKException.Code.OK) {
                if (recordSet.hasUserRecords()) {
                    transmitDataPacketSize.registerFailedEvent(recordSet.getNumBytes());
                }
            } else {
                // If we had data that we flushed then we need it to make sure that
                // background flush in the next pass will make the previous writes
                // visible by advancing the lastAck
                if (recordSet.hasUserRecords()) {
                    transmitDataPacketSize.registerSuccessfulEvent(recordSet.getNumBytes());
                    controlFlushNeeded = true;
                    if (immediateFlushEnabled) {
                        if (0 == minDelayBetweenImmediateFlushMs) {
                            backgroundFlush(true);
                        } else {
                            scheduleFlushWithDelayIfNeeded(new Callable<Void>() {
                                @Override
                                public Void call() throws Exception {
                                    backgroundFlush(true);
                                    return null;
                                }
                            }, immFlushSchedFutureRef);
                        }
                    }
                }
            }

            // update last dlsn before satisifying future
            if (BKException.Code.OK == transmitResult.get()) {
                DLSN lastDLSNInPacket = recordSet.finalizeTransmit(
                        logSegmentSequenceNumber, entryId);
                if (recordSet.hasUserRecords()) {
                    if (null != lastDLSNInPacket && lastDLSN.compareTo(lastDLSNInPacket) < 0) {
                        lastDLSN = lastDLSNInPacket;
                    }
                }
            }
        }

        if (BKException.Code.OK == transmitResult.get()) {
            recordSet.completeTransmit(logSegmentSequenceNumber, entryId);
        } else {
            recordSet.abortTransmit(FutureUtils.transmitException(transmitResult.get()));
        }

        if (cancelPendingPromises) {
            // Since the writer is in a bad state no more packets will be tramsitted, and its safe to
            // assign a new empty packet. This is to avoid a race with closeInternal which may also
            // try to cancel the current packet;
            final BKTransmitPacket packetCurrentSaved;
            synchronized (this) {
                packetCurrentSaved = new BKTransmitPacket(recordSetWriter);
                recordSetWriter = newRecordSetWriter();
            }
            packetCurrentSaved.getRecordSet().abortTransmit(
                    new WriteCancelledException(streamName,
                            FutureUtils.transmitException(transmitResult.get())));
        }
    }

    @Override
    synchronized public void run()  {
        backgroundFlush(false);
    }

    synchronized private void backgroundFlush(boolean controlFlushOnly)  {
        if (null != closeFuture) {
            // if the log segment is closing, skip any background flushing
            LOG.debug("Skip background flushing since log segment {} is closing.", getFullyQualifiedLogSegment());
            return;
        }
        try {
            boolean newData = haveDataToTransmit();

            if (controlFlushNeeded || (!controlFlushOnly && newData)) {
                // If we need this periodic transmit to persist previously written data but
                // there is no new data (which would cause the transmit to be skipped) generate
                // a control record
                if (!newData) {
                    writeControlLogRecord();
                }

                transmit();
                pFlushSuccesses.inc();
            } else {
                pFlushMisses.inc();
            }
        } catch (IOException exc) {
            LOG.error("Log Segment {}: Error encountered by the periodic flush", fullyQualifiedLogSegment, exc);
        }
    }

}
