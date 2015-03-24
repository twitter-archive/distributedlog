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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import com.twitter.distributedlog.stats.AlertStatsLogger;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.CachingStatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.stats.OpStatsListener;
import com.twitter.distributedlog.util.CompressionCodec;
import com.twitter.distributedlog.util.CompressionUtils;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.SafeQueueingFuturePool;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;
import com.twitter.util.TimeoutException;

import scala.runtime.BoxedUnit;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Output stream for BookKeeper based Distributed Log Manager.
 * Multiple complete log records are packed into a single bookkeeper
 * entry before sending it over the network. The fact that the log record entries
 * are complete in the bookkeeper entries means that each bookkeeper log entry
 * can be read as a complete edit log. This is useful for reading, as we don't
 * need to read through the entire log segment to get the last written entry.
 */
class BKPerStreamLogWriter implements LogWriter, AddCallback, Runnable {
    static final Logger LOG = LoggerFactory.getLogger(BKPerStreamLogWriter.class);

    public static class Buffer extends ByteArrayOutputStream {
        Buffer(int initialCapacity) {
            super(initialCapacity);
        }

        byte[] getData() {
            return buf;
        }
    }

    private static class BKTransmitPacket {
        public BKTransmitPacket(long ledgerSequenceNo, int initialBufferSize) {
            this.ledgerSequenceNo = ledgerSequenceNo;
            this.promiseList = new LinkedList<Promise<DLSN>>();
            this.isControl = false;
            this.transmitComplete = new Promise<Integer>();
            this.buffer = new Buffer(initialBufferSize * 6 / 5);
            this.maxTxId = Long.MIN_VALUE;
        }

        public void reset() {
            if (null != promiseList) {
                // Likely will have to move promise fulfillment to a separate thread
                // so safest to just create a new list so the old list can move with
                // with the thread, hence avoiding using clear to measure accurate GC
                // behavior
                cancelPromises(BKException.Code.InterruptedException);
            }
            promiseList = new LinkedList<Promise<DLSN>>();
            buffer.reset();
        }

        public long getLedgerSequenceNo() {
            return ledgerSequenceNo;
        }

        public void addToPromiseList(Promise<DLSN> nextPromise, long txId) {
            promiseList.add(nextPromise);
            maxTxId = Math.max(maxTxId, txId);
        }

        public Buffer getBuffer() {
            return buffer;
        }

        private void satisfyPromises(long entryId) {
            long nextSlotId = 0;
            for(Promise<DLSN> promise : promiseList) {
                promise.setValue(new DLSN(ledgerSequenceNo, entryId, nextSlotId));
                nextSlotId++;
            }
            promiseList.clear();
        }

        private void cancelPromises(int transmitResult) {
            cancelPromises(transmitResultToException(transmitResult));
        }

        private void cancelPromises(Throwable t) {
            for(Promise<DLSN> promise : promiseList) {
                promise.setException(t);
            }
            promiseList.clear();
        }

        public void processTransmitComplete(long entryId, int transmitResult) {
            if (transmitResult != BKException.Code.OK) {
                cancelPromises(transmitResult);
            } else {
                satisfyPromises(entryId);
            }
        }

        public DLSN finalize(long entryId, int transmitResult) {
            if (transmitResult == BKException.Code.OK) {
                lastDLSN = new DLSN(ledgerSequenceNo, entryId, promiseList.size() - 1);
            }
            return lastDLSN;
        }

        public void setControl(boolean control) {
            isControl = control;
        }

        public boolean isControl() {
            return isControl;
        }

        public void awaitTransmitComplete(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
            Await.ready(transmitComplete, Duration.fromTimeUnit(timeout, unit));
        }

        public Future<Integer> awaitTransmitComplete() {
            return transmitComplete;
        }

        public void setTransmitComplete(int transmitResult) {
            transmitComplete.setValue(transmitResult);
        }

        public void notifyTransmit() {
            transmitTime = System.nanoTime();
        }

        public long getTransmitTime() {
            return transmitTime;
        }

        public long getWriteCount() {
            return promiseList.size();
        }

        public long getMaxTxId() {
            return maxTxId;
        }

        private boolean isControl;
        private long ledgerSequenceNo;
        private DLSN lastDLSN;
        private List<Promise<DLSN>> promiseList;
        private Promise<Integer> transmitComplete;
        private Buffer buffer;
        private long transmitTime;
        private long maxTxId;
    }

    private final String fullyQualifiedLogSegment;
    private final String streamName;
    private final int logSegmentMetadataVersion;
    private BKTransmitPacket packetCurrent;
    private BKTransmitPacket packetPrevious;
    private final AtomicInteger outstandingTransmits;
    private final int transmissionThreshold;
    protected final LedgerHandle lh;
    private final CompressionCodec.Type compressionType;
    private final ReentrantLock transmitLock = new ReentrantLock();
    private final AtomicInteger transmitResult
        = new AtomicInteger(BKException.Code.OK);
    private final DistributedReentrantLock lock;
    private LogRecord.Writer writer;
    private DLSN lastDLSN = DLSN.InvalidDLSN;
    private final long startTxId;
    private long lastTxId = DistributedLogConstants.INVALID_TXID;
    private long lastTxIdFlushed = DistributedLogConstants.INVALID_TXID;
    private long lastTxIdAcknowledged = DistributedLogConstants.INVALID_TXID;
    private long outstandingBytes = 0;
    private AtomicInteger shouldFlushControl = new AtomicInteger(0);
    private final int flushTimeoutSeconds;
    private int preFlushCounter;
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
    private boolean enforceLock = true;
    private boolean closed = true;
    private final boolean enableRecordCounts;
    private int positionWithinLogSegment = 0;
    private final long ledgerSequenceNumber;
    // Used only for values that *could* change (e.g. buffer size etc.)
    private final DistributedLogConfiguration conf;
    private final ScheduledExecutorService executorService;

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

    // alert stats
    private final AlertStatsLogger alertStatsLogger;

    // add complete processing
    private final SafeQueueingFuturePool<Void> addCompleteFuturePool;

    // write rate limiter
    private final WriteLimiter writeLimiter;

    // failure injection
    private final FailureInjector failureInjector;

    // manage failure injection for the class
    static class FailureInjector {

        final double injectedDelayPercent;
        final int injectedDelayMs;
        final boolean enabled;

        public FailureInjector(DistributedLogConfiguration conf, String stream) {
            this.injectedDelayPercent = conf.getEIInjectedWriteDelayPercent();
            this.injectedDelayMs = conf.getEIInjectedWriteDelayMs();
            String failureStream = conf.getEIInjectedWriteDelayStreamName();
            this.enabled = ((null == failureStream) || failureStream.equals(stream)) &&
                (0 != injectedDelayMs) &&
                (0 != injectedDelayPercent);
        }

        void writeDelay() {
            try {
                if (enabled && Utils.randomPercent(injectedDelayPercent)) {
                    Thread.sleep(injectedDelayMs);
                }
            } catch (InterruptedException ex) {
                LOG.warn("delay was interrupted ", ex);
            }
        }
    }

    /**
     * Construct an edit log output stream which writes to a ledger.
     */
    protected BKPerStreamLogWriter(String streamName,
                                   String logSegmentName,
                                   DistributedLogConfiguration conf,
                                   int logSegmentMetadataVersion,
                                   LedgerHandle lh, DistributedReentrantLock lock,
                                   long startTxId, long ledgerSequenceNumber,
                                   ScheduledExecutorService executorService,
                                   FuturePool orderedFuturePool,
                                   StatsLogger statsLogger,
                                   AlertStatsLogger alertStatsLogger,
                                   PermitLimiter globalWriteLimiter)
        throws IOException {
        super();

        // set up a write limiter
        PermitLimiter streamWriteLimiter = null;
        if (conf.getPerWriterOutstandingWriteLimit() < 0) {
            streamWriteLimiter = PermitLimiter.NULL_PERMIT_LIMITER;
        } else {
            streamWriteLimiter = new SimplePermitLimiter(
                conf.getOutstandingWriteLimitDarkmode(),
                conf.getPerWriterOutstandingWriteLimit(),
                statsLogger.scope("streamWriteLimiter"),
                false);
        }
        this.writeLimiter = new WriteLimiter(streamName, streamWriteLimiter, globalWriteLimiter);

        // alert stats
        this.alertStatsLogger = alertStatsLogger;

        // stats
        if (conf.getEnablePerStreamStat()) {
            this.envelopeStatsLogger = new CachingStatsLogger(
                new BroadCastStatsLogger.Two(statsLogger, statsLogger.scope(streamName))
            );
        } else {
            this.envelopeStatsLogger = statsLogger;
        }

        StatsLogger flushStatsLogger = statsLogger.scope("flush");
        StatsLogger pFlushStatsLogger = flushStatsLogger.scope("periodic");
        pFlushSuccesses = pFlushStatsLogger.getCounter("success");
        pFlushMisses = pFlushStatsLogger.getCounter("miss");

        // transmit
        StatsLogger transmitStatsLogger = statsLogger.scope("transmit");
        StatsLogger transmitDataStatsLogger = statsLogger.scope("data");
        transmitDataSuccesses = transmitDataStatsLogger.getCounter("success");
        transmitDataMisses = transmitDataStatsLogger.getCounter("miss");
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
        StatsLogger transmitOutstandingLogger = transmitStatsLogger.scope("outstanding");
        String statPrefixForStream = streamName.replaceAll(":|<|>|/", "_");
        if (conf.getEnablePerStreamStat()) {
            transmitOutstandingLogger.registerGauge(statPrefixForStream + "_requests", new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }
                @Override
                public Number getSample() {
                    return outstandingTransmits.get();
                }
            });
        }

        outstandingTransmits = new AtomicInteger(0);
        this.fullyQualifiedLogSegment = streamName + ":" + logSegmentName;
        this.streamName = streamName;
        this.logSegmentMetadataVersion = logSegmentMetadataVersion;
        this.lh = lh;
        this.lock = lock;
        this.lock.acquire(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

        if (conf.getOutputBufferSize() > DistributedLogConstants.MAX_TRANSMISSION_SIZE) {
            LOG.warn("Setting output buffer size {} greater than max transmission size {} for log segment {}",
                new Object[] {conf.getOutputBufferSize(), DistributedLogConstants.MAX_TRANSMISSION_SIZE, fullyQualifiedLogSegment});
            this.transmissionThreshold = DistributedLogConstants.MAX_TRANSMISSION_SIZE;
        } else {
            this.transmissionThreshold = conf.getOutputBufferSize();
        }

        this.ledgerSequenceNumber = ledgerSequenceNumber;
        this.packetCurrent = new BKTransmitPacket(ledgerSequenceNumber, Math.max(transmissionThreshold, 1024));
        this.packetPrevious = null;
        this.writer = new LogRecord.Writer(new DataOutputStream(packetCurrent.getBuffer()));
        this.startTxId = startTxId;
        this.lastTxId = startTxId;
        this.lastTxIdFlushed = startTxId;
        this.lastTxIdAcknowledged = startTxId;
        this.enableRecordCounts = conf.getEnableRecordCounts();
        this.flushTimeoutSeconds = conf.getLogFlushTimeoutSeconds();
        this.immediateFlushEnabled = conf.getImmediateFlushEnabled();

        // Failure injection
        this.failureInjector = new FailureInjector(conf, streamName);

        // If we are transmitting immediately (threshold == 0) and if immediate
        // flush is enabled, we don't need the periodic flush task
        if (!immediateFlushEnabled || (0 != this.transmissionThreshold)) {
            int periodicFlushFrequency = conf.getPeriodicFlushFrequencyMilliSeconds();
            if (periodicFlushFrequency > 0 && executorService != null) {
                periodicFlushSchedule = executorService.scheduleAtFixedRate(this,
                        periodicFlushFrequency/2, periodicFlushFrequency/2, TimeUnit.MILLISECONDS);
            }
        } else {
            // Min delay heuristic applies only when immediate flush is enabled
            // and transmission threshold is zero
            minDelayBetweenImmediateFlushMs = conf.getMinDelayBetweenImmediateFlushMs();
        }

        this.conf = conf;
        this.executorService = executorService;
        if (null != orderedFuturePool) {
            this.addCompleteFuturePool = new SafeQueueingFuturePool(orderedFuturePool);
        } else {
            this.addCompleteFuturePool = null;
        }
        assert(!this.immediateFlushEnabled || (null != this.executorService));
        this.closed = false;
        this.lastTransmit = Stopwatch.createStarted();
        this.compressionType = CompressionUtils.stringToType(conf.getCompressionType());
    }

    String getFullyQualifiedLogSegment() {
        return fullyQualifiedLogSegment;
    }

    @VisibleForTesting
    DistributedReentrantLock getLock() {
        return this.lock;
    }

    protected final LedgerHandle getLedgerHandle() {
        return this.lh;
    }

    protected final long getLedgerSequenceNumber() {
        return ledgerSequenceNumber;
    }

    protected final long getStartTxId() {
        return startTxId;
    }

    private BKTransmitPacket getTransmitPacket() {
        return new BKTransmitPacket(ledgerSequenceNumber,
            Math.max(transmissionThreshold, getAverageTransmitSize()));
    }

    private boolean envelopeBeforeTransmit() {
        return LogSegmentLedgerMetadata.supportsEnvelopedEntries(logSegmentMetadataVersion);
    }

    public void closeToFinalize() throws IOException {
        // Its important to enforce the write-lock here as we are going to make
        // metadata changes following this call
        IOException throwExc = closeInternal(true, true);

        if (null != throwExc) {
            throw throwExc;
        }
    }

    @Override
    public void close() throws IOException {
        IOException throwExc = closeInternal(true, false);

        if (null != throwExc) {
            throw throwExc;
        }
    }

    private void flushAddCompletes() {
        if (null != addCompleteFuturePool) {
            addCompleteFuturePool.close();
        }
    }

    private synchronized void resetPacket(BKTransmitPacket packet) {
        long numPromises = 0;
        if (null != packet) {
            numPromises = packet.getWriteCount();
            packet.reset();
        }
        LOG.info("Stream {} aborted {} writes", fullyQualifiedLogSegment, numPromises);
    }

    private synchronized long getWritesPendingTransmit() {
        if (null != packetCurrent) {
            return packetCurrent.getWriteCount();
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

    public IOException closeInternal(boolean attemptFlush, boolean enforceLock) {
        synchronized (this) {
            if (closed) {
                return null;
            }
            closed = true;
        }

        IOException throwExc = null;

        // Cancel the periodic flush schedule first
        // The task is allowed to exit gracefully
        // The attempt to flush will synchronize with the
        // last execution of the task
        if (null != periodicFlushSchedule) {
            periodicFlushSchedule.cancel(false);
        }

        if (attemptFlush && !isStreamInError()) {
            try {
                // BookKeeper fencing will disallow multiple writers, so if we have
                // already lost the lock, this operation will fail, so we let the caller
                // decide if we should enforce the lock
                this.enforceLock = enforceLock;
                setReadyToFlush();
                flushAndSync();
            } catch (IOException exc) {
                throwExc = exc;
            }
        }

        LOG.info("Closing BKPerStreamLogWriter for {} lastDLSN = {} outstandingTransmits = {} " +
            "writesPendingTransmit = {} addCompletesPending = {}", new Object[] { fullyQualifiedLogSegment,
            lastDLSN, outstandingTransmits.get(), getWritesPendingTransmit(), getPendingAddCompleteCount() });

        // Save the current packet to reset, leave a new empty packet to avoid a race with
        // addCompleteDeferredProcessing.
        final BKTransmitPacket packetPreviousSaved;
        final BKTransmitPacket packetCurrentSaved;
        synchronized (this) {
            packetPreviousSaved = packetPrevious;
            packetCurrentSaved = packetCurrent;
            packetCurrent = getTransmitPacket();
        }

        // Once the last packet been transmitted, apply any remaining promises asynchronously
        // to avoid blocking close if bk client is slow for some reason.
        if (null != packetPreviousSaved) {
            packetPreviousSaved.awaitTransmitComplete().addEventListener(new FutureEventListener<Integer>() {
                @Override
                public void onSuccess(Integer transmitResult) {
                    flushAddCompletes();
                    resetPacket(packetCurrentSaved);
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.error("Unexpected error on transmit completion ", cause);
                }
            });
        } else {
            // In this case there are no pending add completes, but we still need to flush the
            // current packet.
            resetPacket(packetCurrentSaved);
        }

        if (null == throwExc && !isStreamInError()) {
            // Synchronous closing the ledger handle, if we couldn't close a ledger handle successfully.
            // we should throw the exception to #closeToFinalize, so it would fail completing a log segment.
            try {
                lh.close();
            } catch (BKException.BKLedgerClosedException lce) {
                // if a ledger is already closed, we don't need to throw exception
            } catch (BKException bke) {
                throwExc = new IOException("Failed to close ledger for " + fullyQualifiedLogSegment, bke);
            } catch (InterruptedException ie) {
                throwExc = new DLInterruptedException("Interrupted on closing ledger for " + fullyQualifiedLogSegment, ie);
            }
        }

        lock.release(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

        // If add entry failed because of closing ledger above, we don't need to fail the close operation
        if (null == throwExc && shouldFailCompleteLogSegment()) {
            throwExc = new BKTransmitException("Closing an errored stream : ", transmitResult.get());
        }

        return throwExc;
    }

    @Override
    public void abort() {
        closeInternal(false, false);
    }

    @Override
    synchronized public void write(LogRecord record) throws IOException {
        writeUserRecord(record);
        flushIfNeeded();
    }

    synchronized public Future<DLSN> asyncWrite(LogRecord record) {
        return asyncWrite(record, true);
    }

    synchronized public Future<DLSN> asyncWrite(LogRecord record, boolean flush) {
        Future<DLSN> result = null;
        try {
            if (record.isControl()) {
                result = writeControlLogRecord(record);
                transmit(true);
            } else {
                result = writeUserRecord(record);
                if (flush) {
                    flushIfNeeded();
                }
            }
        } catch (IOException ioe) {
            // We may incorrectly report transmit failure here, but only if we happened to hit
            // packet/xmit size limit conditions AND fail flush above, which should happen rarely
            // (PUBSUB-4498)
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
        if (closed) {
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
        failureInjector.writeDelay();

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
            positionWithinLogSegment++;
            future = writeInternal(record);
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

    public boolean isStreamInError() {
        return (transmitResult.get() != BKException.Code.OK);
    }

    public boolean shouldFailCompleteLogSegment() {
        return (transmitResult.get() != BKException.Code.OK) &&
                (transmitResult.get() != BKException.Code.LedgerClosedException);
    }

    synchronized public Future<DLSN> writeInternal(LogRecord record) throws IOException {
        int logRecordSize = record.getPersistentSize();

        if (logRecordSize > DistributedLogConstants.MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(String.format(
                    "Log Record of size %d written when only %d is allowed",
                    logRecordSize, DistributedLogConstants.MAX_LOGRECORD_SIZE));
        }

        // If we will exceed the max number of bytes allowed per entry
        // initiate a transmit before accepting the new log record
        if ((writer.getPendingBytes() + logRecordSize) >
            DistributedLogConstants.MAX_TRANSMISSION_SIZE) {
            setReadyToFlush();
        }

        if (FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_WriteInternalLostLock)) {
            throw new LockingException("/failpoint/lockpath", "failpoint is simulating a lost lock");
        }

        Promise<DLSN> dlsn = new Promise<DLSN>();
        dlsn.addEventListener(new OpStatsListener(writeTime));

        if (enableRecordCounts) {
            // Set the count here. The caller would appropriately increment it
            // if this log record is to be counted
            record.setPositionWithinLogSegment(positionWithinLogSegment);
        }

        writer.writeOp(record);
        packetCurrent.addToPromiseList(dlsn, record.getTransactionId());

        if (record.getTransactionId() < lastTxId) {
            LOG.info("Log Segment {} TxId decreased Last: {} Record: {}", new Object[] {fullyQualifiedLogSegment, lastTxId, record.getTransactionId()});
        }
        lastTxId = record.getTransactionId();
        if (!record.isControl()) {
            outstandingBytes += (20 + record.getPayload().length);
        }
        return dlsn;
    }

    synchronized private void writeControlLogRecord() throws IOException {
        LogRecord controlRec = new LogRecord(lastTxId, "control".getBytes(UTF_8));
        controlRec.setControl();
        writeControlLogRecord(controlRec);
    }

    synchronized private Future<DLSN> writeControlLogRecord(LogRecord record) throws IOException {
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
    @Override
    public void markEndOfStream() throws IOException {
        synchronized (this) {
            writeEndOfStreamMarker();
            streamEnded = true;
            setReadyToFlush();
        }
        flushAndSync();
    }

    @Override
    synchronized public int writeBulk(List<LogRecord> records) throws IOException {
        int numRecords = 0;
        for (LogRecord r : records) {
            write(r);
            numRecords++;
        }
        return numRecords;
    }

    @Override
    synchronized public long setReadyToFlush() throws IOException {

        FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_TransmitBeforeAddEntry);

        if (transmit(false)) {
            shouldFlushControl.incrementAndGet();
        }

        return lastTxIdAcknowledged;
    }

    @Override
    public long flushAndSync() throws IOException {
        flushAndSyncPhaseOne();
        return flushAndSyncPhaseTwo();
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
            scheduledFutureRef.set(executorService.schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized(this) {
                        scheduledFutureRef.set(null);
                        try {
                            callable.call();
                        } catch (Exception exc) {
                            LOG.error("Delayed flush failed", exc);
                        }
                    }
                }
            }, delayMs, TimeUnit.MILLISECONDS));
        }
    }

    // Based on transmit buffer size, immediate flush, etc., should we flush the current
    // packet now.
    void flushIfNeeded() throws IOException {
        if (outstandingBytes > transmissionThreshold) {
            // If flush delay is disabled, flush immediately, else schedule appropriately.
            if (0 == minDelayBetweenImmediateFlushMs) {
                setReadyToFlush();
            } else {
                scheduleFlushWithDelayIfNeeded(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        setReadyToFlush();
                        return null;
                    }
                }, transmitSchedFutureRef);
            }
        }
    }

    public long flushAndSyncPhaseOne() throws
        LockingException, BKTransmitException, FlushException {
        flushAndSyncInternal();

        synchronized (this) {
            preFlushCounter = shouldFlushControl.get();
            shouldFlushControl.set(0);

            if (preFlushCounter > 0) {
                try {
                    writeControlLogRecord();
                    transmit(true);
                } catch (Exception exc) {
                    shouldFlushControl.addAndGet(preFlushCounter);
                    preFlushCounter = 0;
                    throw new FlushException("Flush encountered an error while writing data to the backend", getLastTxId(), lastTxIdAcknowledged, exc);
                }
            }

            return lastTxIdAcknowledged;
        }
    }

    public long flushAndSyncPhaseTwo() throws FlushException {
        if (preFlushCounter > 0) {
            try {
                flushAndSyncInternal();
            } catch (Exception exc) {
                shouldFlushControl.addAndGet(preFlushCounter);
                throw new FlushException("Flush encountered an error while writing data to backend", getLastTxId(), getLastTxIdAcknowledged(), exc);
            } finally {
                preFlushCounter = 0;
            }
        }
        synchronized (this) {
            return lastTxIdAcknowledged;
        }
    }

    private void checkWriteLock() throws LockingException {
        if (enforceLock) {
            lock.checkOwnershipAndReacquire(false);
        }
    }

    private void flushAndSyncInternal()
        throws LockingException, BKTransmitException, FlushException {
        checkWriteLock();

        long txIdToBePersisted;

        synchronized (this) {
            txIdToBePersisted = lastTxIdFlushed;
        }

        final BKTransmitPacket lastPacket;
        synchronized (this) {
            lastPacket = packetPrevious;
        }

        try {
            if (null != lastPacket) {
                lastPacket.awaitTransmitComplete(flushTimeoutSeconds, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ie) {
            throw new FlushException("Wait for Flush Interrupted", getLastTxId(), getLastTxIdAcknowledged(), ie);
        } catch (TimeoutException te) {
            throw new FlushException("Flush request timed out", getLastTxId(), getLastTxIdAcknowledged());
        }

        if (transmitResult.get() != BKException.Code.OK) {
            LOG.error("Log Segment {} Failed to write to bookkeeper; Error is {}",
                fullyQualifiedLogSegment, BKException.getMessage(transmitResult.get()));
            throw transmitResultToException(transmitResult.get());
        }

        synchronized (this) {
            lastTxIdAcknowledged = Math.max(lastTxIdAcknowledged, txIdToBePersisted);
        }
    }

    /**
     * Transmit the current buffer to bookkeeper.
     * Synchronised at the class. #write() and #setReadyToFlush()
     * are never called at the same time.
     *
     * NOTE: This method should only throw known exceptions so that we don't accidentally
     *       add new code that throws in an inappropriate place.
     */
    private boolean transmit(boolean isControl)
        throws BKTransmitException, LockingException, WriteException, InvalidEnvelopedEntryException {
        BKTransmitPacket packet;
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

                if (packetCurrent.getBuffer().size() == 0) {
                    // Control flushes always have at least the control record to flush
                    transmitDataMisses.inc();
                    return false;
                }

                packet = packetCurrent;
                packet.setControl(isControl);
                outstandingBytes = 0;
                packetCurrent = getTransmitPacket();
                packetPrevious = packet;
                writer = new LogRecord.Writer(new DataOutputStream(packetCurrent.getBuffer()));
                lastTxIdFlushed = lastTxId;

                if (!isControl) {
                    numBytes += packet.getBuffer().size();
                    numFlushesSinceRestart++;
                }
            }

            Buffer toSend = packet.getBuffer();
            if (envelopeBeforeTransmit()) {
                try {
                    // We can't escape this allocation because things need to be read from one byte array
                    // and then written to another. This is the destination.
                    toSend = new Buffer(packet.getBuffer().size());
                    byte[] decompressed = packet.getBuffer().getData();
                    int length = packet.getBuffer().size();
                    EnvelopedEntry entry = new EnvelopedEntry(EnvelopedEntry.CURRENT_VERSION,
                                                              compressionType,
                                                              decompressed,
                                                              length,
                                                              envelopeStatsLogger);
                    // This will cause an allocation of a byte[] for compression. This can be avoided
                    // but we can do that later only if needed.
                    entry.writeFully(new DataOutputStream(toSend));
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
            }

            synchronized (this) {
                packet.notifyTransmit();
                lh.asyncAddEntry(toSend.getData(), 0, toSend.size(),
                                 this, packet);

                if (isControl) {
                    transmitControlSuccesses.inc();
                } else {
                    transmitDataSuccesses.inc();
                }

                lastTransmit.reset().start();
                outstandingTransmits.incrementAndGet();
                controlFlushNeeded = false;
            }
            return true;
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

        return (packetCurrent.getBuffer().size() > 0);
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
                        new Object[] { fullyQualifiedLogSegment, entryId, rc });
                }
            }).addEventListener(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void done) {
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.error("addComplete processing failed for {} entry {} lastTxId {} rc {} with error",
                        new Object[] {fullyQualifiedLogSegment, entryId, transmitPacket.getMaxTxId(), rc, cause});
                }
            });
            // Race condition if we notify before the addComplete is enqueued.
            transmitPacket.setTransmitComplete(effectiveRC.get());
            outstandingTransmits.getAndDecrement();
        } else {
            // Notify transmit complete must be called before deferred processing in the
            // sync case since otherwise callbacks in deferred processing may deadlock.
            transmitPacket.setTransmitComplete(effectiveRC.get());
            outstandingTransmits.getAndDecrement();
            addCompleteDeferredProcessing(transmitPacket, entryId, effectiveRC.get());
        }
    }

    private void addCompleteDeferredProcessing(final BKTransmitPacket transmitPacket,
                                               final long entryId,
                                               final int rc) {
        boolean cancelPendingPromises = false;
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
                if (!transmitPacket.isControl()) {
                    transmitDataPacketSize.registerFailedEvent(transmitPacket.buffer.size());
                }
            } else {
                // If we had data that we flushed then we need it to make sure that
                // background flush in the next pass will make the previous writes
                // visible by advancing the lastAck
                if (!transmitPacket.isControl()) {
                    transmitDataPacketSize.registerSuccessfulEvent(transmitPacket.buffer.size());
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

            DLSN lastDLSNInPacket = transmitPacket.finalize(entryId, transmitResult.get());

            // update last dlsn before satisifying future
            if (BKException.Code.OK == transmitResult.get() && !transmitPacket.isControl()) {
                if (null != lastDLSNInPacket && lastDLSN.compareTo(lastDLSNInPacket) < 0) {
                    lastDLSN = lastDLSNInPacket;
                }
            }
        }
        transmitPacket.processTransmitComplete(entryId, transmitResult.get());

        if (cancelPendingPromises) {
            // Since the writer is in a bad state no more packets will be tramsitted, and its safe to
            // assign a new empty packet. This is to avoid a race with closeInternal which may also
            // try to cancel the current packet;
            final BKTransmitPacket packetCurrentSaved;
            synchronized (this) {
                packetCurrentSaved = packetCurrent;
                packetCurrent = getTransmitPacket();
            }
            packetCurrentSaved.cancelPromises(new WriteCancelledException(fullyQualifiedLogSegment,
                transmitResultToException(transmitResult.get())));
        }
    }

    public synchronized int getAverageTransmitSize() {
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

    public long getLastAddConfirmed() {
        return lh.getLastAddConfirmed();
    }

    @Override
    synchronized public void run()  {
        backgroundFlush(false);
    }

    synchronized private void backgroundFlush(boolean controlFlushOnly)  {
        try {
            boolean newData = haveDataToTransmit();

            if (controlFlushNeeded || (!controlFlushOnly && newData)) {
                // If we need this periodic transmit to persist previously written data but
                // there is no new data (which would cause the transmit to be skipped) generate
                // a control record
                if (!newData) {
                    writeControlLogRecord();
                }

                transmit(!newData);
                pFlushSuccesses.inc();
            } else {
                pFlushMisses.inc();
            }
        } catch (IOException exc) {
            LOG.error("Log Segment {}: Error encountered by the periodic flush", fullyQualifiedLogSegment, exc);
        }
    }



    public boolean shouldStartNewSegment(int numRecords) {
        return (numRecords > (Integer.MAX_VALUE - positionWithinLogSegment));
    }

    public synchronized long getLastTxId() {
        return lastTxId;
    }

    synchronized long getLastTxIdAcknowledged() {
        return lastTxIdAcknowledged;
    }

    public int getPositionWithinLogSegment() {
        return positionWithinLogSegment;
    }

    public DLSN getLastDLSN() {
        return lastDLSN;
    }

    public static BKTransmitException transmitResultToException(int transmitResult) {
        return new BKTransmitException("Failed to write to bookkeeper; Error is ("
            + transmitResult + ") "
            + BKException.getMessage(transmitResult), transmitResult);
    }
}
