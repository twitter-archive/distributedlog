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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.exceptions.OverCapacityException;

import com.twitter.util.Function0;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    private static class BKTransmitPacket {
        public BKTransmitPacket(long ledgerSequenceNo, int initialBufferSize) {
            this.ledgerSequenceNo = ledgerSequenceNo;
            this.promiseList = new LinkedList<Promise<DLSN>>();
            this.isControl = false;
            this.buffer = new DataOutputBuffer(initialBufferSize * 6 / 5);
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

        public void addToPromiseList(Promise<DLSN> nextPromise) {
            promiseList.add(nextPromise);
        }

        public DataOutputBuffer getBuffer() {
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
            for(Promise<DLSN> promise : promiseList) {
                promise.setException(transmitResultToException(transmitResult));
            }
            promiseList.clear();
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

        public DLSN markTransmitComplete(long entryId, int transmitResult) {
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

        boolean          isControl;
        private long ledgerSequenceNo;
        private DLSN lastDLSN;
        private List<Promise<DLSN>> promiseList;
        DataOutputBuffer buffer;
    }

    private final String fullyQualifiedLogSegment;
    private BKTransmitPacket packetCurrent;
    private final AtomicInteger outstandingWrites;
    private final AtomicInteger outstandingTransmits;
    private final int perWriterOutstandingWriteLimit;
    private final boolean perWriterOutstandingWriteLimitDarkmode;
    private final int transmissionThreshold;
    protected final LedgerHandle lh;
    private final Object syncLock = new Object();
    private CountDownLatch syncLatch;
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

    // Indicates whether there are writes that have been successfully transmitted that would need
    // a control record to be transmitted to make them visible to the readers by updating the last
    // add confirmed
    private boolean controlFlushNeeded = false;
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
    private final DistributedLogConfiguration conf;
    private final ScheduledExecutorService executorService;

    // stats
    private final Counter transmitDataSuccesses;
    private final Counter transmitDataMisses;
    private final OpStatsLogger transmitDataPacketSize;
    private final Counter transmitControlSuccesses;
    private final Counter pFlushSuccesses;
    private final Counter pFlushMisses;
    private final FuturePool orderedFuturePool;
    private final StatsLogger writesStatsLogger;
    private final OpStatsLogger outstandingWritesMetric;
    private final Counter outstandingWritesLimitExceeded;

    /**
     * Construct an edit log output stream which writes to a ledger.
     */
    protected BKPerStreamLogWriter(String streamName,
                                   String logSegmentName,
                                   DistributedLogConfiguration conf,
                                   LedgerHandle lh, DistributedReentrantLock lock,
                                   long startTxId, long ledgerSequenceNumber,
                                   ScheduledExecutorService executorService,
                                   FuturePool orderedFuturePool,
                                   StatsLogger statsLogger)
        throws IOException {
        super();

        // stats
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

        // outstanding user record writes
        writesStatsLogger = statsLogger.scope("writes");
        outstandingWritesMetric = writesStatsLogger.getOpStatsLogger("outstanding");
        outstandingWritesLimitExceeded = writesStatsLogger.getCounter("outstandingLimitExceeded");
        outstandingWrites = new AtomicInteger(0);
        writesStatsLogger.registerGauge("outstanding", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }
            @Override
            public Number getSample() {
                return outstandingWrites.get();
            }
        });

        outstandingTransmits = new AtomicInteger(0);
        syncLatch = null;
        this.fullyQualifiedLogSegment = streamName + ":" + logSegmentName;
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
        this.writer = new LogRecord.Writer(packetCurrent.getBuffer());
        this.startTxId = startTxId;
        this.lastTxId = startTxId;
        this.lastTxIdFlushed = startTxId;
        this.lastTxIdAcknowledged = startTxId;
        this.enableRecordCounts = conf.getEnableRecordCounts();
        this.flushTimeoutSeconds = conf.getLogFlushTimeoutSeconds();
        this.immediateFlushEnabled = conf.getImmediateFlushEnabled();
        this.perWriterOutstandingWriteLimit = conf.getPerWriterOutstandingWriteLimit();
        this.perWriterOutstandingWriteLimitDarkmode = conf.getPerWriterOutstandingWriteLimitDarkmode();

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
        this.orderedFuturePool = orderedFuturePool;
        assert(!this.immediateFlushEnabled || (null != this.executorService));
        this.closed = false;
        this.lastTransmit = Stopwatch.createStarted();
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

        // Packet reset will cancel any outstanding promises
        synchronized (this) {
            if (null != packetCurrent) {
                packetCurrent.reset();
            }
        }

        if (null == throwExc && !isStreamInError()) {
            // Synchronous closing the ledger handle, if we couldn't close a ledger handle successfully.
            // we should throw the exception to #closeToFinalize, so it would fail completing a log segment.
            try {
                lh.close();
            } catch (BKException bke) {
                throwExc = new IOException("Failed to close ledger for " + fullyQualifiedLogSegment, bke);
            } catch (InterruptedException ie) {
                throwExc = new DLInterruptedException("Interrupted on closing ledger for " + fullyQualifiedLogSegment, ie);
            }
        }

        lock.release(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

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

    // Record failure so we can quickly abort a bulk write.
    static class FailfastWriteListener implements FutureEventListener<DLSN> {

        private final AtomicReference<Throwable> failure;

        FailfastWriteListener(AtomicReference<Throwable> failure) {
            this.failure = failure;
        }

        @Override
        public void onSuccess(DLSN dlsn) {
            /* do nothing */
        }

        @Override
        public void onFailure(Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    synchronized public Future<DLSN> asyncWrite(LogRecord record) {
        try {
            Future<DLSN> result = null;
            if (record.isControl()) {
                // write control record.
                result = writeControlLogRecord(record);
                transmit(true);
                return result;
            }
            result = writeUserRecord(record);
            flushIfNeeded();
            return result;
        } catch (IOException ioe) {
            LOG.error("Encountered exception while writing a log record to stream {}", fullyQualifiedLogSegment, ioe);
            return Future.exception(ioe);
        }
    }

    synchronized public List<Future<DLSN>> asyncWriteBulk(List<LogRecord> records) {
        AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        ArrayList<Future<DLSN>> results = new ArrayList<Future<DLSN>>(records.size());
        FailfastWriteListener failfastListener = new FailfastWriteListener(failure);

        for (LogRecord record : records) {
            Future<DLSN> result = null;

            // Need to be careful to terminate the bulk operation in case a client error occurs.
            // Ex. some write in the middle of the list may end up being too large, and we must not
            // attempt to apply any more writes after said write fails.
            try {
                result = writeUserRecord(record).addEventListener(failfastListener);
            } catch (IOException ioe) {
                LOG.error("Encountered exception during bulk write while writing a log record to stream {}",
                    fullyQualifiedLogSegment, ioe);
                result = Future.exception(ioe);
                failure.compareAndSet(null, ioe);
            }

            // If we hit any failure we must terminate the bulk write.
            results.add(result);
            if (null != failure.get()) {
                break;
            }
        }

        // Try to flush pending writes. If flush fails here it cancels all pending writes on the
        // ordered future pool thread. Whether an error occurred or not earlier its safe to flush here
        // again since either we end up flushing all writes prior to the failure, or we fail all pending
        // writes. In both cases we haven't violated the requirement that all writes should fail after
        // the first failure.
        flushIfNeededNoThrow();

        // If results are not fully specified, we hit a failure. Cancel the remaining futures.
        if (results.size() < records.size()) {
            assert(null != failure.get());
            appendCancelledFutures(results, records.size() - results.size(), failure.get());
        }

        return (List) results;
    }

    private void appendCancelledFutures(List<Future<DLSN>> futures, int count, Throwable t) {
        final WriteCancelledException cre =
            new WriteCancelledException(fullyQualifiedLogSegment, t);
        for (int i = 0; i < count; i++) {
            Future<DLSN> cancelledFuture = Future.exception(cre);
            futures.add(cancelledFuture);
        }
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

        // This sync call is the only call that increments the outstanding 
        // requests counter, so this may be lower after this call, but it won't 
        // be higher. Thus we should still never exceed the max outstanding
        // requests limit.
        int outstanding = outstandingWrites.get() + 1;
        outstandingWritesMetric.registerSuccessfulEvent(outstandingWrites.get());
        if (perWriterOutstandingWriteLimit > -1 && outstanding > perWriterOutstandingWriteLimit) {
            outstandingWritesLimitExceeded.inc();
            if (!perWriterOutstandingWriteLimitDarkmode) {
                throw new OverCapacityException(outstanding, perWriterOutstandingWriteLimit);
            }
        }
        
        // The count represents the number of user records up to the
        // current record
        // Increment the record count only when writing a user log record
        // Internally generated log records don't increment the count
        // writeInternal will always set a count regardless of whether it was
        // incremented or not.
        Future<DLSN> future = null;
        try {
            outstandingWrites.incrementAndGet();
            positionWithinLogSegment++;
            future = writeInternal(record);
        } catch (IOException ex) {
            outstandingWrites.decrementAndGet();
            positionWithinLogSegment--;
            throw ex;
        }

        // Track outstanding requests and return the future.
        return future.addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN dlsn) {
                outstandingWrites.decrementAndGet();
            }
            @Override
            public void onFailure(Throwable cause) {
                outstandingWrites.decrementAndGet();
            }
        });
    }

    public boolean isStreamInError() {
        return (transmitResult.get() != BKException.Code.OK);
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

        Promise<DLSN> dlsn = new Promise<DLSN>();

        if (enableRecordCounts) {
            // Set the count here. The caller would appropriately increment it
            // if this log record is to be counted
            record.setPositionWithinLogSegment(positionWithinLogSegment);
        }

        writer.writeOp(record);
        packetCurrent.addToPromiseList(dlsn);

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
                                        final AtomicReference<ScheduledFuture<?>> scheduledFutureRef,
                                        long delayMs) {
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
                            LOG.error("Delayed flush failed {}", exc);
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
            long timeSinceLastTransmit = lastTransmit.elapsed(TimeUnit.MILLISECONDS);
            if (minDelayBetweenImmediateFlushMs <= timeSinceLastTransmit) {
                setReadyToFlush();
            } else {
                scheduleFlushWithDelayIfNeeded(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        setReadyToFlush();
                        return null;
                    }
                }, transmitSchedFutureRef, minDelayBetweenImmediateFlushMs - timeSinceLastTransmit);
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

        CountDownLatch latch;
        synchronized (syncLock) {
            latch = syncLatch = new CountDownLatch(outstandingTransmits.get());
        }

        boolean waitSuccessful;
        try {
            waitSuccessful = latch.await(flushTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            throw new FlushException("Wait for Flush Interrupted", getLastTxId(), getLastTxIdAcknowledged(), ie);
        }

        if (!waitSuccessful) {
            throw new FlushException("Flush request timed out", getLastTxId(), getLastTxIdAcknowledged());
        }

        synchronized (syncLock) {
            syncLatch = null;
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
     */
    synchronized private boolean transmit(boolean isControl)
        throws BKTransmitException, LockingException {
        checkWriteLock();

        if (!transmitResult.compareAndSet(BKException.Code.OK,
            BKException.Code.OK)) {
            LOG.error("Log Segment {} Trying to write to an errored stream; Error is {}",
                fullyQualifiedLogSegment,
                BKException.getMessage(transmitResult.get()));
            throw new BKTransmitException("Trying to write to an errored stream;"
                + " Error code : (" + transmitResult.get()
                + ") " + BKException.getMessage(transmitResult.get()), transmitResult.get());
        }

        if (packetCurrent.getBuffer().getLength() > 0) {
            BKTransmitPacket packet = packetCurrent;
            packet.setControl(isControl);
            outstandingBytes = 0;
            packetCurrent = getTransmitPacket();
            writer = new LogRecord.Writer(packetCurrent.getBuffer());
            lastTxIdFlushed = lastTxId;

            if (!isControl) {
                numBytes += packet.getBuffer().getLength();
                numFlushesSinceRestart++;
            }

            lh.asyncAddEntry(packet.getBuffer().getData(), 0, packet.getBuffer().getLength(),
                this, packet);

            if (isControl) {
                transmitDataSuccesses.inc();
            } else {
                transmitControlSuccesses.inc();
            }

            lastTransmit.reset().start();
            outstandingTransmits.incrementAndGet();
            controlFlushNeeded = false;
            return true;
        } else {
            // Control flushes always have at least the control record to flush
            transmitDataMisses.inc();
        }
        return false;
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

        return (packetCurrent.getBuffer().getLength() > 0);
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

        assert (ctx instanceof BKTransmitPacket);
        final BKTransmitPacket transmitPacket = (BKTransmitPacket) ctx;

        // notify the waiters before callback, otherwise, it might block flush
        CountDownLatch l;
        synchronized (syncLock) {
            outstandingTransmits.decrementAndGet();
            l = syncLatch;
        }

        if (null != orderedFuturePool) {
            orderedFuturePool.apply(new Function0<Void>() {
                public Void apply() {
                    addCompleteDeferredProcessing(transmitPacket, entryId, effectiveRC.get());
                    return null;
                }
            });
        } else {
            addCompleteDeferredProcessing(transmitPacket, entryId, effectiveRC.get());
        }

        if (l != null) {
            l.countDown();
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
                LOG.warn("Log segment {}: Tried to set transmit result to ({}) but is already ({})",
                    new Object[] {fullyQualifiedLogSegment, rc, transmitResult.get()});
            }

            if (transmitResult.get() != BKException.Code.OK) {
                if (!transmitPacket.isControl()) {
                    transmitDataPacketSize.registerFailedEvent(transmitPacket.buffer.getLength());
                }
            } else {
                // If we had data that we flushed then we need it to make sure that
                // background flush in the next pass will make the previous writes
                // visible by advancing the lastAck
                if (!transmitPacket.isControl()) {
                    transmitDataPacketSize.registerSuccessfulEvent(transmitPacket.buffer.getLength());
                    controlFlushNeeded = true;
                    if (immediateFlushEnabled) {
                        scheduleFlushWithDelayIfNeeded(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                backgroundFlush(true);
                                return null;
                            }
                        }, immFlushSchedFutureRef, Math.max(0, minDelayBetweenImmediateFlushMs - lastTransmit.elapsed(TimeUnit.MILLISECONDS)));
                    }
                }
            }

            DLSN lastDLSNInPacket = transmitPacket.markTransmitComplete(entryId, transmitResult.get());

            // update last dlsn before satisifying future
            if (BKException.Code.OK == transmitResult.get() && !transmitPacket.isControl()) {
                if (null != lastDLSNInPacket && lastDLSN.compareTo(lastDLSNInPacket) < 0) {
                    lastDLSN = lastDLSNInPacket;
                }
            }
        }
        transmitPacket.processTransmitComplete(entryId, transmitResult.get());

        if (cancelPendingPromises) {
            packetCurrent.cancelPromises(new WriteCancelledException(fullyQualifiedLogSegment, transmitResultToException(transmitResult.get())));
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
