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

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.WriteException;

import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Output stream for BookKeeper based Distributed Log Manager.
 * Multiple complete log records are packed into a single bookkeeper
 * entry before sending it over the network. The fact that the log record entries
 * are complete in the bookkeeper entries means that each bookkeeper log entry
 * can be read as a complete edit log. This is useful for reading, as we don't
 * need to read through the entire log segment to get the last written entry.
 */
class BKPerStreamLogWriter implements LogWriter, AddCallback, Runnable, CloseCallback {
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
            lastDLSN = new DLSN(ledgerSequenceNo, entryId, nextSlotId-1);
        }

        private void cancelPromises(int transmitResult) {
            for(Promise<DLSN> promise : promiseList) {
                promise.setException(new BKTransmitException("Failed to write to bookkeeper; Error is ("
                    + transmitResult + ") "
                    + BKException.getMessage(transmitResult), transmitResult));
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

        public DLSN getLastDLSN() {
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
    private final AtomicInteger outstandingRequests;
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
    private boolean streamEnded = false;
    private ScheduledFuture<?> periodicFlushSchedule = null;
    private boolean enforceLock = true;
    private boolean closed = true;
    private final boolean enableRecordCounts;
    private int recordCount = 0;
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

        StatsLogger transmitOutstandingLogger = transmitStatsLogger.scope("outstanding");
        String statPrefixForStream = streamName.replaceAll(":|<|>|/", "_");
        // outstanding requests
        if (conf.getEnablePerStreamStat()) {
            transmitOutstandingLogger.registerGauge(statPrefixForStream + "_requests", new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return outstandingRequests.get();
                }
            });
        }

        outstandingRequests = new AtomicInteger(0);
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

        // If we are transmitting immediately (threshold == 0) and if immediate
        // flush is enabled, we don't need the periodic flush task
        if (!immediateFlushEnabled || (0 != this.transmissionThreshold)) {
            int periodicFlushFrequency = conf.getPeriodicFlushFrequencyMilliSeconds();
            if (periodicFlushFrequency > 0 && executorService != null) {
                periodicFlushSchedule = executorService.scheduleAtFixedRate(this,
                        periodicFlushFrequency/2, periodicFlushFrequency/2, TimeUnit.MILLISECONDS);
            }
        }
        this.conf = conf;
        this.executorService = executorService;
        this.orderedFuturePool = orderedFuturePool;
        assert(!this.immediateFlushEnabled || (null != this.executorService));
        this.closed = false;
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

        closeLedgerHandle(0);
        lock.release(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

        return throwExc;
    }

    private void closeLedgerHandle(long delayInMs) {
        if (null != executorService && delayInMs > 0) {
            LOG.debug("Log segment {}: scheduling closing ledger handle {} in {} ms.",
                    new Object[] {fullyQualifiedLogSegment, lh.getId(), delayInMs});
            executorService.schedule(new Runnable() {
                @Override
                public void run() {
                    LOG.debug("Closing ledger handle {} for {}.", lh.getId(), fullyQualifiedLogSegment);
                    lh.asyncClose(BKPerStreamLogWriter.this, null);
                }
            }, delayInMs, TimeUnit.MILLISECONDS);
        } else {
            LOG.debug("Closing ledger handle {} for {}.", lh.getId(), fullyQualifiedLogSegment);
            lh.asyncClose(this, null);
        }
    }

    /**
     * Close ledger handle callback.
     *
     * @param rc
     *          return code.
     * @param ledgerHandle
     *          ledger handle.
     * @param ctx
     *          callback ctx
     */
    @Override
    public void closeComplete(int rc, LedgerHandle ledgerHandle, Object ctx) {
        if (BKException.Code.OK == rc ||
            BKException.Code.NoSuchLedgerExistsException == rc ||
            BKException.Code.LedgerClosedException == rc) {
            LOG.debug("Closed ledger handle {} for {} successfully: {}", new Object[] {lh.getId(), fullyQualifiedLogSegment, rc});
        } else {
            LOG.warn("Failed to close ledger handle {} for {}, backoff and retry in {} ms.",
                new Object[] {lh.getId(), fullyQualifiedLogSegment, conf.getBKClientZKSessionTimeoutMilliSeconds()});
            closeLedgerHandle(conf.getBKClientZKSessionTimeoutMilliSeconds());
        }
    }

    @Override
    public void abort() {
        closeInternal(false, false);
    }

    @Override
    synchronized public void write(LogRecord record) throws IOException {
        writeUserRecord(record);
    }

    synchronized public Future<DLSN> asyncWrite(LogRecord record) {
        try {
            if (record.isControl()) {
                // write control record.
                Future<DLSN> result = writeControlLogRecord(record);
                transmit(true);
                return result;
            }
            return writeUserRecord(record);
        } catch (IOException ioe) {
            LOG.error("Encountered exception while writing a log record to stream {}", fullyQualifiedLogSegment, ioe);
            return Future.exception(ioe);
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

        // The count represents the number of user records up to the
        // current record
        // Increment the record count only when writing a user log record
        // Internally generated log records don't increment the count
        // writeInternal will always set a count regardless of whether it was
        // incremented or not.
        recordCount++;
        Future<DLSN> future = writeInternal(record);
        if (outstandingBytes > transmissionThreshold) {
            setReadyToFlush();
        }
        return future;
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
            record.setCount(recordCount);
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

    public long flushAndSyncPhaseOne() throws
        LockingException, OwnershipAcquireFailedException, BKTransmitException, FlushException {
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
            lock.checkWriteLock(false, DistributedReentrantLock.LockReason.PERSTREAMWRITER);
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
            latch = syncLatch = new CountDownLatch(outstandingRequests.get());
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
            throw new BKTransmitException("Failed to write to bookkeeper; Error is ("
                + transmitResult.get() + ") "
                + BKException.getMessage(transmitResult.get()), transmitResult.get());
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

            outstandingRequests.incrementAndGet();
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
        assert (ctx instanceof BKTransmitPacket);
        final BKTransmitPacket transmitPacket = (BKTransmitPacket) ctx;

        // notify the waiters before callback, otherwise, it might block flush
        CountDownLatch l;
        synchronized (syncLock) {
            outstandingRequests.decrementAndGet();
            l = syncLatch;
        }
        if (l != null) {
            l.countDown();
        }

        if (null != orderedFuturePool) {
            orderedFuturePool.apply(new Function0<Void>() {
                public Void apply() {
                    addCompleteDeferredProcessing(transmitPacket, entryId, rc);
                    return null;
                }
            });
        } else {
            addCompleteDeferredProcessing(transmitPacket, entryId, rc);
        }
    }

    private void addCompleteDeferredProcessing(final BKTransmitPacket transmitPacket,
                                              final long entryId,
                                              final int rc) {
        synchronized (this) {
            if (!transmitResult.compareAndSet(BKException.Code.OK, rc)) {
                LOG.warn("Log segment {}: Tried to set transmit result to ({}) but is already ({})",
                    new Object[] {fullyQualifiedLogSegment, rc, transmitResult.get()});
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
                        executorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                immediateFlush(true);
                            }
                        });
                    }
                }
            }
        }

        transmitPacket.processTransmitComplete(entryId, transmitResult.get());

        synchronized (this) {
            if (BKException.Code.OK == transmitResult.get() && !transmitPacket.isControl()) {
                if (lastDLSN.compareTo(transmitPacket.getLastDLSN()) < 0) {
                    lastDLSN = transmitPacket.getLastDLSN();
                }
            }
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
        immediateFlush(false);
    }

    synchronized private void immediateFlush(boolean controlFlushOnly)  {
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
        return (numRecords > (Integer.MAX_VALUE - recordCount));
    }

    public synchronized long getLastTxId() {
        return lastTxId;
    }

    synchronized long getLastTxIdAcknowledged() {
        return lastTxIdAcknowledged;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public DLSN getLastDLSN() {
        return lastDLSN;
    }
}
