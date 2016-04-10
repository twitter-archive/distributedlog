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
package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.BKTransmitException;
import com.twitter.distributedlog.FlushException;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogWriter;
import com.twitter.distributedlog.WriteLimiter;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.feature.CoreFeatureKeys;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.TimeoutException;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.DLSNUtil.*;

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
        public BKTransmitPacket(int initialBufferSize) {
            this.isControl = false;
            this.buffer = new DataOutputBuffer(initialBufferSize * 6 / 5);
            this.transmitComplete = new Promise<Integer>();
            this.numWrites = new AtomicInteger(0);
        }

        public void incWrites() {
            numWrites.getAndIncrement();
        }

        public int getWrites() {
            return numWrites.get();
        }

        public DataOutputBuffer getBuffer() {
            return buffer;
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

        boolean isControl;

        final DataOutputBuffer buffer;
        final Promise<Integer> transmitComplete;
        final AtomicInteger    numWrites;
    }

    private final String streamName;
    private BKTransmitPacket packetCurrent;
    private BKTransmitPacket packetPrevious;
    private final AtomicInteger outstandingTransmits;
    private final int transmissionThreshold;
    private final LedgerHandle lh;
    private final AtomicInteger transmitResult
        = new AtomicInteger(BKException.Code.OK);
    private final DistributedLock lock;
    private LogRecord.Writer writer;
    private long lastTxId = DistributedLogConstants.INVALID_TXID;
    private long lastTxIdFlushed = DistributedLogConstants.INVALID_TXID;
    private long lastTxIdAcknowledged = DistributedLogConstants.INVALID_TXID;
    private long outstandingBytes = 0;
    private AtomicInteger shouldFlushControl = new AtomicInteger(0);
    private final int flushTimeoutSeconds;
    private int preFlushCounter;
    private long numFlushesSinceRestart = 0;
    private long numBytes = 0;
    private boolean periodicFlushNeeded = false;
    private boolean streamEnded = false;
    private ScheduledFuture<?> periodicFlushSchedule = null;
    private boolean enforceLock = true;
    private final boolean enableRecordCounts;
    private int recordCount = 0;

    // stats
    private final StatsLogger statsLogger;
    private final Counter transmitDataSuccesses;
    private final Counter transmitDataMisses;
    private final OpStatsLogger transmitDataPacketSize;
    private final Counter transmitControlSuccesses;
    private final Counter pFlushSuccesses;
    private final Counter pFlushMisses;
    private final Counter pendingWrites;

    private final WriteLimiter writeLimiter;

    /**
     * Construct an edit log output stream which writes to a ledger.
     */
    protected BKPerStreamLogWriter(String streamName,
                                   DistributedLogConfiguration conf,
                                   LedgerHandle lh,
                                   DistributedLock lock,
                                   long startTxId,
                                   ScheduledExecutorService executorService,
                                   StatsLogger statsLogger,
                                   PermitLimiter globalWriteLimiter)
        throws IOException {
        super();

        // set up a write limiter
        PermitLimiter streamWriteLimiter = null;
        if (conf.getPerWriterOutstandingWriteLimit() < 0) {
            streamWriteLimiter = PermitLimiter.NULL_PERMIT_LIMITER;
        } else {
            Feature disableWriteLimitFeature = new SettableFeature(
                    CoreFeatureKeys.DISABLE_WRITE_LIMIT.name().toLowerCase(), 0);
            streamWriteLimiter = new SimplePermitLimiter(
                conf.getOutstandingWriteLimitDarkmode(),
                conf.getPerWriterOutstandingWriteLimit(),
                statsLogger.scope("streamWriteLimiter"),
                false,
                disableWriteLimitFeature);
        }
        this.writeLimiter = new WriteLimiter(streamName, streamWriteLimiter, globalWriteLimiter);

        // stats
        this.statsLogger = statsLogger;
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
        pendingWrites = segWriterStatsLogger.getCounter("pending");

        StatsLogger transmitOutstandingLogger = transmitStatsLogger.scope("outstanding");
        if (conf.getEnablePerStreamStat()) {
            String statPrefixForStream = streamName.replaceAll(":|<|>|/", "_");
            // outstanding requests
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


        this.outstandingTransmits = new AtomicInteger(0);
        this.streamName = streamName;
        this.lh = lh;
        this.lock = lock;
        this.lock.checkOwnershipAndReacquire();

        if (conf.getOutputBufferSize() > DistributedLogConstants.MAX_TRANSMISSION_SIZE) {
            LOG.warn("Setting output buffer size {} greater than max transmission size {}",
                conf.getOutputBufferSize(), DistributedLogConstants.MAX_TRANSMISSION_SIZE);
            this.transmissionThreshold = DistributedLogConstants.MAX_TRANSMISSION_SIZE;
        } else {
            this.transmissionThreshold = conf.getOutputBufferSize();
        }

        this.packetCurrent = new BKTransmitPacket(Math.max(transmissionThreshold, 1024));
        this.writer = new LogRecord.Writer(packetCurrent.getBuffer());
        this.packetPrevious = null;
        this.lastTxId = startTxId;
        this.lastTxIdFlushed = startTxId;
        this.lastTxIdAcknowledged = startTxId;
        this.enableRecordCounts = conf.getEnableRecordCounts();
        this.flushTimeoutSeconds = conf.getLogFlushTimeoutSeconds();
        int periodicFlushFrequency = conf.getPeriodicFlushFrequencyMilliSeconds();
        if (periodicFlushFrequency > 0 && executorService != null) {
            periodicFlushSchedule = executorService.scheduleAtFixedRate(this,
                    periodicFlushFrequency/2, periodicFlushFrequency/2, TimeUnit.MILLISECONDS);
        }
    }

    private BKTransmitPacket getTransmitPacket() {
        return new BKTransmitPacket(Math.max(transmissionThreshold, getAverageTransmitSize()));
    }

    public void closeToFinalize() throws IOException {
        // Its important to enforce the write-lock here as we are going to make
        // metadata changes following this call
        closeInternal(true, true);
    }

    @Override
    public void close() throws IOException {
        closeInternal(true, false);
    }

    public void closeInternal(boolean attemptFlush, boolean enforceLock) throws IOException {
        IOException throwExc = null;
        // Cancel the periodic flush schedule first
        // The task is allowed to exit gracefully
        // The attempt to flush will synchronize with the
        // last execution of the task
        if (null != periodicFlushSchedule) {
            periodicFlushSchedule.cancel(false);
        }

        if (attemptFlush) {
            if (!isStreamInError()) {
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
            } else {
                throwExc = new BKTransmitException("Failed to write to bookkeeper; Error is ("
                    + transmitResult.get() + ") ", transmitResult.get());
            }
        }

        try {
            lh.close();
        } catch (InterruptedException ie) {
            LOG.warn("Interrupted waiting on close", ie);
        } catch (BKException.BKLedgerClosedException lce) {
            LOG.debug("Ledger already closed");
        } catch (BKException bke) {
            LOG.warn("BookKeeper error during close", bke);
        }

        if (attemptFlush && (null != throwExc)) {
            throw throwExc;
        }
    }

    @Override
    public void abort() throws IOException {
        closeInternal(false, false);
    }

    @Override
    synchronized public void write(LogRecord record) throws IOException {
        if (streamEnded) {
            throw new EndOfStreamException("Writing to a stream after it has been marked as completed");
        }

        // The count represents the number of user records up to the
        // current record
        // Increment the record count only when writing a user log record
        // Internally generated log records don't increment the count
        // writeInternal will always set a count regardless of whether it was
        // incremented or not.
        recordCount++;
        writeInternal(record);
        if (outstandingBytes > transmissionThreshold) {
            setReadyToFlush();
        }
    }

    public boolean isStreamInError() {
        return (transmitResult.get() != BKException.Code.OK);
    }

    synchronized public void writeInternal(LogRecord record) throws IOException {
        int logRecordSize = getRecordPersistentSize(record);

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

        if (enableRecordCounts) {
            // Set the count here. The caller would appropriately increment it
            // if this log record is to be counted
            setPositionWithinLogSegment(record, recordCount);
        }

        writeLimiter.acquire();
        writer.writeOp(record);
        packetCurrent.incWrites();
        pendingWrites.inc();

        if (record.getTransactionId() < lastTxId) {
            LOG.info("TxId decreased Last: {} Record: {}", lastTxId, record.getTransactionId());
        }
        lastTxId = record.getTransactionId();
        if (!record.isControl()) {
            outstandingBytes += (20 + record.getPayload().length);
        }
    }

    synchronized private void writeControlLogRecord() throws IOException {
        LogRecord controlRec = new LogRecord(lastTxId, "control".getBytes(UTF_8));
        setControlRecord(controlRec);
        writeInternal(controlRec);
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
        setEndOfStream(endOfStreamRec);
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

    @SuppressWarnings("deprecated")
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
        return setReadyToFlush(false);
    }

    synchronized long setReadyToFlush(boolean isControl) throws IOException {
        if (transmit(isControl)) {
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
                    throw new FlushException("Flush encountered an error while writing data to the backend", getLastTxId(), getLastTxIdAcknowledged(), exc);
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
            lock.checkOwnershipAndReacquire();
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
            LOG.error("Stream {} failed to write to bookkeeper; Error is {}",
                streamName, BKException.getMessage(transmitResult.get()));
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
            LOG.error("Trying to write to an errored stream; Error is ({}) {}",
                transmitResult.get(),
                BKException.getMessage(transmitResult.get()));
            throw new BKTransmitException("Trying to write to an errored stream;"
                + " Error code : (" + transmitResult.get() + ") ", transmitResult.get());
        }
        if (packetCurrent.getBuffer().getLength() > 0) {
            BKTransmitPacket packet = packetCurrent;
            packet.setControl(isControl);
            outstandingBytes = 0;
            packetCurrent = getTransmitPacket();
            packetPrevious = packet;
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

            outstandingTransmits.incrementAndGet();
            periodicFlushNeeded = false;
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
    public void addComplete(int rc, LedgerHandle handle,
                            long entryId, Object ctx) {
        synchronized (this) {
            assert (ctx instanceof BKTransmitPacket);
            BKTransmitPacket transmitPacket = (BKTransmitPacket) ctx;

            outstandingTransmits.decrementAndGet();
            if (!transmitResult.compareAndSet(BKException.Code.OK, rc)) {
                LOG.warn("Tried to set transmit result to (" + rc + ") \""
                    + BKException.getMessage(rc) + "\""
                    + " but is already (" + transmitResult.get() + ") \""
                    + BKException.getMessage(transmitResult.get()) + "\"");
                if (!transmitPacket.isControl) {
                    transmitDataPacketSize.registerFailedEvent(transmitPacket.buffer.getLength());
                }
            }
            else {
                // If we had data that we flushed then we need it to make sure that
                // background flush in the next pass will make the previous writes
                // visible by advancing the lastAck
                periodicFlushNeeded = !transmitPacket.isControl();
                if (!transmitPacket.isControl()) {
                    transmitDataPacketSize.registerSuccessfulEvent(transmitPacket.buffer.getLength());
                }
            }

            transmitPacket.setTransmitComplete(rc);
            pendingWrites.add(-transmitPacket.getWrites());
            writeLimiter.release(transmitPacket.getWrites());
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
        try {
            boolean newData = haveDataToTransmit();

            if (periodicFlushNeeded || newData) {
                // If we need this periodic transmit to persist previously written data but
                // there is no new data (which would cause the transmit to be skipped) generate
                // a control record
                if (!newData) {
                    writeControlLogRecord();
                }

                setReadyToFlush(!newData);
                pFlushSuccesses.inc();
            } else {
                pFlushMisses.inc();
            }
        } catch (IOException exc) {
            LOG.error("Error encountered by the periodic flush", exc);
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

    public static BKTransmitException transmitResultToException(int transmitResult) {
        return new BKTransmitException("Failed to write to bookkeeper; Error is ("
            + transmitResult + ") "
            + BKException.getMessage(transmitResult), transmitResult);
    }
}
