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
import com.twitter.distributedlog.util.Pair;
import com.twitter.util.Future;
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
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
                LOG.trace("Promise satisfied");
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
                    + BKException.getMessage(transmitResult)));
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

    private BKTransmitPacket packetCurrent;
    private final AtomicInteger outstandingRequests;
    private final int transmissionThreshold;
    private final LedgerHandle lh;
    private CountDownLatch syncLatch;
    private final AtomicInteger transmitResult
        = new AtomicInteger(BKException.Code.OK);
    private final DistributedReentrantLock lock;
    private LogRecord.Writer writer;
    private DLSN lastDLSN = DLSN.InvalidDLSN;
    private long lastTxId = DistributedLogConstants.INVALID_TXID;
    private long lastTxIdFlushed = DistributedLogConstants.INVALID_TXID;
    private long lastTxIdAcknowledged = DistributedLogConstants.INVALID_TXID;
    private long outstandingBytes = 0;
    private AtomicInteger shouldFlushControl = new AtomicInteger(0);
    private final int flushTimeoutSeconds;
    private int preFlushCounter;
    private long numFlushes = 0;
    private long numFlushesSinceRestart = 0;
    private long numBytes = 0;
    private boolean periodicFlushNeeded = false;
    private boolean streamEnded = false;
    private ScheduledFuture<?> periodicFlushSchedule = null;
    private boolean enforceLock = true;
    private final long ledgerSequenceNumber;

    private final Queue<BKTransmitPacket> transmitPacketQueue
        = new ConcurrentLinkedQueue<BKTransmitPacket>();

    // stats
    private final StatsLogger statsLogger;
    private final Counter transmitSuccesses;
    private final Counter transmitMisses;
    private final OpStatsLogger transmitPacketSize;
    private final Counter pFlushSuccesses;
    private final Counter pFlushMisses;

    /**
     * Construct an edit log output stream which writes to a ledger.
     */
    protected BKPerStreamLogWriter(DistributedLogConfiguration conf,
                                   LedgerHandle lh, DistributedReentrantLock lock,
                                   long startTxId, long ledgerSequenceNumber,
                                   ScheduledExecutorService executorService,
                                   StatsLogger statsLogger)
        throws IOException {
        super();

        // stats
        this.statsLogger = statsLogger;
        StatsLogger flushStatsLogger = statsLogger.scope("flush");
        StatsLogger pFlushStatsLogger = flushStatsLogger.scope("periodic");
        pFlushSuccesses = pFlushStatsLogger.getCounter("success");
        pFlushMisses = pFlushStatsLogger.getCounter("miss");
        // transmit
        StatsLogger transmitStatsLogger = statsLogger.scope("transmit");
        transmitSuccesses = transmitStatsLogger.getCounter("success");
        transmitMisses = transmitStatsLogger.getCounter("miss");
        transmitPacketSize =  transmitStatsLogger.getOpStatsLogger("packetsize");
        StatsLogger transmitOutstandingLogger = transmitStatsLogger.scope("outstanding");
        // outstanding requests
        transmitOutstandingLogger.registerGauge("requests", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return outstandingRequests.get();
            }
        });

        outstandingRequests = new AtomicInteger(0);
        syncLatch = null;
        this.lh = lh;
        this.lock = lock;
        this.lock.acquire("BKPerStreamLogWriter");

        if (conf.getOutputBufferSize() > DistributedLogConstants.MAX_TRANSMISSION_SIZE) {
            LOG.warn("Setting output buffer size {} greater than max transmission size {}",
                conf.getOutputBufferSize(), DistributedLogConstants.MAX_TRANSMISSION_SIZE);
            this.transmissionThreshold = DistributedLogConstants.MAX_TRANSMISSION_SIZE;
        } else {
            this.transmissionThreshold = conf.getOutputBufferSize();
        }

        this.ledgerSequenceNumber = ledgerSequenceNumber;
        this.packetCurrent = new BKTransmitPacket(ledgerSequenceNumber, Math.max(transmissionThreshold, 1024));
        this.writer = new LogRecord.Writer(packetCurrent.getBuffer());
        this.lastTxId = startTxId;
        this.lastTxIdFlushed = startTxId;
        this.lastTxIdAcknowledged = startTxId;
        this.flushTimeoutSeconds = conf.getLogFlushTimeoutSeconds();
        int periodicFlushFrequency = conf.getPeriodicFlushFrequencyMilliSeconds();
        if (periodicFlushFrequency > 0 && executorService != null) {
            periodicFlushSchedule = executorService.scheduleAtFixedRate(this,
                    periodicFlushFrequency/2, periodicFlushFrequency/2, TimeUnit.MILLISECONDS);
        }
    }

    private BKTransmitPacket getTransmitPacket() {
        BKTransmitPacket packet = transmitPacketQueue.poll();
        if (packet == null) {
            return new BKTransmitPacket(ledgerSequenceNumber,
                Math.max(transmissionThreshold, getAverageTransmitSize()));
        } else {
            return packet;
        }
    }

    private void releasePacket(BKTransmitPacket packet) {
        packet.reset();
        transmitPacketQueue.add(packet);
    }

    public Pair<Long, DLSN> closeToFinalize() throws IOException {
        // Its important to enforce the write-lock here as we are going to make
        // metadata changes following this call
        closeInternal(true, true);
        synchronized (this) {
            return Pair.of(lastTxId, lastDLSN);
        }
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

        try {
            lh.close();
        } catch (InterruptedException ie) {
            LOG.warn("Interrupted waiting on close", ie);
        } catch (BKException.BKLedgerClosedException lce) {
            LOG.debug("Ledger already closed");
        } catch (BKException bke) {
            LOG.warn("BookKeeper error during close", bke);
        } finally {
            lock.release("PerStreamLogWriterClose");
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
        asyncWrite(record);
    }

    synchronized public Future<DLSN> asyncWrite(LogRecord record) throws IOException {
        if (streamEnded) {
            throw new EndOfStreamException("Writing to a stream after it has been marked as completed");
        }

        if ((record.getTransactionId() < 0) ||
            (record.getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

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
        writer.writeOp(record);
        packetCurrent.addToPromiseList(dlsn);

        if (record.getTransactionId() < lastTxId) {
            LOG.info("TxId decreased Last: {} Record: {}", lastTxId, record.getTransactionId());
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
        if (streamEnded) {
            throw new EndOfStreamException("Writing to a stream after it has been marked as completed");
        }

        int numRecords = 0;
        for (LogRecord r : records) {
            if ((r.getTransactionId() < 0) ||
                (r.getTransactionId() == DistributedLogConstants.MAX_TXID)) {
                throw new IOException("Invalid Transaction Id");
            }
            writeInternal(r);
            numRecords++;
            if (outstandingBytes > transmissionThreshold) {
                setReadyToFlush();
            }
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
        }

        if (preFlushCounter > 0) {
            try {
                writeControlLogRecord();
                transmit(true);
            } catch (Exception exc) {
                shouldFlushControl.addAndGet(preFlushCounter);
                preFlushCounter = 0;
                throw new FlushException("Flush error", exc);
            }
        }

        synchronized (this) {
            return lastTxIdAcknowledged;
        }
    }

    public long flushAndSyncPhaseTwo() throws FlushException {
        if (preFlushCounter > 0) {
            try {
                flushAndSyncInternal();
            } catch (Exception exc) {
                shouldFlushControl.addAndGet(preFlushCounter);
                throw new FlushException("Flush error", exc);
            } finally {
                preFlushCounter = 0;
            }
        }
        synchronized (this) {
            return lastTxIdAcknowledged;
        }
    }

    private synchronized CountDownLatch getSyncLatch() {
        return syncLatch;
    }

    private void checkWriteLock() throws LockingException {
        if (enforceLock) {
            lock.checkWriteLock(false);
        }
    }

    private void flushAndSyncInternal()
        throws LockingException, BKTransmitException, FlushException {
        checkWriteLock();

        long txIdToBePersisted;

        synchronized (this) {
            txIdToBePersisted = lastTxIdFlushed;
            syncLatch = new CountDownLatch(outstandingRequests.get());
        }

        boolean waitSuccessful = false;
        try {
            waitSuccessful = getSyncLatch().await(flushTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            throw new FlushException("Wait for Flush Interrupted", ie);
        }

        if (!waitSuccessful) {
            throw new FlushException("Flush Timeout");
        }

        synchronized (this) {
            syncLatch = null;
        }

        if (transmitResult.get() != BKException.Code.OK) {
            LOG.error("Failed to write to bookkeeper; Error is ({}) {}",
                transmitResult.get(), BKException.getMessage(transmitResult.get()));
            throw new BKTransmitException("Failed to write to bookkeeper; Error is ("
                + transmitResult.get() + ") "
                + BKException.getMessage(transmitResult.get()));
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
                + " Error code : (" + transmitResult.get()
                + ") " + BKException.getMessage(transmitResult.get()));
        }
        if (packetCurrent.getBuffer().getLength() > 0) {
            BKTransmitPacket packet = packetCurrent;
            packet.setControl(isControl);
            outstandingBytes = 0;
            packetCurrent = getTransmitPacket();
            writer = new LogRecord.Writer(packetCurrent.getBuffer());
            lastTxIdFlushed = lastTxId;
            numFlushes++;

            if (!isControl) {
                numBytes += packet.getBuffer().getLength();
                numFlushesSinceRestart++;
            }

            lh.asyncAddEntry(packet.getBuffer().getData(), 0, packet.getBuffer().getLength(),
                this, packet);
            transmitSuccesses.inc();
            outstandingRequests.incrementAndGet();
            periodicFlushNeeded = false;
            return true;
        } else {
            transmitMisses.inc();
        }
        return false;
    }

    /**
     *  Checks if there is any data to transmit so that the periodic flush
     *  task can determine if there is anything it needs to do
     */
    synchronized private boolean haveDataToTransmit() throws IOException {
        if (!transmitResult.compareAndSet(BKException.Code.OK, BKException.Code.OK)) {
            // Even if there is data it cannot be transmitted, so effectively nothing to send
            return false;
        }

        return (packetCurrent.getBuffer().getLength() > 0);
    }

    @Override
    public void addComplete(int rc, LedgerHandle handle,
                            long entryId, Object ctx) {
        assert (ctx instanceof BKTransmitPacket);
        BKTransmitPacket transmitPacket = (BKTransmitPacket) ctx;
        synchronized (this) {
            outstandingRequests.decrementAndGet();
            if (!transmitResult.compareAndSet(BKException.Code.OK, rc)) {
                LOG.warn("Tried to set transmit result to (" + rc + ") \""
                    + BKException.getMessage(rc) + "\""
                    + " but is already (" + transmitResult.get() + ") \""
                    + BKException.getMessage(transmitResult.get()) + "\"");
                transmitPacketSize.registerFailedEvent(transmitPacket.buffer.getLength());
            } else {
                // If we had data that we flushed then we need it to make sure that
                // background flush in the next pass will make the previous writes
                // visible by advancing the lastAck
                periodicFlushNeeded |= !transmitPacket.isControl();
                transmitPacketSize.registerSuccessfulEvent(transmitPacket.buffer.getLength());
            }
        }

        transmitPacket.processTransmitComplete(entryId, transmitResult.get());

        synchronized (this) {
            if (BKException.Code.OK == rc && !transmitPacket.isControl()) {
                if (lastDLSN.compareTo(transmitPacket.getLastDLSN()) < 0) {
                    lastDLSN = transmitPacket.getLastDLSN();
                }
            }
            releasePacket(transmitPacket);
            CountDownLatch l = syncLatch;
            if (l != null) {
                l.countDown();
            }
        }
    }

    public synchronized long getNumFlushes() {
        return numFlushes;
    }

    public synchronized void setNumFlushes(long numFlushes) {
        this.numFlushes = numFlushes;
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

                transmit(!newData);
                pFlushSuccesses.inc();
            } else {
                pFlushMisses.inc();
            }
        } catch (IOException exc) {
            LOG.error("Error encountered by the periodic flush", exc);
        }
    }
}
