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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Output stream for BookKeeper based Distributed Log Manager.
 * Multiple complete log records are packed into a single bookkeeper
 * entry before sending it over the network. The fact that the log record entries
 * are complete in the bookkeeper entries means that each bookkeeper log entry
 * can be read as a complete edit log. This is useful for reading, as we don't
 * need to read through the entire log segment to get the last written entry.
 */
class BKPerStreamLogWriter
    implements PerStreamLogWriter, AddCallback {
    static final Logger LOG = LoggerFactory.getLogger(BKPerStreamLogWriter.class);

    private DataOutputBuffer bufCurrent;
    private final AtomicInteger outstandingRequests;
    private final int transmissionThreshold;
    private final LedgerHandle lh;
    private CountDownLatch syncLatch;
    private final AtomicInteger transmitResult
        = new AtomicInteger(BKException.Code.OK);
    private final DistributedReentrantLock lock;
    private LogRecord.Writer writer;
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

    private final Queue<DataOutputBuffer> bufferQueue
        = new ConcurrentLinkedQueue<DataOutputBuffer>();

    /**
     * Construct an edit log output stream which writes to a ledger.
     */
    protected BKPerStreamLogWriter(DistributedLogConfiguration conf,
                                   LedgerHandle lh, DistributedReentrantLock lock,
                                   long startTxId)
        throws IOException {
        super();

        outstandingRequests = new AtomicInteger(0);
        syncLatch = null;
        this.lh = lh;
        this.lock = lock;
        this.lock.acquire("PerStreamLogWriter");
        this.transmissionThreshold
            = conf.getOutputBufferSize();

        this.bufCurrent = new DataOutputBuffer();
        this.writer = new LogRecord.Writer(bufCurrent);
        this.lastTxId = startTxId;
        this.lastTxIdFlushed = startTxId;
        this.lastTxIdAcknowledged = startTxId;
        this.flushTimeoutSeconds = conf.getLogFlushTimeoutSeconds();
    }

    public DataOutputBuffer getBuffer() {
        DataOutputBuffer b = bufferQueue.poll();
        if (b == null) {
            return new DataOutputBuffer(transmissionThreshold * 6 / 5);
        } else {
            return b;
        }
    }

    public void freeBuffer(DataOutputBuffer b) {
        b.reset();
        bufferQueue.add(b);
    }

    public long closeToFinalize() throws IOException {
        close();
        return lastTxId;
    }

    @Override
    public void close() throws IOException {
        if (!isStreamInError()) {
            setReadyToFlush();
            flushAndSync();
        }
        try {
            lh.close();
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted waiting on close", ie);
        } catch (BKException.BKLedgerClosedException lce) {
            LOG.debug("Ledger already closed");
        } catch (BKException bke) {
            throw new IOException("BookKeeper error during close", bke);
        } finally {
            lock.release("PerStreamLogWriterClose");
        }
    }

    @Override
    public void abort() throws IOException {
        try {
            lh.close();
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted waiting on close", ie);
        } catch (BKException bke) {
            throw new IOException("BookKeeper error during abort", bke);
        }

        lock.release("PerStreamLogWriterAbort");

    }

    @Override
    synchronized public void write(LogRecord record) throws IOException {
        lock.checkWriteLock();
        writeInternal(record);
        if (outstandingBytes > transmissionThreshold) {
            setReadyToFlush();
        }
    }

    public boolean isStreamInError() {
        return (transmitResult.get() != BKException.Code.OK);
    }

    synchronized public void writeInternal(LogRecord record) throws IOException {
        writer.writeOp(record);
        if (record.getTransactionId() < lastTxId) {
            LOG.info("TxId decreased Last: {} Record: {}", lastTxId, record.getTransactionId());
        }
        lastTxId = record.getTransactionId();
        if (!record.isControl()) {
            outstandingBytes += (20 + record.getPayload().length);
        }
    }

    synchronized private void writeControlLogRecord() throws IOException {
        lock.checkWriteLock();
        LogRecord controlRec = new LogRecord(lastTxId, "control".getBytes());
        controlRec.setControl();
        writeInternal(controlRec);
    }

    @Override
    synchronized public int writeBulk(List<LogRecord> records) throws IOException {
        lock.checkWriteLock();
        int numRecords = 0;
        for (LogRecord r : records) {
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
        LockingException, BKTransmitException, FlushException {
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

        return lastTxIdAcknowledged;
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
        return lastTxIdAcknowledged;
    }

    private void flushAndSyncInternal()
        throws LockingException, BKTransmitException, FlushException {
        lock.checkWriteLock();

        long txIdToBePersisted;

        synchronized (this) {
            txIdToBePersisted = lastTxIdFlushed;
            syncLatch = new CountDownLatch(outstandingRequests.get());
        }

        boolean waitSuccessful = false;
        try {
            waitSuccessful = syncLatch.await(flushTimeoutSeconds, TimeUnit.SECONDS);
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
     * Synchronised at the FSEditLog level. #write() and #setReadyToFlush()
     * are never called at the same time.
     */
    synchronized private boolean transmit(boolean isControl)
        throws BKTransmitException, LockingException {
        lock.checkWriteLock();

        if (!transmitResult.compareAndSet(BKException.Code.OK,
            BKException.Code.OK)) {
            LOG.error("Trying to write to an errored stream; Error is ({}) {}",
                transmitResult.get(),
                BKException.getMessage(transmitResult.get()));
            throw new BKTransmitException("Trying to write to an errored stream;"
                + " Error code : (" + transmitResult.get()
                + ") " + BKException.getMessage(transmitResult.get()));
        }
        if (bufCurrent.getLength() > 0) {
            DataOutputBuffer buf = bufCurrent;
            outstandingBytes = 0;
            bufCurrent = getBuffer();
            writer = new LogRecord.Writer(bufCurrent);
            lastTxIdFlushed = lastTxId;
            numFlushes++;

            if (!isControl) {
                numBytes += buf.getLength();
                numFlushesSinceRestart++;
            }

            lh.asyncAddEntry(buf.getData(), 0, buf.getLength(),
                this, buf);
            outstandingRequests.incrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    public void addComplete(int rc, LedgerHandle handle,
                            long entryId, Object ctx) {
        synchronized (this) {
            outstandingRequests.decrementAndGet();
            if (!transmitResult.compareAndSet(BKException.Code.OK, rc)) {
                LOG.warn("Tried to set transmit result to (" + rc + ") \""
                    + BKException.getMessage(rc) + "\""
                    + " but is already (" + transmitResult.get() + ") \""
                    + BKException.getMessage(transmitResult.get()) + "\"");
            }
            if (ctx instanceof DataOutputBuffer) {
                freeBuffer((DataOutputBuffer) ctx);
            }
            CountDownLatch l = syncLatch;
            if (l != null) {
                l.countDown();
            }
        }
    }

    public long getNumFlushes() {
        return numFlushes;
    }

    public void setNumFlushes(long numFlushes) {
        this.numFlushes = numFlushes;
    }

    public long getAverageTransmitSize() {
        if (numFlushesSinceRestart > 0) {
            return numBytes/numFlushesSinceRestart;
        }

        return 0;
    }

    public long getLastAddConfirmed() {
        return lh.getLastAddConfirmed();
    }
}
