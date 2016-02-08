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

import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.logsegment.LogSegmentWriter;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.Sizable;
import com.twitter.distributedlog.util.Utils;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
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

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import com.twitter.distributedlog.feature.CoreFeatureKeys;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.io.CompressionUtils;
import com.twitter.distributedlog.lock.DistributedReentrantLock;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.stats.OpStatsListener;
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

import scala.runtime.BoxedUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.LogRecordSet.MAX_LOGRECORD_SIZE;
import static com.twitter.distributedlog.LogRecordSet.MAX_LOGRECORDSET_SIZE;

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

    private static class BKTransmitPacket {

        private boolean isControl;
        private long logSegmentSequenceNo;
        private DLSN lastDLSN;
        private List<Promise<DLSN>> promiseList;
        private Promise<Integer> transmitComplete;
        private long transmitTime;
        private long maxTxId;
        private LogRecordSet.Writer writer;

        public BKTransmitPacket(String logName,
                                long logSegmentSequenceNo,
                                int initialBufferSize,
                                boolean envelopeBeforeTransmit,
                                CompressionCodec.Type codec,
                                StatsLogger statsLogger) {
            this.logSegmentSequenceNo = logSegmentSequenceNo;
            this.promiseList = new LinkedList<Promise<DLSN>>();
            this.isControl = false;
            this.transmitComplete = new Promise<Integer>();
            this.writer = LogRecordSet.newLogRecordSet(
                    logName,
                    initialBufferSize * 6 / 5,
                    envelopeBeforeTransmit,
                    codec,
                    statsLogger);
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
            writer.reset();
        }

        public long getLogSegmentSequenceNo() {
            return logSegmentSequenceNo;
        }

        public void addToPromiseList(Promise<DLSN> nextPromise, long txId) {
            promiseList.add(nextPromise);
            maxTxId = Math.max(maxTxId, txId);
        }

        public LogRecordSet.Writer getRecordSetWriter() {
            return writer;
        }

        private void satisfyPromises(long entryId) {
            long nextSlotId = 0;
            for(Promise<DLSN> promise : promiseList) {
                promise.setValue(new DLSN(logSegmentSequenceNo, entryId, nextSlotId));
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
                lastDLSN = new DLSN(logSegmentSequenceNo, entryId, promiseList.size() - 1);
            }
            return lastDLSN;
        }

        public void setControl(boolean control) {
            isControl = control;
        }

        public boolean isControl() {
            return isControl;
        }

        public int awaitTransmitComplete(long timeout, TimeUnit unit) throws Exception {
            return Await.result(transmitComplete, Duration.fromTimeUnit(timeout, unit));
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
    private final boolean isDurableWriteEnabled;
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
    final private AtomicReference<Exception> scheduledFlushException = new AtomicReference<Exception>(null);
    private boolean enforceLock = true;
    private boolean closed = true;
    private final boolean enableRecordCounts;
    private int positionWithinLogSegment = 0;
    private final long logSegmentSequenceNumber;
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

    // add complete processing
    private final SafeQueueingFuturePool<Void> addCompleteFuturePool;

    private final AlertStatsLogger alertStatsLogger;
    private final WriteLimiter writeLimiter;
    private final FailureInjector writeDelayInjector;

    static interface FailureInjector {
        void inject();
    }

    static class NullFailureInjector implements FailureInjector {
        public static NullFailureInjector INSTANCE = new NullFailureInjector();
        @Override
        public void inject() {
        }
    }

    // manage failure injection for the class
    static class RandomDelayFailureInjector implements FailureInjector {
        private final DynamicDistributedLogConfiguration dynConf;

        public RandomDelayFailureInjector(DynamicDistributedLogConfiguration dynConf) {
            this.dynConf = dynConf;
        }

        private int delayMs() {
            return dynConf.getEIInjectedWriteDelayMs();
        }

        private double delayPct() {
            return dynConf.getEIInjectedWriteDelayPercent();
        }

        private boolean enabled() {
            return delayMs() > 0 && delayPct() > 0;
        }

        @Override
        public void inject() {
            try {
                if (enabled() && Utils.randomPercent(delayPct())) {
                    Thread.sleep(delayMs());
                }
            } catch (InterruptedException ex) {
                LOG.warn("delay was interrupted ", ex);
            }
        }
    }

    /**
     * Construct an edit log output stream which writes to a ledger.
     */
    protected BKLogSegmentWriter(String streamName,
                                 String logSegmentName,
                                 DistributedLogConfiguration conf,
                                 int logSegmentMetadataVersion,
                                 LedgerHandle lh, DistributedReentrantLock lock,
                                 long startTxId, long logSegmentSequenceNumber,
                                 ScheduledExecutorService executorService,
                                 FuturePool orderedFuturePool,
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
        this.lh = lh;
        this.lock = lock;
        this.lock.acquire(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

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
        this.packetCurrent = new BKTransmitPacket(
                streamName,
                logSegmentSequenceNumber,
                Math.max(transmissionThreshold, 1024),
                envelopeBeforeTransmit(),
                compressionType,
                envelopeStatsLogger);
        this.packetPrevious = null;
        this.startTxId = startTxId;
        this.lastTxId = startTxId;
        this.lastTxIdFlushed = startTxId;
        this.lastTxIdAcknowledged = startTxId;
        this.enableRecordCounts = conf.getEnableRecordCounts();
        this.flushTimeoutSeconds = conf.getLogFlushTimeoutSeconds();
        this.immediateFlushEnabled = conf.getImmediateFlushEnabled();
        this.isDurableWriteEnabled = conf.isDurableWriteEnabled();

        // Failure injection
        if (conf.getEIInjectWriteDelay()) {
            this.writeDelayInjector = new RandomDelayFailureInjector(dynConf);
        } else {
            this.writeDelayInjector = NullFailureInjector.INSTANCE;
        }

        // If we are transmitting immediately (threshold == 0) and if immediate
        // flush is enabled, we don't need the periodic flush task
        final int configuredPeriodicFlushFrequency = dynConf.getPeriodicFlushFrequencyMilliSeconds();
        if (!immediateFlushEnabled || (0 != this.transmissionThreshold)) {
            int periodicFlushFrequency = configuredPeriodicFlushFrequency;
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
            this.addCompleteFuturePool = new SafeQueueingFuturePool<Void>(orderedFuturePool);
        } else {
            this.addCompleteFuturePool = null;
        }
        assert(!this.immediateFlushEnabled || (null != this.executorService));
        this.closed = false;
        this.lastTransmit = Stopwatch.createStarted();
    }

    String getFullyQualifiedLogSegment() {
        return fullyQualifiedLogSegment;
    }

    @VisibleForTesting
    DistributedReentrantLock getLock() {
        return this.lock;
    }

    @VisibleForTesting
    void setTransmitResult(int rc) {
        transmitResult.set(rc);
    }

    protected final LedgerHandle getLedgerHandle() {
        return this.lh;
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
        return lh.getLength();
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

    private BKTransmitPacket getTransmitPacket() {
        return new BKTransmitPacket(
                streamName,
                logSegmentSequenceNumber,
                Math.max(transmissionThreshold, getAverageTransmitSize()),
                envelopeBeforeTransmit(),
                compressionType,
                envelopeStatsLogger);
    }

    private boolean envelopeBeforeTransmit() {
        return LogSegmentMetadata.supportsEnvelopedEntries(logSegmentMetadataVersion);
    }

    @Override
    public void close() throws IOException {
        IOException throwExc = closeInternal(false);

        if (null != throwExc) {
            throw throwExc;
        }
    }

    @Override
    public void abort() throws IOException {
        IOException throwExc = closeInternal(true);
        if (null != throwExc) {
            throw throwExc;
        }
    }

    private void flushAddCompletes() {
        if (null != addCompleteFuturePool) {
            addCompleteFuturePool.close();
        }
    }

    private synchronized void abortPacket(BKTransmitPacket packet) {
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

    private IOException closeInternal(boolean abort) {
        synchronized (this) {
            if (closed) {
                return null;
            }
            closed = true;
        }

        IOException throwExc = null;

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
            try {
                // BookKeeper fencing will disallow multiple writers, so if we have
                // already lost the lock, this operation will fail, we don't need enforce locking here.
                this.enforceLock = false;
                LOG.info("Flushing before closing log segment {}", getFullyQualifiedLogSegment());
                // flush any items in output buffer
                doFlush();
                // commit previous flushes
                commit();
            } catch (IOException exc) {
                throwExc = exc;
            }
        }

        LOG.info("Closing BKPerStreamLogWriter (abort={}) for {} :" +
                " lastDLSN = {} outstandingTransmits = {} writesPendingTransmit = {} addCompletesPending = {}",
                new Object[] { abort, fullyQualifiedLogSegment, getLastDLSN(),
                        outstandingTransmits.get(), getWritesPendingTransmit(), getPendingAddCompleteCount() });

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

        // close the log segment if it isn't in error state, so all the outstanding addEntry(s) will callback.
        if (null == throwExc && !isLogSegmentInError()) {
            // Synchronous closing the ledger handle, if we couldn't close a ledger handle successfully.
            // we should throw the exception to #closeToFinalize, so it would fail completing a log segment.
            try {
                lh.close();
            } catch (BKException.BKLedgerClosedException lce) {
                // if a ledger is already closed, we don't need to throw exception
            } catch (BKException bke) {
                if (!abort) {
                    throwExc = new IOException("Failed to close ledger for " + fullyQualifiedLogSegment, bke);
                }
            } catch (InterruptedException ie) {
                if (!abort) {
                    throwExc = new DLInterruptedException("Interrupted on closing ledger for " + fullyQualifiedLogSegment, ie);
                }
            }
        }

        lock.release(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

        // If add entry failed because of closing ledger above, we don't need to fail the close operation
        if (!abort && null == throwExc && shouldFailCompleteLogSegment()) {
            throwExc = new BKTransmitException("Closing an errored stream : ", transmitResult.get());
        }

        return throwExc;
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
                    transmit(false);
                } catch (IOException ioe) {
                    return Future.exception(new WriteCancelledException(fullyQualifiedLogSegment, ioe));
                }
                result = writeControlLogRecord(record);
                transmit(true);
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
        if ((packetCurrent.getRecordSetWriter().getNumBytes() + logRecordSize) > MAX_LOGRECORDSET_SIZE) {
            doFlush();
        }

        checkWriteLock();

        Promise<DLSN> dlsn = new Promise<DLSN>();
        dlsn.addEventListener(new OpStatsListener<DLSN>(writeTime));

        if (enableRecordCounts) {
            // Set the count here. The caller would appropriately increment it
            // if this log record is to be counted
            record.setPositionWithinLogSegment(positionWithinLogSegment);
        }

        try {
            packetCurrent.getRecordSetWriter().writeRecord(record);
        } catch (IOException e) {
            LOG.error("Failed to append record to output buffer for {} : ",
                    getFullyQualifiedLogSegment(), e);
            throw new WriteException(streamName, "Failed to append record to outputbuffer for "
                    + getFullyQualifiedLogSegment());
        }
        packetCurrent.addToPromiseList(dlsn, record.getTransactionId());

        if (record.getTransactionId() < lastTxId) {
            LOG.info("Log Segment {} TxId decreased Last: {} Record: {}",
                    new Object[] {fullyQualifiedLogSegment, lastTxId, record.getTransactionId()});
        }
        if (!record.isControl()) {
            // only update last tx id for user records
            lastTxId = record.getTransactionId();
            outstandingBytes += (20 + record.getPayload().length);
        }
        return dlsn;
    }

    synchronized private void writeControlLogRecord()
            throws BKTransmitException, WriteException, InvalidEnvelopedEntryException,
                   LockingException, LogRecordTooLongException {
        LogRecord controlRec = new LogRecord(lastTxId, "control".getBytes(UTF_8));
        controlRec.setControl();
        writeControlLogRecord(controlRec);
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
     * (TODO: move this method to the log writer level)
     *
     * Flushes all the data up to this point,
     * adds the end of stream marker and marks the stream
     * as read-only in the metadata. No appends to the
     * stream will be allowed after this point
     */
    public void markEndOfStream() throws IOException {
        synchronized (this) {
            writeEndOfStreamMarker();
            streamEnded = true;
            doFlush();
        }
        flushAndSync();
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

    @Override
    synchronized public long setReadyToFlush() throws IOException {
        return doFlush();
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
    synchronized long doFlush()
            throws BKTransmitException, WriteException, InvalidEnvelopedEntryException, LockingException {
        checkStateBeforeTransmit();

        if (transmit(false)) {
            shouldFlushControl.incrementAndGet();
        }
        return lastTxIdAcknowledged;
    }

    @Override
    public long flushAndSync() throws IOException {
        commitPhaseOne();
        return commitPhaseTwo();
    }

    public void commit() throws IOException {
        commitPhaseOne();
        commitPhaseTwo();
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
                doFlush();
            } else {
                scheduleFlushWithDelayIfNeeded(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        doFlush();
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

    /**
     * Commit Phase one: wait for previous transmit to be completed and
     * write a control record to commit previous transmit.
     *
     * @return last acknowledged txn id.
     * @throws LockingException if segment writer lost lock during waiting previous transmit.
     * @throws BKTransmitException if previous transmit failed
     * @throws FlushException when failed to write control record
     * @throws DLInterruptedException when interrupted on waiting for previous transmit complete
     */
    long commitPhaseOne() throws LockingException, BKTransmitException, FlushException, DLInterruptedException {
        awaitPreviousTransmitComplete();

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

            return getLastTxIdAcknowledged();
        }
    }

    /**
     * Commit Phase two: wait for the control record written in phase one to be completed.
     *
     * @return last acknowledged tx id.
     * @throws FlushException when failed to wait for the control record to be completed
     * @throws DLInterruptedException when interrupted on waiting for control record written in phase one to be completed.
     */
    long commitPhaseTwo() throws FlushException, DLInterruptedException {
        if (preFlushCounter > 0) {
            try {
                awaitPreviousTransmitComplete();
            } catch (DLInterruptedException dlie) {
                shouldFlushControl.addAndGet(preFlushCounter);
                throw dlie;
            } catch (Exception exc) {
                shouldFlushControl.addAndGet(preFlushCounter);
                throw new FlushException("Flush encountered an error while writing data to backend", getLastTxId(), getLastTxIdAcknowledged(), exc);
            } finally {
                preFlushCounter = 0;
            }
        }
        synchronized (this) {
            return getLastTxIdAcknowledged();
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
            lock.checkOwnershipAndReacquire(false);
        }
    }

    /**
     * Wait for previous transmit to complete.
     *
     * @throws LockingException if the segment writer already lost lock
     * @throws BKTransmitException if previous transmit failed
     * @throws FlushException if previous transmit doesn't complete in given flush timeout
     * @throws DLInterruptedException when interrupted on waiting previous transmit result
     */
    private void awaitPreviousTransmitComplete()
        throws LockingException, BKTransmitException, FlushException, DLInterruptedException {
        checkWriteLock();

        long txIdToBePersisted;
        synchronized (this) {
            txIdToBePersisted = lastTxIdFlushed;
        }

        final BKTransmitPacket lastPacket;
        synchronized (this) {
            lastPacket = packetPrevious;
        }

        int lastPacketRc;
        try {
            if (null != lastPacket) {
                lastPacketRc = lastPacket.awaitTransmitComplete(flushTimeoutSeconds, TimeUnit.SECONDS);
            } else {
                lastPacketRc = BKException.Code.OK;
            }
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on flushing " + getFullyQualifiedLogSegment()
                    + " : last tx id = " + getLastTxId() + ", last acked tx id = " + getLastTxIdAcknowledged(), ie);
        } catch (Exception te) {
            throw new FlushException("Flush request timed out", getLastTxId(), getLastTxIdAcknowledged());
        }

        if (lastPacketRc != BKException.Code.OK) {
            LOG.error("Log Segment {} Failed to write to bookkeeper; Error is {}",
                fullyQualifiedLogSegment, BKException.getMessage(lastPacketRc));
            throw transmitResultToException(lastPacketRc);
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
     *
     * @param isControl if transmit the packet as a control
     * @return true if we successfully tranmit an entry, false if no data to tranmit
     * @throws BKTransmitException if the segment writer is already in error state
     * @throws LockingException if the segment writer lost lock before transmit
     * @throws WriteException if failed to create the envelope for the data to transmit
     * @throws InvalidEnvelopedEntryException when built an invalid enveloped entry
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

                if (packetCurrent.getRecordSetWriter().getNumRecords() == 0) {
                    // Control flushes always have at least the control record to flush
                    transmitDataMisses.inc();
                    return false;
                }

                packet = packetCurrent;
                packet.setControl(isControl);
                outstandingBytes = 0;
                packetCurrent = getTransmitPacket();
                packetPrevious = packet;
                lastTxIdFlushed = lastTxId;

                if (!isControl) {
                    numBytes += packet.getRecordSetWriter().getNumBytes();
                    numFlushesSinceRestart++;
                }
            }

            Buffer toSend;
            try {
                toSend = packet.getRecordSetWriter().serialize();
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

        return (packetCurrent.getRecordSetWriter().getNumRecords() > 0);
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
                            fullyQualifiedLogSegment, entryId, rc);
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
                    transmitDataPacketSize.registerFailedEvent(transmitPacket.getRecordSetWriter().getNumBytes());
                }
            } else {
                // If we had data that we flushed then we need it to make sure that
                // background flush in the next pass will make the previous writes
                // visible by advancing the lastAck
                if (!transmitPacket.isControl()) {
                    transmitDataPacketSize.registerSuccessfulEvent(transmitPacket.getRecordSetWriter().getNumBytes());
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

    @Override
    synchronized public void run()  {
        backgroundFlush(false);
    }

    synchronized private void backgroundFlush(boolean controlFlushOnly)  {
        if (closed) {
            // if the log segment is closing, skip any background flushing
            LOG.info("Skip background flushing since log segment {} is closing.", getFullyQualifiedLogSegment());
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

                transmit(!newData);
                pFlushSuccesses.inc();
            } else {
                pFlushMisses.inc();
            }
        } catch (IOException exc) {
            LOG.error("Log Segment {}: Error encountered by the periodic flush", fullyQualifiedLogSegment, exc);
        }
    }

    private static BKTransmitException transmitResultToException(int transmitResult) {
        return new BKTransmitException("Failed to write to bookkeeper; Error is ("
            + transmitResult + ") "
            + BKException.getMessage(transmitResult), transmitResult);
    }
}
