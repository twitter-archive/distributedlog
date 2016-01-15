package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.twitter.distributedlog.callback.ReadAheadCallback;
import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LedgerDataAccessor {
    static final Logger LOG = LoggerFactory.getLogger(LedgerDataAccessor.class);

    private final String streamName;
    private final LinkedBlockingQueue<LogRecordWithDLSN> readAheadRecords;
    private final int maxCachedRecords;
    private final AtomicReference<DLSN> minActiveDLSN = new AtomicReference<DLSN>(DLSN.NonInclusiveLowerBound);
    private DLSN lastReadAheadDLSN = DLSN.InvalidDLSN;
    private final AtomicReference<IOException> lastException = new AtomicReference<IOException>();
    // callbacks
    private final AsyncNotification notification;
    private ReadAheadCallback readAheadCallback = null;

    // variables for idle reader detection
    private final Stopwatch lastEntryProcessTime;

    // Stats
    private final AtomicLong cacheBytes = new AtomicLong(0);

    private final AlertStatsLogger alertStatsLogger;
    private final StatsLogger statsLogger;
    private final OpStatsLogger readAheadDeliveryLatencyStat;
    private final OpStatsLogger negativeReadAheadDeliveryLatencyStat;
    // Flags on controlling delivery latency stats collection
    private final boolean traceDeliveryLatencyEnabled;
    private volatile boolean suppressDeliveryLatency = true;
    private final long deliveryLatencyWarnThresholdMillis;

    LedgerDataAccessor(String streamName,
                       StatsLogger statsLogger,
                       AlertStatsLogger alertStatsLogger,
                       AsyncNotification notification,
                       int maxCachedRecords,
                       boolean traceDeliveryLatencyEnabled,
                       long deliveryLatencyWarnThresholdMillis,
                       Ticker ticker) {
        this.streamName = streamName;
        this.maxCachedRecords = maxCachedRecords;
        this.notification = notification;

        // create the readahead queue
        readAheadRecords = new LinkedBlockingQueue<LogRecordWithDLSN>();

        // start the idle reader detection
        lastEntryProcessTime = Stopwatch.createStarted(ticker);

        // Flags to control delivery latency tracing
        this.traceDeliveryLatencyEnabled = traceDeliveryLatencyEnabled;
        this.deliveryLatencyWarnThresholdMillis = deliveryLatencyWarnThresholdMillis;
        // Stats
        StatsLogger readAheadStatsLogger = statsLogger.scope("readahead");
        this.statsLogger = readAheadStatsLogger;
        this.alertStatsLogger = alertStatsLogger;
        this.readAheadDeliveryLatencyStat =
                readAheadStatsLogger.getOpStatsLogger("delivery_latency");
        this.negativeReadAheadDeliveryLatencyStat =
                readAheadStatsLogger.getOpStatsLogger("negative_delivery_latency");
    }

    /**
     * Trigger read ahead callback
     */
    private synchronized void invokeReadAheadCallback() {
        if (null != readAheadCallback) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Cache has space, schedule the read ahead");
            }
            readAheadCallback.resumeReadAhead();
            readAheadCallback = null;
        }
    }

    /**
     * Register a readhead callback.
     *
     * @param readAheadCallback
     *          read ahead callback
     */
    synchronized void setReadAheadCallback(ReadAheadCallback readAheadCallback) {
        this.readAheadCallback = readAheadCallback;
        if (!isCacheFull()) {
            invokeReadAheadCallback();
        }
    }

    private void setLastException(IOException exc) {
        lastException.set(exc);
    }

    /**
     * Poll next record from the readahead queue.
     *
     * @return next record from readahead queue. null if no records available in the queue.
     * @throws IOException
     */
    LogRecordWithDLSN getNextReadAheadRecord() throws IOException {
        if (null != lastException.get()) {
            throw lastException.get();
        }

        LogRecordWithDLSN record = readAheadRecords.poll();

        if (null != record) {
            cacheBytes.addAndGet(-record.getPayload().length);
            if (!isCacheFull()) {
                invokeReadAheadCallback();
            }
        }

        return record;
    }

    /**
     * Check whether the reader becomes stall.
     *
     * @param idleReaderErrorThreshold
     *          idle reader error threshold
     * @param timeUnit
     *          time unit of the idle reader error threshold
     * @return true if the reader becomes stall, otherwise false.
     */
    boolean checkForReaderStall(int idleReaderErrorThreshold, TimeUnit timeUnit) {
        // If the read ahead cache has records that have not been consumed, then somehow
        // this is a stalled reader
        // Note: There is always the possibility that a new record just arrived at which point
        // The readAheadRecords is non empty but it has not had a chance to satisfy the promise; this
        // is unavoidable and acceptable.
        return !readAheadRecords.isEmpty() || (lastEntryProcessTime.elapsed(timeUnit) > idleReaderErrorThreshold);
    }

    /**
     * Set an ledger entry to readahead cache
     *
     * @param key
     *          read position of the entry
     * @param entry
     *          the ledger entry
     * @param reason
     *          the reason to add the entry to readahead (for logging)
     * @param envelopeEntries
     *          whether this entry an enveloped entries or not
     * @param startSequenceId
     *          the start sequence id
     */
    void set(LedgerReadPosition key,
             LedgerEntry entry,
             String reason,
             boolean envelopeEntries,
             long startSequenceId) {
        processNewLedgerEntry(key, entry, reason, envelopeEntries, startSequenceId);
        lastEntryProcessTime.reset().start();
        AsyncNotification n = notification;
        if (null != n) {
            n.notifyOnOperationComplete();
        }
    }

    boolean isCacheFull() {
        return getNumCachedRecords() >= maxCachedRecords;
    }

    /**
     * Return number cached records.
     *
     * @return number cached records.
     */
    int getNumCachedRecords() {
        return readAheadRecords.size();
    }

    /**
     * Return number cached bytes.
     *
     * @return number cached bytes.
     */
    long getNumCachedBytes() {
        return cacheBytes.get();
    }

    void setSuppressDeliveryLatency(boolean suppressed) {
        this.suppressDeliveryLatency = suppressed;
    }

    void setMinActiveDLSN(DLSN minActiveDLSN) {
        this.minActiveDLSN.set(minActiveDLSN);
    }

    /**
     * Process the new ledger entry and propagate the records into readahead queue.
     *
     * @param readPosition
     *          position of the ledger entry
     * @param ledgerEntry
     *          ledger entry
     * @param reason
     *          reason to add this ledger entry
     * @param envelopeEntries
     *          whether this entry is enveloped
     * @param startSequenceId
     *          the start sequence id of this log segment
     */
    private void processNewLedgerEntry(final LedgerReadPosition readPosition,
                                       final LedgerEntry ledgerEntry,
                                       final String reason,
                                       boolean envelopeEntries,
                                       long startSequenceId) {
        try {
            LogRecord.Reader reader = new LedgerEntryReader(streamName, readPosition.getLogSegmentSequenceNumber(),
                    ledgerEntry, envelopeEntries, startSequenceId, statsLogger);
            while(true) {
                LogRecordWithDLSN record = reader.readOp();

                if (null == record) {
                    break;
                }

                if (lastReadAheadDLSN.compareTo(record.getDlsn()) >= 0) {
                    LOG.error("Out of order reads last {} : curr {}", lastReadAheadDLSN, record.getDlsn());
                    throw new LogReadException("Out of order reads");
                }
                lastReadAheadDLSN = record.getDlsn();

                if (record.isControl()) {
                    continue;
                }

                if (minActiveDLSN.get().compareTo(record.getDlsn()) > 0) {
                    continue;
                }

                if (traceDeliveryLatencyEnabled && !suppressDeliveryLatency) {
                    long currentMs = System.currentTimeMillis();
                    long deliveryMs = currentMs - record.getTransactionId();
                    if (deliveryMs >= 0) {
                        readAheadDeliveryLatencyStat.registerSuccessfulEvent(deliveryMs);
                    } else {
                        negativeReadAheadDeliveryLatencyStat.registerSuccessfulEvent(-deliveryMs);
                    }
                    if (deliveryMs > deliveryLatencyWarnThresholdMillis) {
                        LOG.warn("Record {} for stream {} took long time to deliver : publish time = {}, available time = {}, delivery time = {}, reason = {}.",
                                 new Object[] { record.getDlsn(), streamName, record.getTransactionId(), currentMs, deliveryMs, reason });
                    }
                }
                readAheadRecords.add(record);
                cacheBytes.addAndGet(record.getPayload().length);
            }
        } catch (InvalidEnvelopedEntryException ieee) {
            alertStatsLogger.raise("Found invalid enveloped entry on stream {} : ", streamName, ieee);
            setLastException(ieee);
        } catch (IOException exc) {
            setLastException(exc);
        }
    }

    public void clear() {
        readAheadRecords.clear();
        cacheBytes.set(0L);
    }

    @Override
    public String toString() {
        return String.format("%s: Cache Bytes: %d, Num Cached Records: %d",
            streamName, cacheBytes.get(), getNumCachedRecords());
    }
}
