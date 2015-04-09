package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.DLInterruptedException;

public class LedgerDataAccessor {
    static final Logger LOG = LoggerFactory.getLogger(LedgerDataAccessor.class);

    //
    private boolean readAheadEnabled = false;
    private int readAheadWaitTime = 100;
    private int readAheadBatchSize = 1;
    private final LedgerHandleCache ledgerHandleCache;
    private final Counter readAheadHits;
    private final Counter readAheadWaits;
    private final Counter readAheadMisses;
    private final Counter readAheadAddHits;
    private final Counter readAheadAddMisses;
    private final OpStatsLogger readAheadDeliveryLatencyStat;
    private final OpStatsLogger negativeReadAheadDeliveryLatencyStat;
    private final StatsLogger statsLogger;
    private final AlertStatsLogger alertStatsLogger;
    private final Map<LedgerReadPosition, ReadAheadCacheValue> readAheadCache;
    private final LinkedBlockingQueue<LogRecordWithDLSN> readAheadRecords;
    private DLSN lastReadAheadDLSN = DLSN.InvalidDLSN;
    private AtomicReference<IOException> lastException = new AtomicReference<IOException>();
    private AtomicReference<LedgerReadPosition> lastRemovedKey = new AtomicReference<LedgerReadPosition>();
    private AsyncNotification notification;
    private AtomicLong cacheBytes = new AtomicLong(0);
    private BKLogPartitionReadHandler.ReadAheadCallback readAheadCallback = null;
    private final String streamName;
    private final boolean traceDeliveryLatencyEnabled;
    private volatile boolean suppressDeliveryLatency = true;
    private final long deliveryLatencyWarnThresholdMillis;
    private AtomicReference<DLSN> minActiveDLSN = new AtomicReference<DLSN>(DLSN.NonInclusiveLowerBound);
    private Stopwatch lastEntryProcessTime = Stopwatch.createStarted();


    LedgerDataAccessor(LedgerHandleCache ledgerHandleCache, String streamName,
                       StatsLogger statsLogger, AlertStatsLogger alertStatsLogger) {
        this(ledgerHandleCache, streamName, statsLogger, alertStatsLogger,
                null, false, DistributedLogConstants.LATENCY_WARN_THRESHOLD_IN_MILLIS);
    }

    LedgerDataAccessor(LedgerHandleCache ledgerHandleCache, String streamName, StatsLogger statsLogger, AlertStatsLogger alertStatsLogger,
                       AsyncNotification notification, boolean traceDeliveryLatencyEnabled, long deliveryLatencyWarnThresholdMillis) {
        this.ledgerHandleCache = ledgerHandleCache;
        this.streamName = streamName;
        StatsLogger readAheadStatsLogger = statsLogger.scope("readahead");
        this.statsLogger = readAheadStatsLogger;
        this.alertStatsLogger = alertStatsLogger;
        this.readAheadMisses = readAheadStatsLogger.getCounter("miss");
        this.readAheadHits = readAheadStatsLogger.getCounter("hit");
        this.readAheadWaits = readAheadStatsLogger.getCounter("wait");
        this.readAheadAddHits = readAheadStatsLogger.getCounter("add_hit");
        this.readAheadAddMisses = readAheadStatsLogger.getCounter("add_miss");
        this.readAheadDeliveryLatencyStat = readAheadStatsLogger.getOpStatsLogger("delivery_latency");
        this.negativeReadAheadDeliveryLatencyStat =
                readAheadStatsLogger.getOpStatsLogger("negative_delivery_latency");
        this.notification = notification;
        this.traceDeliveryLatencyEnabled = traceDeliveryLatencyEnabled;
        this.deliveryLatencyWarnThresholdMillis = deliveryLatencyWarnThresholdMillis;

        if (null == notification) {
            readAheadCache = Collections.synchronizedMap(new LinkedHashMap<LedgerReadPosition, ReadAheadCacheValue>(16, 0.75f, true));
            readAheadRecords = null;
        } else {
           readAheadCache = null;
           readAheadRecords = new LinkedBlockingQueue<LogRecordWithDLSN>();
        }
    }

    public synchronized void setReadAheadCallback(BKLogPartitionReadHandler.ReadAheadWorker readAheadCallback, long maxEntries) {
        this.readAheadCallback = readAheadCallback;
        if (getNumCacheEntries() < maxEntries) {
            invokeReadAheadCallback();
        }
    }

    public void setReadAheadEnabled(int waitTime, int batchSize) {
        readAheadEnabled = true;
        readAheadWaitTime = waitTime;
        readAheadBatchSize = batchSize;
    }

    private void setLastException(IOException exc) {
        lastException.set(exc);
    }

    public long getLastAddConfirmed(LedgerDescriptor ledgerDesc) throws IOException {
        return ledgerHandleCache.getLastAddConfirmed(ledgerDesc);
    }

    public long getLength(LedgerDescriptor ledgerDesc) throws IOException {
        return ledgerHandleCache.getLength(ledgerDesc);
    }

    public void closeLedger(LedgerDescriptor ledgerDesc)
        throws InterruptedException, BKException {
        ledgerHandleCache.closeLedger(ledgerDesc);
    }

    public LedgerEntry getEntry(LedgerDescriptor ledgerDesc, LedgerReadPosition key, boolean nonBlocking, boolean enableTrace)
        throws IOException {
        try {
            boolean shouldWaitForReadAhead = readAheadEnabled;

            // Read Ahead is completing the read after the foreground reader
            // Don't add the entry to the readAheadCache
            if (key.definitelyLessThanOrEqualTo(lastRemovedKey.get())) {
                shouldWaitForReadAhead = false;
            }

            if (shouldWaitForReadAhead) {
                if (nonBlocking) {
                    // If its a non blocking call then we simply return if the read ahead hasn't read
                    // up to this point, we will read the data on subsequent calls
                    ReadAheadCacheValue value;
                    synchronized(readAheadCache) {
                        value = readAheadCache.get(key);
                    }
                    if ((null == value) || (null == value.getLedgerEntry())) {
                        if (enableTrace) {
                            LOG.info("LedgerDataAccessor {}: Read-ahead miss for non blocking read {}",
                                toString(), key);
                        } else {
                            LOG.trace("LedgerDataAccessor {}: Read-ahead miss for non blocking read {}",
                                toString(), key);
                        }
                        return null;
                    } else {
                        readAheadHits.inc();
                        if (enableTrace) {
                            LOG.info("LedgerDataAccessor {}: Read-ahead readAheadCache hit for non blocking read {}",
                                toString(), key);
                        } else {
                            LOG.trace("LedgerDataAccessor {}: Read-ahead readAheadCache hit for non blocking read {}",
                                toString(), key);
                        }
                        return value.getLedgerEntry();
                    }
                } else {
                    ReadAheadCacheValue newValue = new ReadAheadCacheValue();
                    ReadAheadCacheValue value;
                    synchronized(readAheadCache) {
                        value = readAheadCache.get(key);
                        if (null == value) {
                            readAheadCache.put(key, newValue);
                            value = newValue;
                        }
                    }

                    synchronized (value) {
                        if (null == value.getLedgerEntry()) {
                            value.wait(readAheadWaitTime);
                            if (enableTrace) {
                                LOG.info("LedgerDataAccessor {}: Read-ahead wait for {}",
                                    toString(), key);
                            }
                            readAheadWaits.inc();
                        }
                    }
                    if (null != value.getLedgerEntry()) {
                        readAheadHits.inc();
                        if (enableTrace) {
                            LOG.info("LedgerDataAccessor {}: Read-ahead cache hit for {}",
                                toString(), key);
                        }
                        return value.getLedgerEntry();
                    }
                }
            }

            readAheadMisses.inc();
            if (enableTrace) {
                LOG.info("LedgerDataAccessor {}: Read-ahead cache miss for {}",
                    toString(), key);
            }
            if ((readAheadMisses.get() % 1000) == 0) {
                LOG.debug("Read ahead readAheadCache miss {}", readAheadMisses.get());
            }

            Enumeration<LedgerEntry> entries
                = ledgerHandleCache.readEntries(ledgerDesc, key.getEntryId(), key.getEntryId());
            assert (entries.hasMoreElements());
            LedgerEntry e = entries.nextElement();
            assert !entries.hasMoreElements();
            return e;
        } catch (BKException bke) {
            if ((bke.getCode() == BKException.Code.NoSuchLedgerExistsException) ||
                (ledgerDesc.isFenced() &&
                    (bke.getCode() == BKException.Code.NoSuchEntryException))) {
                String errorMessage = String.format("Ledger %d Not Found or Entry %d Not Found In Closed Ledger %d",
                                                    ledgerDesc.getLedgerId(), key.getEntryId(), ledgerDesc.getLedgerId());
                throw new LogReadException(errorMessage);
            }
            LOG.info("Reached the end of the stream");
            LOG.debug("Encountered exception at end of stream", bke);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted reading entries from bookkeeper", ie);
        }

        return null;
    }

    public LogRecordWithDLSN getNextReadAheadRecord() throws IOException {
        if (null != lastException.get()) {
            throw lastException.get();
        }

        LogRecordWithDLSN record = readAheadRecords.poll();

        if (null != record) {
            invokeReadAheadCallback();
        }

        return record;
    }

    public boolean checkForReaderStall(int idleReaderErrorThreshold, TimeUnit timeUnit) {
        // If the read ahead cache has records that have not been consumed, then somehow
        // this is a stalled reader
        // Note: There is always the possibility that a new record just arrived at which point
        // The readAheadRecords is non empty but it has not had a chance to satisfy the promise; this
        // is unavoidable and acceptable.
        return !readAheadRecords.isEmpty() || (lastEntryProcessTime.elapsed(timeUnit) > idleReaderErrorThreshold);
    }

    public void set(LedgerReadPosition key, LedgerEntry entry, String reason, boolean envelopeEntries) {
        LOG.trace("Set Called");
        if (null != readAheadCache) {
            setReadAheadCacheEntry(key, entry);
        } else {
            LOG.trace("Calling process");
            processNewLedgerEntry(key, entry, reason, envelopeEntries);
            lastEntryProcessTime.reset().start();
            AsyncNotification n = notification;
            if (null != n) {
                n.notifyOnOperationComplete();
            }
        }
    }

    private void setReadAheadCacheEntry(LedgerReadPosition key, LedgerEntry entry) {
        // Read Ahead is completing the read after the foreground reader
        // Don't add the entry to the readAheadCache
        if (key.definitelyLessThanOrEqualTo(lastRemovedKey.get())) {
            return;
        }

        ReadAheadCacheValue newValue = new ReadAheadCacheValue();

        ReadAheadCacheValue value;
        synchronized(readAheadCache) {
            value = readAheadCache.get(key);
            if (null == value) {
                readAheadCache.put(key, newValue);
                value = newValue;
                readAheadAddMisses.inc();
            } else {
                readAheadAddHits.inc();
            }
        }

        synchronized (value) {
            if (null == value.getLedgerEntry()) {
                cacheBytes.addAndGet(entry.getLength());
            }

            value.setLedgerEntry(entry);
            value.notifyAll();
        }

        AsyncNotification n = notification;
        if (null != n) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Notification of new entry Stream: {}, LedgerReadPoistion: {}", streamName, key);
            }
            n.notifyOnOperationComplete();
        }
    }

    public void remove(LedgerReadPosition key) {
        // As long as the lastRemovedKey value is not definitely greater than the key, advance the
        // lastRemovedKey
        // **Note**
        // Only the foreground reader should call this method and repositioning should never happen
        // in the previous ledger as once the ledger has been advanced at least one entry from the
        // new ledger has already been read. So if ledger ids are different its safe to assume that
        // we have been positioned on the next ledger.
        if (!key.definitelyLessThanOrEqualTo(lastRemovedKey.get())) {
            lastRemovedKey.set(key);
            purgeReadAheadCache(lastRemovedKey.get());
        }
        removeInternal(key);
    }

    public void removeInternal(LedgerReadPosition key) {
        ReadAheadCacheValue value;
        synchronized(readAheadCache) {
            value = readAheadCache.remove(key);
        }
        if (null != value) {
            // If this is a synchronization place holder that was added by
            // getWithWait then the entry may be null
            if (null != value.getLedgerEntry()) {
                cacheBytes.addAndGet(-value.getLedgerEntry().getLength());
            }
            invokeReadAheadCallback();
        }
    }

    public int getNumCacheEntries() {
        if (null != readAheadCache) {
            synchronized(readAheadCache) {
                return readAheadCache.size();
            }
        } else {
            return readAheadRecords.size();
        }
    }

    public void purgeReadAheadCache(LedgerReadPosition threshold) {
        boolean removedEntry = false;

        // Limit the traversal to batch size so that an individual call is
        // not stalled for long
        int count = Math.max(readAheadBatchSize, 1);

        synchronized(readAheadCache) {
            Iterator<Map.Entry<LedgerReadPosition, ReadAheadCacheValue>> it = readAheadCache.entrySet().iterator();
            while (it.hasNext() && (count > 0)) {
                Map.Entry<LedgerReadPosition, ReadAheadCacheValue> entry = it.next();
                if (entry.getKey().definitelyLessThanOrEqualTo(threshold)) {
                    it.remove();
                    removedEntry = true;
                } else {
                    break;
                }
                count--;
            }
        }

        if (removedEntry) {
            invokeReadAheadCallback();
        }
    }

    void setSuppressDeliveryLatency(boolean suppressed) {
        this.suppressDeliveryLatency = suppressed;
    }

    void setMinActiveDLSN(DLSN minActiveDLSN) {
        this.minActiveDLSN.set(minActiveDLSN);
    }

    void processNewLedgerEntry(final LedgerReadPosition readPosition, final LedgerEntry ledgerEntry,
                               final String reason, boolean envelopeEntries) {
        try {
            LogRecord.Reader reader = new LedgerEntryReader(streamName, readPosition.getLedgerSequenceNumber(),
                    ledgerEntry, envelopeEntries, statsLogger);
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

            }
        } catch (InvalidEnvelopedEntryException ieee) {
            alertStatsLogger.raise("Found invalid enveloped entry on stream {} : ", streamName, ieee);
            setLastException(ieee);
        } catch (IOException exc) {
            setLastException(exc);
        }
    }

    private synchronized void invokeReadAheadCallback() {
        if (null != readAheadCallback) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Cache has space, schedule the read ahead");
            }
            readAheadCallback.resumeReadAhead();
            readAheadCallback = null;
        }
    }

    public void clear() {
        if (null != readAheadCache) {
            synchronized(readAheadCache) {
                readAheadCache.clear();
            }
        } else {
            readAheadRecords.clear();
        }
    }

    static private class ReadAheadCacheValue {
        LedgerEntry entry = null;

        public ReadAheadCacheValue() {
            entry = null;
        }

        public LedgerEntry getLedgerEntry() {
            return entry;
        }

        public void setLedgerEntry(LedgerEntry value) {
            entry = value;
        }
    }

    @Override
    public String toString() {
        return String.format("%s: Last Removed Key: %s, Cache Bytes: %d, Num Cached Entries: %d",
            streamName, lastRemovedKey.get(), cacheBytes.get(), getNumCacheEntries());
    }
}
