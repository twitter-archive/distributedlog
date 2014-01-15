package com.twitter.distributedlog;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private final Map<LedgerReadPosition, ReadAheadCacheValue> readAheadCache = Collections.synchronizedMap(new LinkedHashMap<LedgerReadPosition, ReadAheadCacheValue>(16, 0.75f, true));
    private AtomicReference<LedgerReadPosition> lastRemovedKey = new AtomicReference<LedgerReadPosition>();
    private final AsyncNotification notification;
    private AtomicLong cacheBytes = new AtomicLong(0);
    private BKLogPartitionReadHandler.ReadAheadCallback readAheadCallback = null;

    LedgerDataAccessor(LedgerHandleCache ledgerHandleCache, StatsLogger statsLogger) {
        this(ledgerHandleCache, statsLogger, null);
    }

    LedgerDataAccessor(LedgerHandleCache ledgerHandleCache, StatsLogger statsLogger, AsyncNotification notification) {
        this.ledgerHandleCache = ledgerHandleCache;
        StatsLogger readAheadStatsLogger = statsLogger.scope("readahead");
        this.readAheadMisses = readAheadStatsLogger.getCounter("miss");
        this.readAheadHits = readAheadStatsLogger.getCounter("hit");
        this.readAheadWaits = readAheadStatsLogger.getCounter("wait");
        this.notification = notification;
        //Number of entries in the readAheadCache
        readAheadStatsLogger.registerGauge("num_cache_entries", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return getNumCacheEntries();
            }
        });
        readAheadStatsLogger.registerGauge("num_cache_bytes", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return cacheBytes.get();
            }
        });
    }

    public synchronized void setReadAheadCallback(BKLogPartitionReadHandler.ReadAheadWorker readAheadCallback, long maxEntries) {
        this.readAheadCallback = readAheadCallback;
        if (getNumCacheEntries() < maxEntries) {
            invokeReadAheadCallback();
        }
    }

    public void setReadAheadEnabled(boolean enabled, int waitTime, int batchSize) {
        readAheadEnabled = enabled;
        readAheadWaitTime = waitTime;
        readAheadBatchSize = batchSize;
    }

    public long getLastAddConfirmed(LedgerDescriptor ledgerDesc) throws IOException {
        return ledgerHandleCache.getLastAddConfirmed(ledgerDesc);
    }

    public long getLength(LedgerDescriptor ledgerDesc) throws IOException {
        return ledgerHandleCache.getLength(ledgerDesc);
    }

    public void closeLedger(LedgerDescriptor ledgerDesc)
        throws InterruptedException, BKException, IOException {
        ledgerHandleCache.closeLedger(ledgerDesc);
    }

    public LedgerEntry getEntry(LedgerDescriptor ledgerDesc, LedgerReadPosition key, boolean nonBlocking)
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
                        return null;
                    } else {
                        LOG.trace("Read-ahead readAheadCache hit for non blocking read");
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
                            readAheadWaits.inc();
                        }
                    }
                    if (null != value.getLedgerEntry()) {
                        readAheadHits.inc();
                        return value.getLedgerEntry();
                    }

                    readAheadMisses.inc();
                    if ((readAheadMisses.get() % 1000) == 0) {
                        LOG.debug("Read ahead readAheadCache miss {}", readAheadMisses.get());
                    }
                }
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
                throw new LogReadException("Ledger or Entry Not Found In A Closed Ledger");
            }
            LOG.info("Reached the end of the stream");
            LOG.debug("Encountered exception at end of stream", bke);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted reading entries from bookkeeper", ie);
        }

        return null;
    }

    public void set(LedgerReadPosition key, LedgerEntry entry) {
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
            }
        }

        synchronized (value) {
            if (null == value.getLedgerEntry()) {
                cacheBytes.addAndGet(entry.getLength());
            }

            value.setLedgerEntry(entry);
            value.notifyAll();
        }
        if (null != notification) {
            LOG.debug("Notification of new entry");
            notification.notifyOnOperationComplete();
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
        synchronized(readAheadCache) {
            return readAheadCache.size();
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
        synchronized(readAheadCache) {
            readAheadCache.clear();
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
}
