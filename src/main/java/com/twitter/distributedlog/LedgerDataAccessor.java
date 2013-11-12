package com.twitter.distributedlog;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class LedgerDataAccessor {
    static final Logger LOG = LoggerFactory.getLogger(LedgerDataAccessor.class);

    //
    private boolean readAheadEnabled = false;
    private int readAheadWaitTime = 100;
    private final LedgerHandleCache ledgerHandleCache;
    private Object notificationObject = null;
    private final Counter readAheadHits;
    private final Counter readAheadWaits;
    private final Counter readAheadMisses;
    private final ConcurrentHashMap<LedgerReadPosition, ReadAheadCacheValue> cache = new ConcurrentHashMap<LedgerReadPosition, ReadAheadCacheValue>();
    private HashSet<Long> cachedLedgerIds = new HashSet<Long>();
    private AtomicReference<LedgerReadPosition> lastRemovedKey = new AtomicReference<LedgerReadPosition>();

    LedgerDataAccessor(LedgerHandleCache ledgerHandleCache) {
        this(ledgerHandleCache, NullStatsLogger.INSTANCE);
    }

    LedgerDataAccessor(LedgerHandleCache ledgerHandleCache, StatsLogger statsLogger) {
        this.ledgerHandleCache = ledgerHandleCache;
        StatsLogger readAheadStatsLogger = statsLogger.scope("readahead");
        this.readAheadMisses = readAheadStatsLogger.getCounter("miss");
        this.readAheadHits = readAheadStatsLogger.getCounter("hit");
        this.readAheadWaits = readAheadStatsLogger.getCounter("wait");
    }

    public void setNotificationObject(Object notificationObject) {
        this.notificationObject = notificationObject;
    }

    public void setReadAheadEnabled(boolean enabled, int waitTime) {
        readAheadEnabled = enabled;
        readAheadWaitTime = waitTime;
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

    public LedgerEntry getWithWait(LedgerDescriptor ledgerDesc, LedgerReadPosition key)
        throws InterruptedException, BKException, IOException {
        if (readAheadEnabled) {
            ReadAheadCacheValue newValue = new ReadAheadCacheValue();
            ReadAheadCacheValue value = cache.putIfAbsent(key, newValue);
            if (null == value) {
                value = newValue;
            }
            if (null == value.getLedgerEntry() && null != notificationObject) {
                synchronized (notificationObject) {
                    notificationObject.notifyAll();
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
                LOG.debug("Read ahead cache miss {}", readAheadMisses.get());
            }
        }

        Enumeration<LedgerEntry> entries
            = ledgerHandleCache.readEntries(ledgerDesc, key.getEntryId(), key.getEntryId());
        assert (entries.hasMoreElements());
        LedgerEntry e = entries.nextElement();
        assert !entries.hasMoreElements();
        return e;
    }

    public void set(LedgerReadPosition key, LedgerEntry entry) {
        // Read Ahead is completing the read after the foreground reader
        // Don't add the entry to the cache
        LedgerReadPosition removeKey = lastRemovedKey.get();
        if ((null != removeKey) && (removeKey.getLedgerId() == key.getLedgerId()) &&
            (removeKey.getEntryId() >= key.getEntryId())) {
            return;
        }

        ReadAheadCacheValue newValue = new ReadAheadCacheValue();
        ReadAheadCacheValue value = cache.putIfAbsent(key, newValue);
        if (!cachedLedgerIds.contains(key.getLedgerId())) {
            cachedLedgerIds.add(key.getLedgerId());
        }
        if (null == value) {
            value = newValue;
        }
        synchronized (value) {
            value.setLedgerEntry(entry);
            value.notifyAll();
        }
    }

    public void remove(LedgerReadPosition key) {
        lastRemovedKey.set(key);
        removeInternal(key);
    }

    public void removeInternal(LedgerReadPosition key) {
        if ((null != cache.remove(key)) && (null != notificationObject)) {
            synchronized (notificationObject) {
                notificationObject.notifyAll();
            }
        }
    }

    public int getNumCacheEntries() {
        return cache.size();
    }

    public void removeLedger(long ledgerId) {
        if (!cachedLedgerIds.contains(ledgerId)) {
            LOG.debug("Ledger purge skipped");
            return;
        }
        LOG.debug("Ledger purged");

        cachedLedgerIds = new HashSet<Long>();

        Iterator<Map.Entry<LedgerReadPosition, ReadAheadCacheValue>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<LedgerReadPosition, ReadAheadCacheValue> entry = it.next();
            if (entry.getKey().getLedgerId() == ledgerId) {
                it.remove();
            } else {
                if (!cachedLedgerIds.contains(entry.getKey().getLedgerId())) {
                    cachedLedgerIds.add(entry.getKey().getLedgerId());
                }
            }
        }

        if (null != notificationObject) {
            synchronized (notificationObject) {
                notificationObject.notifyAll();
            }
        }
    }

    public void clear() {
        cache.clear();
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
