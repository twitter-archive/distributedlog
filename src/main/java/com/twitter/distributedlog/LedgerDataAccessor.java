package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerDataAccessor {
    static final Logger LOG = LoggerFactory.getLogger(LedgerDataAccessor.class);

    private boolean readAheadEnabled = false;
    private int readAheadWaitTime = 100;
    private final LedgerHandleCache ledgerHandleCache;
    private Long notificationObject = null;
    private AtomicLong readAheadMisses = new AtomicLong(0);
    ConcurrentHashMap<LedgerReadPosition, ReadAheadCacheValue> cache = new ConcurrentHashMap<LedgerReadPosition, ReadAheadCacheValue>();
    HashSet<Long> cachedLedgerIds = new HashSet<Long>();

    LedgerDataAccessor(LedgerHandleCache ledgerHandleCache) {
        this.ledgerHandleCache = ledgerHandleCache;
    }

    public void setNotificationObject(Long notificationObject) {
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
                }
            }
            if (null != value.getLedgerEntry()) {
                return value.getLedgerEntry();
            }

            if ((readAheadMisses.incrementAndGet() % 1000) == 0) {
                LOG.info("Read ahead cache miss {}", readAheadMisses.get());
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
        ReadAheadCacheValue newValue = new ReadAheadCacheValue();
        ReadAheadCacheValue value = cache.putIfAbsent(key, newValue);
        if (!cachedLedgerIds.contains(key.getLedgerId())) {
            cachedLedgerIds.add(key.getLedgerId());
        }
        if (null == value) {
            value = newValue;
        }
        synchronized(value) {
            value.setLedgerEntry(entry);
            value.notifyAll();
        }
    }

    public void remove(LedgerReadPosition key) {
        if (null != cache.get(key)) {
            cache.remove(key);
            if (null!= notificationObject) {
                synchronized(notificationObject) {
                    notificationObject.notifyAll();
                }
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
        while(it.hasNext()) {
            Map.Entry<LedgerReadPosition, ReadAheadCacheValue> entry = it.next();
            if(entry.getKey().getLedgerId() == ledgerId) {
                it.remove();
            } else {
                if (!cachedLedgerIds.contains(entry.getKey().getLedgerId())) {
                    cachedLedgerIds.add(entry.getKey().getLedgerId());
                }
            }
        }

        if (null!= notificationObject) {
            synchronized(notificationObject) {
                notificationObject.notifyAll();
            }
        }
    }

    public void clear() {
        cache.clear();
    }

    private class ReadAheadCacheValue {
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
