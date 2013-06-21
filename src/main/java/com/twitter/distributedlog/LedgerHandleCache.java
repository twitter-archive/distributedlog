package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerHandleCache {
    static final Logger LOG = LoggerFactory.getLogger(LedgerHandleCache.class);

    ConcurrentHashMap<LedgerDescriptor, RefCountedLedgerHandle> handlesMap =
        new ConcurrentHashMap<LedgerDescriptor, RefCountedLedgerHandle>();

    private final BookKeeper bkc;
    private final String digestpw;

    LedgerHandleCache(BookKeeper bkc, String digestpw) {
        this.bkc = bkc;
        this.digestpw = digestpw;
    }

    public synchronized LedgerDescriptor openLedger(long ledgerId, boolean fence) throws IOException {
        LedgerDescriptor ledgerDesc = new LedgerDescriptor(ledgerId, fence);
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);

        try {
            if (null == refhandle) {
                refhandle = new RefCountedLedgerHandle();
                if (!ledgerDesc.getFenced()) {
                    refhandle.handle = bkc.openLedgerNoRecovery(ledgerDesc.getLedgerId(),
                        BookKeeper.DigestType.CRC32,
                        digestpw.getBytes());
                } else {
                    refhandle.handle = bkc.openLedger(ledgerDesc.getLedgerId(),
                        BookKeeper.DigestType.CRC32,
                        digestpw.getBytes());
                }
                handlesMap.put(ledgerDesc, refhandle);
            }
        } catch (Exception e) {
            LOG.error("Ledger Handle Cache open ledger failed for partition " + ledgerDesc.getLedgerId(), e);
            throw new IOException("Could not open ledger for " + ledgerDesc.getLedgerId(), e);
        }
        refhandle.addRef();
        return ledgerDesc;
    }

    public synchronized void closeLedger(LedgerDescriptor ledgerDesc)
        throws InterruptedException, BKException, IOException {
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);

        if ((null != refhandle) && (refhandle.removeRef())) {
            refhandle.handle.close();
            handlesMap.remove(ledgerDesc);
        }
    }

    public long getLastAddConfirmed(LedgerDescriptor ledgerDesc) throws IOException {
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);

        if (null == refhandle) {
            throw new IOException("Accessing Ledger without opening");
        }

        return refhandle.handle.getLastAddConfirmed();
    }

    public synchronized void readLastConfirmed(LedgerDescriptor ledgerDesc)
        throws InterruptedException, BKException, IOException {
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);

        if (null == refhandle) {
            throw new IOException("Accessing Ledger without opening");
        }

        refhandle.handle.readLastConfirmed();
    }

    public synchronized Enumeration<LedgerEntry> readEntries(LedgerDescriptor ledgerDesc, long first, long last)
        throws InterruptedException, BKException, IOException {
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);

        if (null == refhandle) {
            throw new IOException("Accessing Ledger without opening");
        }

        return refhandle.handle.readEntries(first, last);
    }

    public synchronized long getLength(LedgerDescriptor ledgerDesc) throws IOException {
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);

        if (null == refhandle) {
            throw new IOException("Accessing Ledger without opening");
        }

        return refhandle.handle.getLength();
    }

    private class RefCountedLedgerHandle {
        public LedgerHandle handle;
        AtomicLong refcount = new AtomicLong(0);

        public void addRef() {
            refcount.incrementAndGet();
        }

        public boolean removeRef() {
            return (refcount.decrementAndGet() == 0);
        }

    }
}
