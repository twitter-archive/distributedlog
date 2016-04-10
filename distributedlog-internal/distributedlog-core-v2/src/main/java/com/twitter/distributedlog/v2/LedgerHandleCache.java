package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.BookKeeperClient;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Charsets.UTF_8;

public class LedgerHandleCache {
    static final Logger LOG = LoggerFactory.getLogger(LedgerHandleCache.class);

    ConcurrentHashMap<LedgerDescriptor, RefCountedLedgerHandle> handlesMap =
        new ConcurrentHashMap<LedgerDescriptor, RefCountedLedgerHandle>();

    private final BookKeeperClient bkc;
    private final String digestpw;

    LedgerHandleCache(BookKeeperClient bkc, String digestpw) {
        this.bkc = bkc;
        this.digestpw = digestpw;
    }

    /**
     * Open the given ledger <i>ledgerDesc</i>.
     *
     * @param ledgerDesc
     *          ledger description
     * @param callback
     *          open callback.
     * @param ctx
     *          callback context
     */
    private void asyncOpenLedger(LedgerDescriptor ledgerDesc, AsyncCallback.OpenCallback callback, Object ctx) {
        try {
            if (!ledgerDesc.isFenced()) {
                bkc.get().asyncOpenLedgerNoRecovery(ledgerDesc.getLedgerId(),
                        BookKeeper.DigestType.CRC32, digestpw.getBytes(UTF_8), callback, ctx);
            } else {
                bkc.get().asyncOpenLedger(ledgerDesc.getLedgerId(),
                        BookKeeper.DigestType.CRC32, digestpw.getBytes(UTF_8), callback, ctx);
            }
        } catch (IOException ace) {
            // :) when we can't get bkc, it means bookie handle not available
            callback.openComplete(BKException.Code.BookieHandleNotAvailableException, null, ctx);
        }
    }

    public void asyncOpenLedger(LogSegmentLedgerMetadata metadata, boolean fence,
                                final BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor> callback) {
        final LedgerDescriptor ledgerDesc = new LedgerDescriptor(metadata.getLedgerId(), metadata.getLedgerSequenceNumber(), fence);
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);
        if (null == refhandle) {
            asyncOpenLedger(ledgerDesc, new AsyncCallback.OpenCallback() {
                @Override
                public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                    if (BKException.Code.OK != rc) {
                        callback.operationComplete(rc, null);
                        return;
                    }
                    RefCountedLedgerHandle newRefHandle = new RefCountedLedgerHandle(lh);
                    RefCountedLedgerHandle oldRefHandle = handlesMap.putIfAbsent(ledgerDesc, newRefHandle);
                    if (null != oldRefHandle) {
                        oldRefHandle.addRef();
                        if (newRefHandle.removeRef()) {
                            newRefHandle.handle.asyncClose(new AsyncCallback.CloseCallback() {
                                @Override
                                public void closeComplete(int i, LedgerHandle ledgerHandle, Object o) {
                                    // No action necessary
                                }
                            }, null);
                        }
                    }
                    callback.operationComplete(BKException.Code.OK, ledgerDesc);
                }
            }, null);
        } else {
            refhandle.addRef();
            callback.operationComplete(BKException.Code.OK, ledgerDesc);
        }
    }

    public LedgerDescriptor openLedger(LogSegmentLedgerMetadata metadata, boolean fence) throws IOException, BKException {
        final SyncObject<LedgerDescriptor> syncObject = new SyncObject<LedgerDescriptor>();
        syncObject.inc();
        asyncOpenLedger(metadata, fence, new BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>() {
            @Override
            public void operationComplete(int rc, LedgerDescriptor ledgerDescriptor) {
                syncObject.setrc(rc);
                syncObject.setValue(ledgerDescriptor);
                syncObject.dec();
            }
        });
        try {
            syncObject.block(0);
        } catch (InterruptedException e) {
            LOG.error("Interrupted when opening ledger {} : ", metadata.getLedgerId(), e);
            throw new IOException("Could not open ledger for " + metadata.getLedgerId(), e);
        }
        if (BKException.Code.OK == syncObject.getrc()) {
            return syncObject.getValue();
        }
        throw BKException.create(syncObject.getrc());
    }

    private RefCountedLedgerHandle getLedgerHandle(LedgerDescriptor ledgerDescriptor) {
        return null == ledgerDescriptor ? null : handlesMap.get(ledgerDescriptor);
    }

    public void closeLedger(LedgerDescriptor ledgerDesc)
        throws InterruptedException, BKException {
        RefCountedLedgerHandle refhandle = getLedgerHandle(ledgerDesc);

        if ((null != refhandle) && (refhandle.removeRef())) {
            refhandle = handlesMap.remove(ledgerDesc);
            if (refhandle.getRefCount() > 0) {
                // In the rare race condition that a ref count was added immediately
                // after the close de-refed it and the remove was called

                // Try to put the handle back in the map
                handlesMap.putIfAbsent(ledgerDesc, refhandle);

                // ReadOnlyLedgerHandles don't have much overhead, so lets just leave
                // the handle open even if it had already been replaced

            } else {
                refhandle.handle.close();
            }
        }
    }

    /**
     * Whether a ledger is closed or not.
     *
     * @param ledgerDesc
     *          ledger descriptor.
     * @return true if a ledger is closed, otherwise false.
     * @throws BKException
     */
    public boolean isLedgerHandleClosed(LedgerDescriptor ledgerDesc) throws IOException {
        RefCountedLedgerHandle refhandle = getLedgerHandle(ledgerDesc);

        if (null == refhandle) {
            throw new IOException("Accessing Ledger without opening");
        }

        return refhandle.handle.isClosed();
    }

    public long getLastAddConfirmed(LedgerDescriptor ledgerDesc) throws IOException {
        RefCountedLedgerHandle refhandle = getLedgerHandle(ledgerDesc);

        if (null == refhandle) {
            throw new IOException("Accessing Ledger without opening");
        }

        return refhandle.handle.getLastAddConfirmed();
    }

    public void asyncTryReadLastConfirmed(LedgerDescriptor ledgerDesc,
                                          AsyncCallback.ReadLastConfirmedCallback callback, Object ctx) {
        RefCountedLedgerHandle refHandle = handlesMap.get(ledgerDesc);
        if (null == refHandle) {
            callback.readLastConfirmedComplete(BKException.Code.NoSuchLedgerExistsException, -1, ctx);
            return;
        }
        refHandle.handle.asyncTryReadLastConfirmed(callback, ctx);
    }

    public long tryReadLastConfirmed(LedgerDescriptor ledgerDesc) throws BKException, InterruptedException {
        final SyncObject<Long> syncObject = new SyncObject<Long>();
        syncObject.inc();
        asyncTryReadLastConfirmed(ledgerDesc, new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastAddConfirmed, Object ctx) {
                syncObject.setrc(rc);
                syncObject.setValue(lastAddConfirmed);
                syncObject.dec();
            }
        }, null);
        syncObject.block(0);
        if (BKException.Code.OK == syncObject.getrc()) {
            return syncObject.getValue();
        }
        throw BKException.create(syncObject.getrc());
    }

    public void asyncReadLastConfirmed(LedgerDescriptor ledgerDesc,
                                       AsyncCallback.ReadLastConfirmedCallback callback, Object ctx) {
        RefCountedLedgerHandle refHandle = handlesMap.get(ledgerDesc);
        if (null == refHandle) {
            callback.readLastConfirmedComplete(BKException.Code.NoSuchLedgerExistsException, -1, ctx);
            return;
        }
        refHandle.handle.asyncReadLastConfirmed(callback, ctx);
    }

    public void readLastConfirmed(LedgerDescriptor ledgerDesc)
        throws InterruptedException, BKException, IOException {
        final SyncObject<Long> syncObject = new SyncObject<Long>();
        syncObject.inc();
        asyncReadLastConfirmed(ledgerDesc, new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastAddConfirmed, Object context) {
                syncObject.setrc(rc);
                syncObject.setValue(lastAddConfirmed);
                syncObject.dec();
            }
        }, null);
        syncObject.block(0);
        if (BKException.Code.OK == syncObject.getrc()) {
            return;
        }
        throw BKException.create(syncObject.getrc());
    }

    public void asyncReadEntries(LedgerDescriptor ledgerDesc, long first, long last,
                                 AsyncCallback.ReadCallback callback, Object ctx) {
        RefCountedLedgerHandle refHandle = handlesMap.get(ledgerDesc);
        if (null == refHandle) {
            callback.readComplete(BKException.Code.NoSuchLedgerExistsException, null, null, ctx);
            return;
        }
        refHandle.handle.asyncReadEntries(first, last, callback, ctx);
    }

    public synchronized Enumeration<LedgerEntry> readEntries(LedgerDescriptor ledgerDesc, long first, long last)
        throws InterruptedException, BKException, IOException {
        final SyncObject<Enumeration<LedgerEntry>> syncObject =
                new SyncObject<Enumeration<LedgerEntry>>();
        syncObject.inc();
        asyncReadEntries(ledgerDesc, first, last, new AsyncCallback.ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle ledgerHandle, Enumeration<LedgerEntry> entries, Object ctx) {
                syncObject.setrc(rc);
                syncObject.setValue(entries);
                syncObject.dec();
            }
        }, null);
        syncObject.block(0);
        if (BKException.Code.OK == syncObject.getrc()) {
            return syncObject.getValue();
        }
        throw BKException.create(syncObject.getrc());
    }

    public void asyncReadLastConfirmedAndEntry(LedgerDescriptor ledgerDesc,
                                               long entryId,
                                               long timeOutInMillis,
                                               boolean parallel,
                                               AsyncCallback.ReadLastConfirmedAndEntryCallback callback,
                                               Object ctx) {
        RefCountedLedgerHandle refHandle = handlesMap.get(ledgerDesc);
        if (null == refHandle) {
            callback.readLastConfirmedAndEntryComplete(BKException.Code.NoSuchLedgerExistsException, -1, null, ctx);
            return;
        }
        refHandle.handle.asyncReadLastConfirmedAndEntry(entryId, timeOutInMillis, parallel, callback, ctx);
    }

    public long getLength(LedgerDescriptor ledgerDesc) throws IOException {
        RefCountedLedgerHandle refhandle = getLedgerHandle(ledgerDesc);

        if (null == refhandle) {
            throw new IOException("Accessing Ledger without opening");
        }

        return refhandle.handle.getLength();
    }

    static private class RefCountedLedgerHandle {
        public final LedgerHandle handle;
        final AtomicLong refcount = new AtomicLong(0);

        RefCountedLedgerHandle(LedgerHandle lh) {
            this.handle = lh;
            addRef();
        }

        long getRefCount() {
            return refcount.get();
        }

        public void addRef() {
            refcount.incrementAndGet();
        }

        public boolean removeRef() {
            return (refcount.decrementAndGet() == 0);
        }

    }
}