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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A central place on managing open ledgers.
 */
public class LedgerHandleCache {
    static final Logger LOG = LoggerFactory.getLogger(LedgerHandleCache.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private BookKeeperClient bkc;
        private String digestpw;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;

        private Builder() {}

        public Builder bkc(BookKeeperClient bkc) {
            this.bkc = bkc;
            return this;
        }

        public Builder conf(DistributedLogConfiguration conf) {
            this.digestpw = conf.getBKDigestPW();
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public LedgerHandleCache build() {
            Preconditions.checkNotNull(bkc, "No bookkeeper client is provided");
            Preconditions.checkNotNull(digestpw, "No bookkeeper digest password is provided");
            Preconditions.checkNotNull(statsLogger, "No stats logger is provided");
            return new LedgerHandleCache(bkc, digestpw, statsLogger);
        }
    }

    final ConcurrentHashMap<LedgerDescriptor, RefCountedLedgerHandle> handlesMap =
        new ConcurrentHashMap<LedgerDescriptor, RefCountedLedgerHandle>();

    private final BookKeeperClient bkc;
    private final String digestpw;

    private final OpStatsLogger openStats;
    private final OpStatsLogger openNoRecoveryStats;

    private LedgerHandleCache(BookKeeperClient bkc, String digestpw, StatsLogger statsLogger) {
        this.bkc = bkc;
        this.digestpw = digestpw;
        // Stats
        openStats = statsLogger.getOpStatsLogger("open_ledger");
        openNoRecoveryStats = statsLogger.getOpStatsLogger("open_ledger_no_recovery");
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

    /**
     * Open the log segment.
     *
     * @param metadata
     *          the log segment metadata
     * @param fence
     *          whether to fence the log segment during open
     * @return a future presenting the open result.
     */
    public Future<LedgerDescriptor> asyncOpenLedger(LogSegmentMetadata metadata, boolean fence) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final OpStatsLogger openStatsLogger = fence ? openStats : openNoRecoveryStats;
        final Promise<LedgerDescriptor> promise = new Promise<LedgerDescriptor>();
        final LedgerDescriptor ledgerDesc = new LedgerDescriptor(metadata.getLedgerId(), metadata.getLogSegmentSequenceNumber(), fence);
        RefCountedLedgerHandle refhandle = handlesMap.get(ledgerDesc);
        if (null == refhandle) {
            asyncOpenLedger(ledgerDesc, new AsyncCallback.OpenCallback() {
                @Override
                public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                    if (BKException.Code.OK != rc) {
                        promise.setException(BKException.create(rc));
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
                    promise.setValue(ledgerDesc);
                }
            }, null);
        } else {
            refhandle.addRef();
            promise.setValue(ledgerDesc);
        }
        return promise.addEventListener(new FutureEventListener<LedgerDescriptor>() {
            @Override
            public void onSuccess(LedgerDescriptor value) {
                openStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            }

            @Override
            public void onFailure(Throwable cause) {
                openStatsLogger.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            }
        });
    }

    /**
     * Open a ledger synchronously.
     *
     * @param metadata
     *          log segment metadata
     * @param fence
     *          whether to fence the log segment during open
     * @return ledger descriptor
     * @throws BKException
     */
    public LedgerDescriptor openLedger(LogSegmentMetadata metadata, boolean fence) throws BKException {
        return FutureUtils.bkResult(asyncOpenLedger(metadata, fence));
    }

    private RefCountedLedgerHandle getLedgerHandle(LedgerDescriptor ledgerDescriptor) {
        return null == ledgerDescriptor ? null : handlesMap.get(ledgerDescriptor);
    }

    /**
     * Close the ledger asynchronously.
     *
     * @param ledgerDesc
     *          ledger descriptor.
     * @return future presenting the closing result.
     */
    public Future<Void> asyncCloseLedger(LedgerDescriptor ledgerDesc) {
        final Promise<Void> promise = new Promise<Void>();

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
                promise.setValue(null);
            } else {
                refhandle.handle.asyncClose(new AsyncCallback.CloseCallback() {
                    @Override
                    public void closeComplete(int rc, LedgerHandle ledgerHandle, Object ctx) {
                        if (BKException.Code.OK == rc) {
                            promise.setValue(null);
                        } else {
                            promise.setException(BKException.create(rc));
                        }
                    }
                }, null);
            }
        } else {
            promise.setValue(null);
        }
        return promise;
    }

    /**
     * Close the ledger synchronously.
     *
     * @param ledgerDesc
     *          ledger descriptor.
     * @throws BKException
     */
    public void closeLedger(LedgerDescriptor ledgerDesc) throws BKException {
        FutureUtils.bkResult(asyncCloseLedger(ledgerDesc));
    }

    /**
     * Get the last add confirmed of <code>ledgerDesc</code>.
     *
     * @param ledgerDesc
     *          ledger descriptor.
     * @return last add confirmed of <code>ledgerDesc</code>
     * @throws BKException
     */
    public long getLastAddConfirmed(LedgerDescriptor ledgerDesc) throws BKException {
        RefCountedLedgerHandle refhandle = getLedgerHandle(ledgerDesc);

        if (null == refhandle) {
            LOG.error("Accessing ledger {} without opening.", ledgerDesc);
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }

        return refhandle.handle.getLastAddConfirmed();
    }

    /**
     * Whether a ledger is closed or not.
     *
     * @param ledgerDesc
     *          ledger descriptor.
     * @return true if a ledger is closed, otherwise false.
     * @throws BKException
     */
    public boolean isLedgerHandleClosed(LedgerDescriptor ledgerDesc) throws BKException {
        RefCountedLedgerHandle refhandle = getLedgerHandle(ledgerDesc);

        if (null == refhandle) {
            LOG.error("Accessing ledger {} without opening.", ledgerDesc);
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }

        return refhandle.handle.isClosed();
    }

    /**
     * Async try read last confirmed.
     *
     * @param ledgerDesc
     *          ledger descriptor
     * @return future presenting read last confirmed result.
     */
    public Future<Long> asyncTryReadLastConfirmed(LedgerDescriptor ledgerDesc) {
        RefCountedLedgerHandle refHandle = handlesMap.get(ledgerDesc);
        if (null == refHandle) {
            LOG.error("Accessing ledger {} without opening.", ledgerDesc);
            return Future.exception(BKException.create(BKException.Code.UnexpectedConditionException));
        }
        final Promise<Long> promise = new Promise<Long>();
        refHandle.handle.asyncTryReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastAddConfirmed, Object ctx) {
                if (BKException.Code.OK == rc) {
                    promise.setValue(lastAddConfirmed);
                } else {
                    promise.setException(BKException.create(rc));
                }
            }
        }, null);
        return promise;
    }

    /**
     * Try read last confirmed.
     *
     * @param ledgerDesc
     *          ledger descriptor
     * @return last confirmed
     * @throws BKException
     */
    public long tryReadLastConfirmed(LedgerDescriptor ledgerDesc) throws BKException {
        return FutureUtils.bkResult(asyncTryReadLastConfirmed(ledgerDesc));
    }

    /**
     * Async read last confirmed and entry
     *
     * @param ledgerDesc
     *          ledger descriptor
     * @param entryId
     *          entry id to read
     * @param timeOutInMillis
     *          time out if no newer entry available
     * @param parallel
     *          whether to read from replicas in parallel
     */
    public Future<Pair<Long, LedgerEntry>> asyncReadLastConfirmedAndEntry(
            LedgerDescriptor ledgerDesc,
            long entryId,
            long timeOutInMillis,
            boolean parallel) {
        RefCountedLedgerHandle refHandle = handlesMap.get(ledgerDesc);
        if (null == refHandle) {
            LOG.error("Accessing ledger {} without opening.", ledgerDesc);
            return Future.exception(BKException.create(BKException.Code.UnexpectedConditionException));
        }
        final Promise<Pair<Long, LedgerEntry>> promise = new Promise<Pair<Long, LedgerEntry>>();
        refHandle.handle.asyncReadLastConfirmedAndEntry(entryId, timeOutInMillis, parallel,
                new AsyncCallback.ReadLastConfirmedAndEntryCallback() {
                    @Override
                    public void readLastConfirmedAndEntryComplete(int rc, long lac, LedgerEntry ledgerEntry, Object ctx) {
                        if (BKException.Code.OK == rc) {
                            promise.setValue(Pair.of(lac, ledgerEntry));
                        } else {
                            promise.setException(BKException.create(rc));
                        }
                    }
                }, null);
        return promise;
    }

    /**
     * Async Read Entries
     *
     * @param ledgerDesc
     *          ledger descriptor
     * @param first
     *          first entry
     * @param last
     *          second entry
     */
    public Future<Enumeration<LedgerEntry>> asyncReadEntries(
            LedgerDescriptor ledgerDesc, long first, long last) {
        RefCountedLedgerHandle refHandle = handlesMap.get(ledgerDesc);
        if (null == refHandle) {
            LOG.error("Accessing ledger {} without opening.", ledgerDesc);
            return Future.exception(BKException.create(BKException.Code.UnexpectedConditionException));
        }
        final Promise<Enumeration<LedgerEntry>> promise = new Promise<Enumeration<LedgerEntry>>();
        refHandle.handle.asyncReadEntries(first, last, new AsyncCallback.ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx) {
                if (BKException.Code.OK == rc) {
                    promise.setValue(entries);
                } else {
                    promise.setException(BKException.create(rc));
                }
            }
        }, null);
        return promise;
    }

    public Enumeration<LedgerEntry> readEntries(LedgerDescriptor ledgerDesc, long first, long last)
            throws BKException {
        return FutureUtils.bkResult(asyncReadEntries(ledgerDesc, first, last));
    }

    public long getLength(LedgerDescriptor ledgerDesc) throws BKException {
        RefCountedLedgerHandle refhandle = getLedgerHandle(ledgerDesc);

        if (null == refhandle) {
            LOG.error("Accessing ledger {} without opening.", ledgerDesc);
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }

        return refhandle.handle.getLength();
    }

    public void clear() {
        if (null != handlesMap) {
            Iterator<Map.Entry<LedgerDescriptor, RefCountedLedgerHandle>> handlesMapIter = handlesMap.entrySet().iterator();
            while (handlesMapIter.hasNext()) {
                Map.Entry<LedgerDescriptor, RefCountedLedgerHandle> entry = handlesMapIter.next();
                // Make it inaccessible through the map
                handlesMapIter.remove();
                // now close the ledger
                entry.getValue().forceClose();
            }
        }
    }

    static class RefCountedLedgerHandle {
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

        public void forceClose() {
            try {
                handle.close();
            } catch (BKException.BKLedgerClosedException exc) {
                // Ignore
            } catch (Exception exc) {
                LOG.warn("Exception while closing ledger {}", handle, exc);
            }
        }

    }
}
