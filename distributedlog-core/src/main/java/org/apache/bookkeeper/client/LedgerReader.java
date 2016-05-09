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
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reader used for DL tools to read entries
 *
 * TODO: move this to bookkeeper project?
 */
public class LedgerReader {

    static final Logger logger = LoggerFactory.getLogger(LedgerReader.class);

    public static class ReadResult<T> {
        final long entryId;
        final int rc;
        final T value;
        final InetSocketAddress srcAddr;

        ReadResult(long entryId, int rc, T value, InetSocketAddress srcAddr) {
            this.entryId = entryId;
            this.rc = rc;
            this.value = value;
            this.srcAddr = srcAddr;
        }

        public long getEntryId() {
            return entryId;
        }

        public int getResultCode() {
            return rc;
        }

        public T getValue() {
            return value;
        }

        public InetSocketAddress getBookieAddress() {
            return srcAddr;
        }
    }

    private final BookieClient bookieClient;

    public LedgerReader(BookKeeper bkc) {
        bookieClient = bkc.getBookieClient();
    }

    static public SortedMap<Long, ArrayList<BookieSocketAddress>> bookiesForLedger(final LedgerHandle lh) {
        return lh.getLedgerMetadata().getEnsembles();
    }

    public void readEntriesFromAllBookies(final LedgerHandle lh, long eid,
                                          final GenericCallback<Set<ReadResult<InputStream>>> callback) {
        List<Integer> writeSet = lh.distributionSchedule.getWriteSet(eid);
        final AtomicInteger numBookies = new AtomicInteger(writeSet.size());
        final Set<ReadResult<InputStream>> readResults = new HashSet<ReadResult<InputStream>>();
        ReadEntryCallback readEntryCallback = new ReadEntryCallback() {
            @Override
            public void readEntryComplete(int rc, long lid, long eid, ChannelBuffer buffer, Object ctx) {
                BookieSocketAddress bookieAddress = (BookieSocketAddress) ctx;
                ReadResult<InputStream> rr;
                if (BKException.Code.OK != rc) {
                    rr = new ReadResult<InputStream>(eid, rc, null, bookieAddress.getSocketAddress());
                } else {
                    try {
                        ChannelBufferInputStream is = lh.macManager.verifyDigestAndReturnData(eid, buffer);
                        rr = new ReadResult<InputStream>(eid, BKException.Code.OK, is, bookieAddress.getSocketAddress());
                    } catch (BKException.BKDigestMatchException e) {
                        rr = new ReadResult<InputStream>(eid, BKException.Code.DigestMatchException, null, bookieAddress.getSocketAddress());
                    }
                }
                readResults.add(rr);
                if (numBookies.decrementAndGet() == 0) {
                    callback.operationComplete(BKException.Code.OK, readResults);
                }
            }
        };

        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsemble(eid);
        for (Integer idx : writeSet) {
            bookieClient.readEntry(ensemble.get(idx), lh.getId(), eid, readEntryCallback, ensemble.get(idx));
        }
    }

    /**
     * Forward reading entries from last add confirmed.
     *
     * @param lh
     *          ledger handle to read entries
     * @param callback
     *          callback with the entries from last add confirmed.
     */
    public void forwardReadEntriesFromLastConfirmed(final LedgerHandle lh,
                                                    final GenericCallback<List<LedgerEntry>> callback) {
        final List<LedgerEntry> resultList = new ArrayList<LedgerEntry>();

        final AsyncCallback.ReadCallback readCallback = new AsyncCallback.ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx) {
                if (BKException.Code.NoSuchEntryException == rc) {
                    callback.operationComplete(BKException.Code.OK, resultList);
                } else if (BKException.Code.OK == rc) {
                    while (entries.hasMoreElements()) {
                        resultList.add(entries.nextElement());
                    }
                    long entryId = (Long) ctx;
                    ++entryId;
                    PendingReadOp readOp = new PendingReadOp(lh, lh.bk.scheduler, entryId, entryId, this, entryId);
                    readOp.initiate();
                } else {
                    callback.operationComplete(rc, resultList);
                }
            }
        };

        ReadLastConfirmedOp.LastConfirmedDataCallback readLACCallback = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
            @Override
            public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData recoveryData) {
                if (BKException.Code.OK != rc) {
                    callback.operationComplete(rc, resultList);
                    return;
                }

                if (LedgerHandle.INVALID_ENTRY_ID >= recoveryData.lastAddConfirmed) {
                    callback.operationComplete(BKException.Code.OK, resultList);
                    return;
                }

                long entryId = recoveryData.lastAddConfirmed;
                PendingReadOp readOp = new PendingReadOp(lh, lh.bk.scheduler, entryId, entryId, readCallback, entryId);
                try {
                    readOp.initiate();
                } catch (Throwable t) {
                    logger.error("Failed to initialize pending read entry {} for ledger {} : ",
                                 new Object[] { entryId, lh.getLedgerMetadata(), t });
                }
            }
        };
        // Read Last AddConfirmed
        new ReadLastConfirmedOp(lh, readLACCallback).initiate();
    }

    public void readLacs(final LedgerHandle lh, long eid,
                         final GenericCallback<Set<ReadResult<Long>>> callback) {
        List<Integer> writeSet = lh.distributionSchedule.getWriteSet(eid);
        final AtomicInteger numBookies = new AtomicInteger(writeSet.size());
        final Set<ReadResult<Long>> readResults = new HashSet<ReadResult<Long>>();
        ReadEntryCallback readEntryCallback = new ReadEntryCallback() {
            @Override
            public void readEntryComplete(int rc, long lid, long eid, ChannelBuffer buffer, Object ctx) {
                InetSocketAddress bookieAddress = (InetSocketAddress) ctx;
                ReadResult<Long> rr;
                if (BKException.Code.OK != rc) {
                    rr = new ReadResult<Long>(eid, rc, null, bookieAddress);
                } else {
                    try {
                        DigestManager.RecoveryData data = lh.macManager.verifyDigestAndReturnLastConfirmed(buffer);
                        rr = new ReadResult<Long>(eid, BKException.Code.OK, data.lastAddConfirmed, bookieAddress);
                    } catch (BKException.BKDigestMatchException e) {
                        rr = new ReadResult<Long>(eid, BKException.Code.DigestMatchException, null, bookieAddress);
                    }
                }
                readResults.add(rr);
                if (numBookies.decrementAndGet() == 0) {
                    callback.operationComplete(BKException.Code.OK, readResults);
                }
            }
        };

        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsemble(eid);
        for (Integer idx : writeSet) {
            bookieClient.readEntry(ensemble.get(idx), lh.getId(), eid, readEntryCallback, ensemble.get(idx));
        }
    }
}
