package org.apache.bookkeeper.client;

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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reader used for DL tools to read entries
 *
 * TODO: move this to bookkeeper project?
 */
public class LedgerReader {

    static final Logger logger = LoggerFactory.getLogger(LedgerReader.class);

    public static class ReadResult {
        final long entryId;
        final int rc;
        final InputStream entryStream;
        final InetSocketAddress srcAddr;

        ReadResult(long entryId, int rc, InputStream entryStream, InetSocketAddress srcAddr) {
            this.entryId = entryId;
            this.rc = rc;
            this.entryStream = entryStream;
            this.srcAddr = srcAddr;
        }

        public long getEntryId() {
            return entryId;
        }

        public int getResultCode() {
            return rc;
        }

        public InputStream getEntryStream() {
            return entryStream;
        }

        public InetSocketAddress getBookieAddress() {
            return srcAddr;
        }
    }

    private final BookieClient bookieClient;

    public LedgerReader(BookKeeper bkc) {
        bookieClient = bkc.getBookieClient();
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

    public void readEntriesFromAllBookies(final LedgerHandle lh, long eid,
                                          final GenericCallback<Set<ReadResult>> callback) {
        if (eid < 0 || eid > lh.getLastAddConfirmed()) {
            callback.operationComplete(BKException.Code.ReadException, null);
            return;
        }

        List<Integer> writeSet = lh.distributionSchedule.getWriteSet(eid);
        final AtomicInteger numBookies = new AtomicInteger(writeSet.size());
        final Set<ReadResult> readResults = new HashSet<ReadResult>();
        ReadEntryCallback readEntryCallback = new ReadEntryCallback() {
            @Override
            public void readEntryComplete(int rc, long lid, long eid, ChannelBuffer buffer, Object ctx) {
                InetSocketAddress bookieAddress = (InetSocketAddress) ctx;
                ReadResult rr;
                if (BKException.Code.OK != rc) {
                    rr = new ReadResult(eid, rc, null, bookieAddress);
                } else {
                    try {
                        ChannelBufferInputStream is = lh.macManager.verifyDigestAndReturnData(eid, buffer);
                        rr = new ReadResult(eid, BKException.Code.OK, is, bookieAddress);
                    } catch (BKException.BKDigestMatchException e) {
                        rr = new ReadResult(eid, BKException.Code.DigestMatchException, null, bookieAddress);
                    }
                }
                readResults.add(rr);
                if (numBookies.decrementAndGet() == 0) {
                    callback.operationComplete(BKException.Code.OK, readResults);
                }
            }
        };

        ArrayList<InetSocketAddress> ensemble = lh.getLedgerMetadata().getEnsemble(eid);
        for (Integer idx : writeSet) {
            bookieClient.readEntry(ensemble.get(idx), lh.getId(), eid, readEntryCallback, ensemble.get(idx));
        }
    }
}
