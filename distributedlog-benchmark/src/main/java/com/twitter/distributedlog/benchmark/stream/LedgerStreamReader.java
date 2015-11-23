package com.twitter.distributedlog.benchmark.stream;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reading ledger in a streaming way.
 */
public class LedgerStreamReader implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(LedgerStreamReader.class);

    class PendingReadRequest implements AsyncCallback.ReadCallback {

        final long entryId;
        boolean isDone = false;
        int rc;
        LedgerEntry entry = null;

        PendingReadRequest(long entryId) {
            this.entryId = entryId;
        }

        void read() {
            lh.asyncReadEntries(entryId, entryId, this, null);
        }

        void complete(ReadEntryListener listener) {
            listener.onEntryComplete(rc, lh, entry, null);
        }

        @Override
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> enumeration, Object ctx) {
            this.rc = rc;
            if (BKException.Code.OK == rc && enumeration.hasMoreElements()) {
                entry = enumeration.nextElement();
            } else {
                entry = null;
            }
            isDone = true;
            // construct a new read request
            long nextEntry = nextReadEntry.getAndIncrement();
            if (nextEntry <= lac) {
                PendingReadRequest nextRead =
                        new PendingReadRequest(nextEntry);
                pendingReads.add(nextRead);
                nextRead.read();
            }
            triggerCallbacks();
        }
    }

    private final LedgerHandle lh;
    private final long lac;
    private final ReadEntryListener readEntryListener;
    private final int concurrency;
    private final AtomicLong nextReadEntry = new AtomicLong(0);
    private final CountDownLatch done = new CountDownLatch(1);
    private final ConcurrentLinkedQueue<PendingReadRequest> pendingReads =
            new ConcurrentLinkedQueue<PendingReadRequest>();

    public LedgerStreamReader(LedgerHandle lh,
                              ReadEntryListener readEntryListener,
                              int concurrency) {
        this.lh = lh;
        this.lac = lh.getLastAddConfirmed();
        this.readEntryListener = readEntryListener;
        this.concurrency = concurrency;
        for (int i = 0; i < concurrency; i++) {
            long entryId = nextReadEntry.getAndIncrement();
            if (entryId > lac) {
                break;
            }
            PendingReadRequest request = new PendingReadRequest(entryId);
            pendingReads.add(request);
            request.read();
        }
        if (pendingReads.isEmpty()) {
            done.countDown();
        }
    }

    synchronized void triggerCallbacks() {
        PendingReadRequest request;
        while ((request = pendingReads.peek()) != null) {
            if (!request.isDone) {
                break;
            }
            pendingReads.remove();
            request.complete(readEntryListener);
        }
        if (pendingReads.isEmpty()) {
            done.countDown();
        }
    }

    @Override
    public void run() {
        try {
            done.await();
        } catch (InterruptedException e) {
            logger.info("Interrupted on stream reading ledger {} : ", lh.getId(), e);
        }
    }
}
