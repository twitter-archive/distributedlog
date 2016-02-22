package com.twitter.distributedlog.impl;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * Ledger based log segment entry writer.
 */
public class BKLogSegmentEntryWriter implements LogSegmentEntryWriter {

    private final LedgerHandle lh;

    public BKLogSegmentEntryWriter(LedgerHandle lh) {
        this.lh = lh;
    }

    @VisibleForTesting
    public LedgerHandle getLedgerHandle() {
        return this.lh;
    }

    @Override
    public long getLogSegmentId() {
        return lh.getId();
    }

    @Override
    public void close() throws BKException, InterruptedException {
        lh.close();
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length,
                              AsyncCallback.AddCallback callback, Object ctx) {
        lh.asyncAddEntry(data, offset, length, callback, ctx);
    }

    @Override
    public long size() {
        return lh.getLength();
    }
}
