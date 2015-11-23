package com.twitter.distributedlog.benchmark.stream;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;

/**
 * Read ledgers in batches
 */
public class LedgerBatchReader implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(LedgerBatchReader.class);

    private final LedgerHandle lh;
    private final ReadEntryListener readEntryListener;
    private final int batchSize;

    public LedgerBatchReader(LedgerHandle lh,
                             ReadEntryListener readEntryListener,
                             int batchSize) {
        this.lh = lh;
        this.batchSize = batchSize;
        this.readEntryListener = readEntryListener;
    }

    @Override
    public void run() {
        long lac = lh.getLastAddConfirmed();

        long entryId = 0L;

        while (entryId <= lac) {
            long startEntryId = entryId;
            long endEntryId = Math.min(startEntryId + batchSize - 1, lac);

            Enumeration<LedgerEntry> entries = null;
            while (null == entries) {
                try {
                    entries = lh.readEntries(startEntryId, endEntryId);
                } catch (BKException bke) {
                    logger.error("Encountered exceptions on reading [ {} - {} ] ",
                            new Object[] { startEntryId, endEntryId, bke });
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (null == entries) {
                break;
            }

            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                readEntryListener.onEntryComplete(BKException.Code.OK, lh, entry, null);
            }

            entryId = endEntryId + 1;
        }

    }
}
