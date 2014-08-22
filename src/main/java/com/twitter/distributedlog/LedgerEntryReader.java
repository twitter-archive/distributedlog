package com.twitter.distributedlog;

import org.apache.bookkeeper.client.LedgerEntry;

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * Help class on reading records batched in a {@link org.apache.bookkeeper.client.LedgerEntry}.
 */
public class LedgerEntryReader extends LogRecord.Reader {
    /**
     * Construct the reader
     *
     * @param name
     *          name of the reader
     * @param ledgerSeqNo
     *          ledger sequence number
     * @param ledgerEntry
     *          ledger entry
     */
    public LedgerEntryReader(final String name, final long ledgerSeqNo, final LedgerEntry ledgerEntry) {
        this(name, ledgerSeqNo, ledgerEntry.getEntryId(), ledgerEntry.getEntryInputStream());
    }

    /**
     * Construct the reader from an input stream
     *
     * @param name
     *          name of the reader
     * @param ledgerSeqNo
     *          ledger sequence number
     * @param entryId
     *          entry id
     * @param in
     *          input stream of the data
     */
    public LedgerEntryReader(final String name, final long ledgerSeqNo, final long entryId, final InputStream in) {
        super(new RecordStream() {
            long slotId = 0;

            @Override
            public void advanceToNextRecord() {
                slotId++;
            }

            @Override
            public DLSN getCurrentPosition() {
                return new DLSN(ledgerSeqNo, entryId, slotId);
            }

            @Override
            public String getName() {
                return name;
            }
        }, new DataInputStream(in), 0);
    }
}
