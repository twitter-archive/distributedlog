package com.twitter.distributedlog;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Help class on reading records batched in a {@link org.apache.bookkeeper.client.LedgerEntry}.
 */
public class LedgerEntryReader extends LogRecord.Reader {

    private final static Logger LOGGER = LoggerFactory.getLogger(LedgerEntryReader.class);

    public static DataInputStream getInputStream(InputStream src, boolean envelopeEntries,
                                                 StatsLogger statsLogger) throws IOException {
        InputStream stream = src;
        if (envelopeEntries) {
            stream = EnvelopedEntry.fromInputStream(src, statsLogger);
        }
        return new DataInputStream(stream);
    }

    /**
     * Construct the reader
     *
     * @param name
     *          name of the reader
     * @param ledgerSeqNo
     *          ledger sequence number
     * @param ledgerEntry
     *          ledger entry
     * @param envelopeEntries
     *          Are the entries enveloped.
     * @param startSequenceId
     *          Start sequence id for the records to generate sequence id.
     * @param statsLogger
     *          stats logger
     */
    public LedgerEntryReader(final String name,
                             final long ledgerSeqNo,
                             final LedgerEntry ledgerEntry,
                             final boolean envelopeEntries,
                             final long startSequenceId,
                             StatsLogger statsLogger) throws IOException {
        this(name, ledgerSeqNo, ledgerEntry.getEntryId(), ledgerEntry.getEntryInputStream(),
                envelopeEntries, startSequenceId, statsLogger);
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
     * @param envelopeEntries
     *          Are the entries enveloped.
     * @param startSequenceId
     *          Start sequence id for the records to generate sequence id.
     * @param statsLogger
     *          stats logger
     */
    public LedgerEntryReader(final String name,
                             final long ledgerSeqNo,
                             final long entryId,
                             final InputStream in,
                             final boolean envelopeEntries,
                             final long startSequenceId,
                             StatsLogger statsLogger) throws IOException {
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
        }, getInputStream(in, envelopeEntries, statsLogger), 0, startSequenceId);
    }
}
