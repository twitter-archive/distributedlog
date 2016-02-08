package com.twitter.distributedlog;

import org.apache.bookkeeper.stats.StatsLogger;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Record reader to read records from an enveloped entry buffer.
 */
class EnvelopedRecordSetReader implements LogRecordSet.Reader, RecordStream {

    private final long logSegmentSeqNo;
    private final long entryId;
    private final LogRecord.Reader reader;

    // slot id
    private long slotId = 0;

    EnvelopedRecordSetReader(long logSegmentSeqNo,
                             long entryId,
                             long startSequenceId,
                             InputStream in,
                             boolean envelopedEntry,
                             StatsLogger statsLogger)
            throws IOException {
        this.logSegmentSeqNo = logSegmentSeqNo;
        this.entryId = entryId;
        InputStream src = in;
        if (envelopedEntry) {
            src = EnvelopedEntry.fromInputStream(in, statsLogger);
        }
        this.reader = new LogRecord.Reader(this, new DataInputStream(src), startSequenceId);
    }

    @Override
    public LogRecordWithDLSN nextRecord() throws IOException {
        return reader.readOp();
    }

    @Override
    public boolean skipTo(long txId) throws IOException {
        return reader.skipTo(txId, true);
    }

    @Override
    public boolean skipTo(DLSN dlsn) throws IOException {
        return reader.skipTo(dlsn);
    }

    //
    // Record Stream
    //

    @Override
    public void advanceToNextRecord() {
        slotId++;
    }

    @Override
    public DLSN getCurrentPosition() {
        return new DLSN(logSegmentSeqNo, entryId, slotId);
    }

    @Override
    public String getName() {
        return "EnvelopedReader";
    }
}
