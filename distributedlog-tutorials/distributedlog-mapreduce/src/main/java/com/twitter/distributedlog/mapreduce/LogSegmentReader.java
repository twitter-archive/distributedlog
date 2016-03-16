package com.twitter.distributedlog.mapreduce;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LedgerEntryReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Enumeration;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Record Reader to read from a log segment split
 */
class LogSegmentReader extends RecordReader<DLSN, LogRecordWithDLSN> {

    final String streamName;
    final BookKeeper bk;
    final LedgerHandle lh;
    final LogSegmentMetadata metadata;

    long entryId = -1L;
    LogRecord.Reader reader = null;
    LogRecordWithDLSN currentRecord = null;
    int readPos = 0;

    LogSegmentReader(String streamName,
                     DistributedLogConfiguration conf,
                     BookKeeper bk,
                     LogSegmentSplit split)
            throws IOException {
        this.streamName = streamName;
        this.bk = bk;
        this.metadata = split.getMetadata();
        try {
            this.lh = bk.openLedgerNoRecovery(
                    split.getLedgerId(),
                    BookKeeper.DigestType.CRC32,
                    conf.getBKDigestPW().getBytes(UTF_8));
        } catch (BKException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // do nothing
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        LogRecordWithDLSN record;
        currentRecord = null;
        if (null != reader) {
            record = reader.readOp();
            if (null != record) {
                currentRecord = record;
                readPos = record.getPositionWithinLogSegment();
                return true;
            } else {
                reader = null;
            }
        }
        ++entryId;
        if (entryId > lh.getLastAddConfirmed()) {
            return false;
        }
        try {
            Enumeration<LedgerEntry> entries =
                    lh.readEntries(entryId, entryId);
            if (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                reader = new LedgerEntryReader(
                        streamName,
                        metadata.getLogSegmentSequenceNumber(),
                        entry,
                        LogSegmentMetadata.supportsEnvelopedEntries(
                                metadata.getVersion()),
                        metadata.getStartSequenceId(),
                        NullStatsLogger.INSTANCE);
            }
            return nextKeyValue();
        } catch (BKException e) {
            throw new IOException(e);
        }
    }

    @Override
    public DLSN getCurrentKey()
            throws IOException, InterruptedException {
        return currentRecord.getDlsn();
    }

    @Override
    public LogRecordWithDLSN getCurrentValue()
            throws IOException, InterruptedException {
        return currentRecord;
    }

    @Override
    public float getProgress()
            throws IOException, InterruptedException {
        if (metadata.getRecordCount() > 0) {
            return ((float) (readPos + 1)) / metadata.getRecordCount();
        }
        return 1;
    }

    @Override
    public void close() throws IOException {
        try {
            lh.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (BKException e) {
            throw new IOException(e);
        }
    }
}
