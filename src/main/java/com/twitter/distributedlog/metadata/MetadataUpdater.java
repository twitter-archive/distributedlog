package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;

import java.io.IOException;

/**
 * An updater to update metadata.
 */
public interface MetadataUpdater {

    /**
     * Update the log segment metadata with correct last <i>record</i>.
     *
     * @param segment
     *          log segment to update last dlsn.
     * @param record
     *          correct last record.
     * @return new log segment
     * @throws IOException
     */
    LogSegmentLedgerMetadata updateLastRecord(LogSegmentLedgerMetadata segment, LogRecordWithDLSN record)
            throws IOException;

    /**
     * Change ledger sequence number of <i>segment</i> to given <i>ledgerSeqNo</i>.
     *
     * @param segment
     *          log segment to change sequence number.
     * @param ledgerSeqNo
     *          ledger sequence number to change.
     * @return new log segment
     * @throws IOException
     */
    LogSegmentLedgerMetadata changeSequenceNumber(LogSegmentLedgerMetadata segment, long ledgerSeqNo)
            throws IOException;

    /**
     * Change the truncation status of a <i>log segment</i>
     *
     * @param segment
     *          log segment to change sequence number.
     * @param isTruncated
     *          is the segment truncated
     * @param isPartiallyTruncated
     *          is the segment partially truncated
     * @return new log segment
     * @throws IOException
     */
    LogSegmentLedgerMetadata changeTruncationStatus(LogSegmentLedgerMetadata segment,
                                                    LogSegmentLedgerMetadata.TruncationStatus truncationStatus)
            throws IOException;
}
