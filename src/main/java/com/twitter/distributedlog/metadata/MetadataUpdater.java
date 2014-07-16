package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.LogSegmentLedgerMetadata;

import java.io.IOException;

/**
 * An updater to update metadata.
 */
public interface MetadataUpdater {

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
