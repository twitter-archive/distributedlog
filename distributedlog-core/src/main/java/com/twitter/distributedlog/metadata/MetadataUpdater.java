package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DLSN;
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
     * Change the truncation status of a <i>log segment</i> to be active
     *
     * @param segment
     *          log segment to change truncation status to active.
     * @return new log segment
     * @throws IOException
     */
    LogSegmentLedgerMetadata setLogSegmentActive(LogSegmentLedgerMetadata segment)
            throws IOException;

    /**
     * Change the truncation status of a <i>log segment</i> to truncated
     *
     * @param segment
     *          log segment to change truncation status to truncated.
     * @return new log segment
     * @throws IOException
     */
    LogSegmentLedgerMetadata setLogSegmentTruncated(LogSegmentLedgerMetadata segment)
        throws IOException;

    /**
     * Change the truncation status of a <i>log segment</i> to partially truncated
     *
     * @param segment
     *          log segment to change sequence number.
     * @param minActiveDLSN
     *          DLSN within the log segment before which log has been truncated
     * @return new log segment
     * @throws IOException
     */
    LogSegmentLedgerMetadata setLogSegmentPartiallyTruncated(LogSegmentLedgerMetadata segment, DLSN minActiveDLSN)
        throws IOException;

}
