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
}
