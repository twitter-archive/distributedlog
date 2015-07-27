package com.twitter.distributedlog.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;

public class ZkMetadataUpdater implements MetadataUpdater {

    static final Logger LOG = LoggerFactory.getLogger(ZkMetadataUpdater.class);

    public static MetadataUpdater createMetadataUpdater(DistributedLogConfiguration conf,
                                                        ZooKeeperClient zkc) {
        return new ZkMetadataUpdater(conf, zkc);
    }

    protected final ZooKeeperClient zkc;
    protected final LogSegmentMetadata.LogSegmentMetadataVersion metadataVersion;

    public ZkMetadataUpdater(DistributedLogConfiguration conf,
                             ZooKeeperClient zkc) {
        this.zkc = zkc;
        this.metadataVersion = LogSegmentMetadata.LogSegmentMetadataVersion.of(conf.getDLLedgerMetadataLayoutVersion());
    }

    private String formatLedgerSequenceNumber(long ledgerSeqNo) {
        return String.format("%018d", ledgerSeqNo);
    }

    @Override
    public LogSegmentMetadata updateLastRecord(LogSegmentMetadata segment, LogRecordWithDLSN record)
            throws IOException {
        DLSN dlsn = record.getDlsn();
        Preconditions.checkState(!segment.isInProgress(), "Updating last dlsn for an inprogress log segment isn't supported.");
        Preconditions.checkArgument(segment.isDLSNinThisSegment(dlsn),
                "DLSN " + dlsn + " doesn't belong to segment " + segment);
        final LogSegmentMetadata newSegment = segment.mutator()
                .setLastDLSN(dlsn)
                .setLastTxId(record.getTransactionId())
                .setRecordCount(record)
                .build();
        updateSegmentMetadata(newSegment);
        return newSegment;
    }

    @VisibleForTesting
    @Override
    public LogSegmentMetadata changeSequenceNumber(LogSegmentMetadata segment,
                                                         long ledgerSeqNo) throws IOException {
        final LogSegmentMetadata newSegment = segment.mutator()
                .setLedgerSequenceNumber(ledgerSeqNo)
                .setZkPath(segment.getZkPath().replace(formatLedgerSequenceNumber(segment.getLedgerSequenceNumber()),
                        formatLedgerSequenceNumber(ledgerSeqNo)))
                .build();
        addNewSegmentAndDeleteOldSegment(newSegment, segment);
        return newSegment;
    }

    /**
     * Change the truncation status of a <i>log segment</i> to be active
     *
     * @param segment log segment to change truncation status to active.
     * @return new log segment
     */
    @Override
    public LogSegmentMetadata setLogSegmentActive(LogSegmentMetadata segment) throws IOException {
        final LogSegmentMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentMetadata.TruncationStatus.ACTIVE)
            .build();
        addNewSegmentAndDeleteOldSegment(newSegment, segment);
        return newSegment;    }

    /**
     * Change the truncation status of a <i>log segment</i> to truncated
     *
     * @param segment log segment to change truncation status to truncated.
     * @return new log segment
     */
    @Override
    public LogSegmentMetadata setLogSegmentTruncated(LogSegmentMetadata segment) throws IOException {
        final LogSegmentMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentMetadata.TruncationStatus.TRUNCATED)
            .build();
        addNewSegmentAndDeleteOldSegment(newSegment, segment);
        return newSegment;
    }

    /**
     * Change the truncation status of a <i>log segment</i> to partially truncated
     *
     * @param segment log segment to change sequence number.
     * @param minActiveDLSN DLSN within the log segment before which log has been truncated
     * @return new log segment
     */
    @Override
    public LogSegmentMetadata setLogSegmentPartiallyTruncated(LogSegmentMetadata segment, DLSN minActiveDLSN) throws IOException {
        final LogSegmentMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentMetadata.TruncationStatus.PARTIALLY_TRUNCATED)
            .setMinActiveDLSN(minActiveDLSN)
            .build();
        addNewSegmentAndDeleteOldSegment(newSegment, segment);
        return newSegment;
    }

    protected void updateSegmentMetadata(LogSegmentMetadata segment) throws IOException {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        try {
            zkc.get().setData(segment.getZkPath(), finalisedData, -1);
        } catch (KeeperException e) {
            LOG.error("Failed on updating log segment {}", segment, e);
            throw new ZKException("Failed on updating log segment " + segment, e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted on updating segment {}", segment, e);
            throw new DLInterruptedException("Interrupted on updating segment " + segment, e);
        }
    }

    protected void addNewSegmentAndDeleteOldSegment(LogSegmentMetadata newSegment,
                                                    LogSegmentMetadata oldSegment) throws IOException {
        LOG.info("old segment {} new segment {}", oldSegment, newSegment);
        try {
            Transaction txn = zkc.get().transaction();
            byte[] finalisedData = newSegment.getFinalisedData().getBytes(UTF_8);
            // delete old log segment
            txn.delete(oldSegment.getZkPath(), -1);
            // create new log segment
            txn.create(newSegment.getZkPath(), finalisedData, zkc.getDefaultACL(), CreateMode.PERSISTENT);
            // commit the transaction
            txn.commit();
        } catch (InterruptedException e) {
            LOG.error("Interrupted on transaction for adding new segment {} and deleting old segment {}", new Object[] { newSegment, oldSegment, e});
            throw new DLInterruptedException("Interrupted on transaction for adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment, e);
        } catch (KeeperException ke) {
            LOG.error("Failed on adding new segment {} and deleting old segment {}", new Object[] { newSegment, oldSegment, ke});
            throw new ZKException("Failed on transaction for adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment, ke);
        }
    }

}
