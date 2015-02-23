package com.twitter.distributedlog.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.DistributedLogConstants.ZK_VERSION;

public class ZkMetadataUpdater implements MetadataUpdater {

    static final Logger LOG = LoggerFactory.getLogger(ZkMetadataUpdater.class);

    static Class<? extends ZkMetadataUpdater> ZK_METADATA_UPDATER_CLASS = null;
    static final Class[] ZK_METADATA_UPDATER_CONSTRUCTOR_ARGS = { ZooKeeperClient.class };

    public static MetadataUpdater createMetadataUpdater(ZooKeeperClient zkc) {
        if (ZK_VERSION.getVersion().equals(DistributedLogConstants.ZK33)) {
            return new ZkMetadataUpdater(zkc);
        } else {
            if (null == ZK_METADATA_UPDATER_CLASS) {
                try {
                    ZK_METADATA_UPDATER_CLASS = (Class<? extends ZkMetadataUpdater>)
                            Class.forName("com.twitter.distributedlog.metadata.ZkMetadataUpdater34", true,
                                          ZkMetadataUpdater.class.getClassLoader());
                    LOG.info("Instantiate zk metadata updater class : {}", ZK_METADATA_UPDATER_CLASS);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Can't initialize the zk metadata updater class : ", e);
                }
            }
            // Create the new instance
            Constructor<? extends ZkMetadataUpdater> constructor;
            try {
                constructor = ZK_METADATA_UPDATER_CLASS.getDeclaredConstructor(ZK_METADATA_UPDATER_CONSTRUCTOR_ARGS);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("No constructor found for zk metadata updater class " + ZK_METADATA_UPDATER_CLASS, e);
            }
            try {
                return constructor.newInstance(zkc);
            } catch (InstantiationException e) {
                throw new RuntimeException("Failed to instantiate zk metadata updater : ", e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Encountered illegal access when instantiating zk metadata updater : ", e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException("Encountered invocation target exception when instantiating zk metadata updater : ", e);
            }
        }
    }

    protected final ZooKeeperClient zkc;

    public ZkMetadataUpdater(ZooKeeperClient zkc) {
        this.zkc = zkc;
    }

    private String formatLedgerSequenceNumber(long ledgerSeqNo) {
        return String.format("%018d", ledgerSeqNo);
    }

    @Override
    public LogSegmentLedgerMetadata updateLastRecord(LogSegmentLedgerMetadata segment, LogRecordWithDLSN record)
            throws IOException {
        DLSN dlsn = record.getDlsn();
        Preconditions.checkState(!segment.isInProgress(), "Updating last dlsn for an inprogress log segment isn't supported.");
        Preconditions.checkArgument(segment.isDLSNinThisSegment(dlsn),
                "DLSN " + dlsn + " doesn't belong to segment " + segment);
        final LogSegmentLedgerMetadata newSegment = segment.mutator()
                .setLastDLSN(dlsn)
                .setLastTxId(record.getTransactionId())
                .setRecordCount(record)
                .build();
        updateSegmentMetadata(newSegment);
        return newSegment;
    }

    @VisibleForTesting
    @Override
    public LogSegmentLedgerMetadata changeSequenceNumber(LogSegmentLedgerMetadata segment,
                                                         long ledgerSeqNo) throws IOException {
        final LogSegmentLedgerMetadata newSegment = segment.mutator()
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
    public LogSegmentLedgerMetadata setLogSegmentActive(LogSegmentLedgerMetadata segment) throws IOException {
        final LogSegmentLedgerMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentLedgerMetadata.TruncationStatus.ACTIVE)
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
    public LogSegmentLedgerMetadata setLogSegmentTruncated(LogSegmentLedgerMetadata segment) throws IOException {
        final LogSegmentLedgerMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentLedgerMetadata.TruncationStatus.TRUNCATED)
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
    public LogSegmentLedgerMetadata setLogSegmentPartiallyTruncated(LogSegmentLedgerMetadata segment, DLSN minActiveDLSN) throws IOException {
        final LogSegmentLedgerMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentLedgerMetadata.TruncationStatus.PARTIALLY_TRUNCATED)
            .setMinActiveDLSN(minActiveDLSN)
            .build();
        addNewSegmentAndDeleteOldSegment(newSegment, segment);
        return newSegment;
    }

    protected void updateSegmentMetadata(LogSegmentLedgerMetadata segment) throws IOException {
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

    protected void addNewSegmentAndDeleteOldSegment(LogSegmentLedgerMetadata newSegment,
                                                    LogSegmentLedgerMetadata oldSegment) throws IOException {
        try {
            byte[] finalisedData = newSegment.getFinalisedData().getBytes(UTF_8);
            if (newSegment.getZkPath().equalsIgnoreCase(oldSegment.getZkPath())) {
                zkc.get().setData(newSegment.getZkPath(), finalisedData, -1);
            } else {
                zkc.get().create(newSegment.getZkPath(), finalisedData, zkc.getDefaultACL(), CreateMode.PERSISTENT);
                // delete old log segment
                zkc.get().delete(oldSegment.getZkPath(), -1);
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted on adding new segment {} and deleting old segment {}",
                new Object[] { newSegment, oldSegment, ie} );
            throw new DLInterruptedException("Interrupted on adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment + " : ", ie);
        } catch (KeeperException ke) {
            LOG.error("Failed on adding new segment {} and deleting old segment {}", new Object[] { newSegment, oldSegment, ke});
            throw new ZKException("Failed on adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment + " : ", ke);
        }
    }

}
