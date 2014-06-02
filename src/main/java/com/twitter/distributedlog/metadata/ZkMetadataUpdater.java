package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
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

    protected void addNewSegmentAndDeleteOldSegment(LogSegmentLedgerMetadata newSegment,
                                                    LogSegmentLedgerMetadata oldSegment) throws IOException {
        try {
            // create new log segment
            byte[] finalisedData = newSegment.getFinalisedData().getBytes(UTF_8);
            zkc.get().create(newSegment.getZkPath(), finalisedData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // delete old log segment
            zkc.get().delete(oldSegment.getZkPath(), -1);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment + " : ", ie);
        } catch (KeeperException ke) {
            throw new ZKException("Failed on adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment + " : ", ke);
        }
    }
}