package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;

import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;

public class ZkMetadataUpdater34 extends ZkMetadataUpdater {

    public ZkMetadataUpdater34(ZooKeeperClient zkc) {
        super(zkc);
    }

    @Override
    protected void addNewSegmentAndDeleteOldSegment(LogSegmentLedgerMetadata newSegment,
                                                    LogSegmentLedgerMetadata oldSegment) throws IOException {
        try {
            Transaction txn = zkc.get().transaction();
            byte[] finalisedData = newSegment.getFinalisedData().getBytes(UTF_8);
            // delete old log segment
            zkc.get().delete(oldSegment.getZkPath(), -1);
            // create new log segment
            zkc.get().create(newSegment.getZkPath(), finalisedData, zkc.getDefaultACL(), CreateMode.PERSISTENT);
            // commit the transaction
            txn.commit();
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on transaction for adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment, e);
        } catch (KeeperException ke) {
            throw new ZKException("Failed on transaction for adding new segment " + newSegment
                    + " and deleting old segment " + oldSegment, ke);
        }
    }
}
