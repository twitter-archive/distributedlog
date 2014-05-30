package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;

import java.io.IOException;

public class DryrunZkMetadataUpdater extends ZkMetadataUpdater {

    public DryrunZkMetadataUpdater(ZooKeeperClient zkc) {
        super(zkc);
    }

    @Override
    protected void addNewSegmentAndDeleteOldSegment(LogSegmentLedgerMetadata newSegment,
                                                    LogSegmentLedgerMetadata oldSegment) throws IOException {
        // nop
    }
}
