package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;

import java.io.IOException;

public class DryrunZkMetadataUpdater extends ZkMetadataUpdater {

    public DryrunZkMetadataUpdater(DistributedLogConfiguration conf, ZooKeeperClient zkc) {
        super(conf, zkc);
    }

    @Override
    protected void updateSegmentMetadata(LogSegmentLedgerMetadata segment) throws IOException {
        // nop
    }

    @Override
    protected void addNewSegmentAndDeleteOldSegment(LogSegmentLedgerMetadata newSegment,
                                                    LogSegmentLedgerMetadata oldSegment) throws IOException {
        // nop
    }
}
