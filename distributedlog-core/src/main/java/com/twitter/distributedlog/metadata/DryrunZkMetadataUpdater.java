package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.ZooKeeperClient;

import java.io.IOException;

public class DryrunZkMetadataUpdater extends ZkMetadataUpdater {

    public DryrunZkMetadataUpdater(DistributedLogConfiguration conf, ZooKeeperClient zkc) {
        super(conf, zkc);
    }

    @Override
    protected void updateSegmentMetadata(LogSegmentMetadata segment) throws IOException {
        // nop
    }

    @Override
    protected void addNewSegmentAndDeleteOldSegment(LogSegmentMetadata newSegment,
                                                    LogSegmentMetadata oldSegment) throws IOException {
        // nop
    }
}
