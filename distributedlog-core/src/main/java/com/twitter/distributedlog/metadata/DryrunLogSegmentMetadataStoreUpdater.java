package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.util.Future;

public class DryrunLogSegmentMetadataStoreUpdater extends LogSegmentMetadataStoreUpdater {

    public DryrunLogSegmentMetadataStoreUpdater(DistributedLogConfiguration conf,
                                                LogSegmentMetadataStore metadataStore) {
        super(conf, metadataStore);
    }

    @Override
    public Transaction<Object> transaction() {
        return new Transaction<Object>() {
            @Override
            public void addOp(Op<Object> operation) {
                // no-op
            }

            @Override
            public Future<Void> execute() {
                return Future.Void();
            }
        };
    }
}
