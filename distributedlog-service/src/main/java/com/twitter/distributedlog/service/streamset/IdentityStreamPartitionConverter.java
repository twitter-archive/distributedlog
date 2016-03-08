package com.twitter.distributedlog.service.streamset;

/**
 * Map stream name to partition of the same name.
 */
public class IdentityStreamPartitionConverter extends CacheableStreamPartitionConverter {
    @Override
    protected Partition newPartition(String streamName) {
        return new Partition(streamName, 0);
    }
}
