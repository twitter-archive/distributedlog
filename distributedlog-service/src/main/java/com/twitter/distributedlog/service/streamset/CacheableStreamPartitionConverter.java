package com.twitter.distributedlog.service.streamset;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class CacheableStreamPartitionConverter implements StreamPartitionConverter {

    private final ConcurrentMap<String, Partition> partitions;

    protected CacheableStreamPartitionConverter() {
        this.partitions = new ConcurrentHashMap<String, Partition>();
    }

    @Override
    public Partition convert(String streamName) {
        Partition p = partitions.get(streamName);
        if (null != p) {
            return p;
        }
        // not found
        Partition newPartition = newPartition(streamName);
        Partition oldPartition = partitions.putIfAbsent(streamName, newPartition);
        if (null == oldPartition) {
            return newPartition;
        } else {
            return oldPartition;
        }
    }

    /**
     * Create the partition from <code>streamName</code>.
     *
     * @param streamName
     *          stream name
     * @return partition id of the stream
     */
    protected abstract Partition newPartition(String streamName);
}
