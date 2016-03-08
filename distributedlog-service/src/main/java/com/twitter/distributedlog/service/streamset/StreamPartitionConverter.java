package com.twitter.distributedlog.service.streamset;

/**
 * Map stream name to a partition.
 *
 * @see Partition
 */
public interface StreamPartitionConverter {

    /**
     * Convert the stream name to partition.
     *
     * @param streamName
     *          stream name
     * @return partition
     */
    Partition convert(String streamName);
}
