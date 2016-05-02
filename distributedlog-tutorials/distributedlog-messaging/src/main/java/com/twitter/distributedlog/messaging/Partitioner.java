package com.twitter.distributedlog.messaging;

/**
 * Partitioner
 */
public interface Partitioner<KEY> {
    int partition(KEY key, int totalPartitions);
}
