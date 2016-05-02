package com.twitter.distributedlog.messaging;

/**
 * Partitioner where key is an integer
 */
public class IntPartitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer key, int totalPartitions) {
        return key % totalPartitions;
    }
}
