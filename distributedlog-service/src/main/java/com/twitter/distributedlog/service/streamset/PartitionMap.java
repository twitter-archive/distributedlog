package com.twitter.distributedlog.service.streamset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartitionMap {

    private final Map<String, Set<Partition>> partitionMap;

    public PartitionMap() {
        partitionMap = new HashMap<String, Set<Partition>>();
    }

    public synchronized boolean addPartition(Partition partition, int maxPartitions) {
        if (maxPartitions <= 0) {
            return true;
        }
        Set<Partition> partitions = partitionMap.get(partition.getStream());
        if (null == partitions) {
            partitions = new HashSet<Partition>();
            partitions.add(partition);
            partitionMap.put(partition.getStream(), partitions);
            return true;
        }
        if (partitions.contains(partition) || partitions.size() < maxPartitions) {
            partitions.add(partition);
            return true;
        }
        return false;
    }

    public synchronized boolean removePartition(Partition partition) {
        Set<Partition> partitions = partitionMap.get(partition.getStream());
        return null != partitions && partitions.remove(partition);
    }
}
