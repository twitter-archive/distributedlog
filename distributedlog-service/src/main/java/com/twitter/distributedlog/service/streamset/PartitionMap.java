/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
