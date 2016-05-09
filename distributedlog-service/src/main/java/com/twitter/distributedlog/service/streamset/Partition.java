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

import com.google.common.base.Objects;

/**
 * `Partition` defines the relationship between a `virtual` stream and a
 * physical DL stream.
 * <p>A `virtual` stream could be partitioned into multiple partitions
 * and each partition is effectively a DL stream.
 */
public class Partition {

    // Name of its parent stream.
    private final String stream;

    // Unique id of the partition within the stream.
    // It can be just simply an index id.
    public final int id;

    public Partition(String stream, int id) {
        this.stream = stream;
        this.id = id;
    }

    /**
     * Get the `virtual` stream name.
     *
     * @return the stream name.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Get the partition id of this partition.
     *
     * @return partition id
     */
    public int getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Partition)) {
            return false;
        }
        Partition partition = (Partition) o;

        return id == partition.id && Objects.equal(stream, partition.stream);
    }

    @Override
    public int hashCode() {
        int result = stream.hashCode();
        result = 31 * result + id;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Partition(")
          .append(stream)
          .append(", ")
          .append(id)
          .append(")");
        return sb.toString();
    }
}
