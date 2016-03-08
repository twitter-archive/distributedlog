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
     * @return
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
