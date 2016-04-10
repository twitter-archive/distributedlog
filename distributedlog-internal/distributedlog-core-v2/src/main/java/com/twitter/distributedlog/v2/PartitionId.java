package com.twitter.distributedlog.v2;

public class PartitionId {
    private final int partition;

    public PartitionId(int partition) {
        this.partition = partition;
    }

    public int getValue() {
        return partition;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PartitionId)) {
            return false;
        }
        PartitionId that = (PartitionId) other;
        return (this.partition == that.partition);
    }

    @Override
    public int hashCode() {
        return (partition * 13 ^ partition * 17);
    }

    @Override
    public String toString() {
        return String.format("%d", partition);
    }
}
