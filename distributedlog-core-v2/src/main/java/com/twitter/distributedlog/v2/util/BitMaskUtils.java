package com.twitter.distributedlog.v2.util;

import com.google.common.base.Preconditions;

public class BitMaskUtils {

    /**
     * 1) Unset all bits where value in mask is set.
     * 2) Set these bits to value specified by newValue.
     *
     * e.g.
     * if oldValue = 1010, mask = 0011, newValue = 0001
     * 1) 1010 -> 1000
     * 2) 1000 -> 1001
     *
     * @param oldValue
     * @param mask
     * @param newValue
     * @return
     */
    public static long set(long oldValue, long mask, long newValue) {
        Preconditions.checkArgument(oldValue >= 0L && mask >= 0L && newValue >= 0L);
        return ((oldValue & (~mask)) | (newValue & mask));
    }

    /**
     * Get the bits where mask is 1
     * @param value
     * @param mask
     * @return
     */
    public static long get(long value, long mask) {
        Preconditions.checkArgument(value >= 0L && mask >= 0L);
        return (value & mask);
    }
}
