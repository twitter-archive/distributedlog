package com.twitter.distributedlog.util;

/**
 * The {@code Sizable} interface is to provide the capability of calculating size
 * of any objects.
 */
public interface Sizable {
    /**
     * Calculate the size for this instance.
     *
     * @return size of the instance.
     */
    long size();
}
