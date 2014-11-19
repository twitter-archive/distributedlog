package com.twitter.distributedlog.service.balancer;

/**
 * Choose a stream to rebalance
 */
public interface StreamChooser {
    /**
     * Choose a stream to rebalance.
     *
     * @return stream chose
     */
    String choose();
}
