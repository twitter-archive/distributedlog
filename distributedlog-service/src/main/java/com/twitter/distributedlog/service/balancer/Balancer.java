package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;

public interface Balancer {

    /**
     * Rebalance all the streams from <i>source</i> to others.
     *
     * @param source
     *          source target name.
     * @param rebalanceConcurrency
     *          the concurrency to move streams for re-balance.
     * @param rebalanceRateLimiter
     *          the rate limiting to move streams for re-balance.
     */
    void balanceAll(String source,
                    int rebalanceConcurrency,
                    Optional<RateLimiter> rebalanceRateLimiter);

    /**
     * Balance the streams across all targets.
     *
     * @param rebalanceWaterMark
     *          rebalance water mark. if number of streams of a given target is less than
     *          the water mark, no streams will be re-balanced from this target.
     * @param rebalanceTolerancePercentage
     *          tolerance percentage for the balancer. if number of streams of a given target is
     *          less than average + average * <i>tolerancePercentage</i> / 100.0, no streams will
     *          be re-balanced from that target.
     * @param rebalanceConcurrency
     *          the concurrency to move streams for re-balance.
     * @param rebalanceRateLimiter
     *          the rate limiting to move streams for re-balance.
     */
    void balance(int rebalanceWaterMark,
                 double rebalanceTolerancePercentage,
                 int rebalanceConcurrency,
                 Optional<RateLimiter> rebalanceRateLimiter);

    /**
     * Close the balancer.
     */
    void close();
}
