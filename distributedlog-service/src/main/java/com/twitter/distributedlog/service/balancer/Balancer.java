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
