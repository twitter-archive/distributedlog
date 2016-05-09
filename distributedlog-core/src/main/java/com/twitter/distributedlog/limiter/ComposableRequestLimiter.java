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
package com.twitter.distributedlog.limiter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.GuavaRateLimiter;
import com.twitter.distributedlog.limiter.RateLimiter;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collect rate limiter implementation, cost(Request), overlimit, etc. behavior.
 */
public class ComposableRequestLimiter<Request> implements RequestLimiter<Request> {
    protected static final Logger LOG = LoggerFactory.getLogger(ComposableRequestLimiter.class);

    private final RateLimiter limiter;
    private final OverlimitFunction<Request> overlimitFunction;
    private final CostFunction<Request> costFunction;
    private final Counter overlimitCounter;

    static public interface OverlimitFunction<Request> {
        void apply(Request request) throws OverCapacityException;
    }
    static public interface CostFunction<Request> {
        int apply(Request request);
    }

    public ComposableRequestLimiter(
            RateLimiter limiter,
            OverlimitFunction<Request> overlimitFunction,
            CostFunction<Request> costFunction,
            StatsLogger statsLogger) {
        Preconditions.checkNotNull(limiter);
        Preconditions.checkNotNull(overlimitFunction);
        Preconditions.checkNotNull(costFunction);
        this.limiter = limiter;
        this.overlimitFunction = overlimitFunction;
        this.costFunction = costFunction;
        this.overlimitCounter = statsLogger.getCounter("overlimit");
    }

    @Override
    public void apply(Request request) throws OverCapacityException {
        int permits = costFunction.apply(request);
        if (!limiter.acquire(permits)) {
            overlimitCounter.inc();
            overlimitFunction.apply(request);
        }
    }
}
