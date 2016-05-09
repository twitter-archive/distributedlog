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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.twitter.distributedlog.exceptions.OverCapacityException;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Chain request limiters for easier management of multi limiter policy.
 */
public class ChainedRequestLimiter<Request> implements RequestLimiter<Request> {
    private final ImmutableList<RequestLimiter<Request>> limiters;
    private final OpStatsLogger applyTime;

    public static class Builder<Request> {
        private final ImmutableList.Builder<RequestLimiter<Request>> limitersBuilder;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;

        public Builder() {
            this.limitersBuilder = new ImmutableList.Builder<RequestLimiter<Request>>();
        }

        public Builder<Request> addLimiter(RequestLimiter<Request> limiter) {
            this.limitersBuilder.add(limiter);
            return this;
        }

        public Builder<Request> statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public ChainedRequestLimiter<Request> build() {
            return new ChainedRequestLimiter<Request>(limitersBuilder.build(), statsLogger);
        }
    }

    private ChainedRequestLimiter(ImmutableList<RequestLimiter<Request>> limiters,
                                  StatsLogger statsLogger) {
        this.limiters = limiters;
        this.applyTime = statsLogger.getOpStatsLogger("apply");
    }

    public void apply(Request request) throws OverCapacityException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            for (RequestLimiter<Request> limiter : limiters) {
                limiter.apply(request);
            }
        } finally {
            applyTime.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
        }
    }
}
