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
package com.twitter.distributedlog.benchmark.utils;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper over rate limiter
 */
public class ShiftableRateLimiter implements Runnable {

    private final RateLimiter rateLimiter;
    private final ScheduledExecutorService executor;
    private final double initialRate, maxRate, changeRate;
    private final long changeInterval;
    private final TimeUnit changeIntervalUnit;
    private double nextRate;

    public ShiftableRateLimiter(double initialRate,
                                double maxRate,
                                double changeRate,
                                long changeInterval,
                                TimeUnit changeIntervalUnit) {
        this.initialRate = initialRate;
        this.maxRate = maxRate;
        this.changeRate = changeRate;
        this.nextRate = initialRate;
        this.changeInterval = changeInterval;
        this.changeIntervalUnit = changeIntervalUnit;
        this.rateLimiter = RateLimiter.create(initialRate);
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.executor.scheduleAtFixedRate(this, changeInterval, changeInterval, changeIntervalUnit);
    }

    public ShiftableRateLimiter duplicate() {
        return new ShiftableRateLimiter(
                initialRate,
                maxRate,
                changeRate,
                changeInterval,
                changeIntervalUnit);
    }

    @Override
    public void run() {
        this.nextRate = Math.min(nextRate + changeRate, maxRate);
        this.rateLimiter.setRate(nextRate);
    }

    public RateLimiter getLimiter() {
        return this.rateLimiter;
    }
}
