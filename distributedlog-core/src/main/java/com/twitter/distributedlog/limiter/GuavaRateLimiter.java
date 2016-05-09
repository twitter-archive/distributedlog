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

import com.google.common.base.Preconditions;

/**
 * Wrap a guava limiter in a simple interface to make testing easier.
 * Notes:
 * 1. Negative limit translates into (virtually) unlimited.
 * 2. Calling acquire with permits == 0 translates into no acquire.
 */
public class GuavaRateLimiter implements RateLimiter {
    com.google.common.util.concurrent.RateLimiter limiter;

    public static RateLimiter of(int limit) {
        if (limit == 0) {
            return RateLimiter.REJECT;
        } else if (limit < 0) {
            return RateLimiter.ACCEPT;
        } else {
            return new GuavaRateLimiter(limit);
        }
    }

    public GuavaRateLimiter(int limit) {
        double effectiveLimit = limit;
        if (limit < 0) {
            effectiveLimit = Double.POSITIVE_INFINITY;
        }
        this.limiter = com.google.common.util.concurrent.RateLimiter.create(effectiveLimit);
    }

    @Override
    public boolean acquire(int permits) {
        Preconditions.checkState(permits >= 0);
        if (permits > 0) {
            return limiter.tryAcquire(permits);
        } else {
            return true;
        }
    }
}
