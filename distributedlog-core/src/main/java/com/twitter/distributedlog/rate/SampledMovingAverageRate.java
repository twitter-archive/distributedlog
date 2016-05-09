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
package com.twitter.distributedlog.rate;

import com.twitter.common.stats.Rate;
import com.twitter.util.TimerTask;
import com.twitter.util.Timer;
import com.twitter.util.Time;
import java.util.concurrent.atomic.AtomicLong;

class SampledMovingAverageRate implements MovingAverageRate {
    private final Rate rate;
    private final AtomicLong total;

    private double value;

    public SampledMovingAverageRate(int intervalSecs) {
        this.total = new AtomicLong(0);
        this.rate = Rate.of("Ignore", total)
            .withWindowSize(intervalSecs)
            .build();
        this.value = 0;
    }

    @Override
    public double get() {
        return value;
    }

    @Override
    public void add(long amount) {
        total.getAndAdd(amount);
    }

    @Override
    public void inc() {
        add(1);
    }

    void sample() {
        value = rate.doSample();
    }
}
