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

import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.TimerTask;
import com.twitter.util.Timer;
import com.twitter.util.Time;
import java.util.concurrent.CopyOnWriteArrayList;
import scala.runtime.BoxedUnit;

public class MovingAverageRateFactory {

    private static final int DEFAULT_INTERVAL_SECS = 1;

    private final Timer timer;
    private final TimerTask timerTask;
    private final CopyOnWriteArrayList<SampledMovingAverageRate> avgs;

    public MovingAverageRateFactory(Timer timer) {
        this.avgs = new CopyOnWriteArrayList<SampledMovingAverageRate>();
        this.timer = timer;
        Function0<BoxedUnit> sampleTask = new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                sampleAll();
                return null;
            }
        };
        this.timerTask = timer.schedulePeriodically(
            Time.now(), Duration.fromSeconds(DEFAULT_INTERVAL_SECS), sampleTask);
    }

    public MovingAverageRate create(int intervalSecs) {
        SampledMovingAverageRate avg = new SampledMovingAverageRate(intervalSecs);
        avgs.add(avg);
        return avg;
    }

    public void close() {
        timerTask.cancel();
        avgs.clear();
    }

    private void sampleAll() {
        for (SampledMovingAverageRate avg : avgs) {
            avg.sample();
        }
    }
}