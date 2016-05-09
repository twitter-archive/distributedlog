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
package com.twitter.distributedlog.client.stats;

import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;

/**
 * Stats Logger per operation type
 */
public class OpStatsLogger {

    private final Stat successLatencyStat;
    private final Stat failureLatencyStat;
    private final Stat redirectStat;

    public OpStatsLogger(StatsReceiver statsReceiver) {
        StatsReceiver latencyStatReceiver = statsReceiver.scope("latency");
        successLatencyStat = latencyStatReceiver.stat0("success");
        failureLatencyStat = latencyStatReceiver.stat0("failure");
        StatsReceiver redirectStatReceiver = statsReceiver.scope("redirects");
        redirectStat = redirectStatReceiver.stat0("times");
    }

    public void completeRequest(long micros, int numTries) {
        successLatencyStat.add(micros);
        redirectStat.add(numTries);
    }

    public void failRequest(long micros, int numTries) {
        failureLatencyStat.add(micros);
        redirectStat.add(numTries);
    }

}
