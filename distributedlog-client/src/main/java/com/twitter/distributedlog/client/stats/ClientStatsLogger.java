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

import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.stats.Counter;
import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 */
public class ClientStatsLogger {

    // Stats
    private final StatsReceiver statsReceiver;
    private final StatsReceiver responseStatsReceiver;
    private final ConcurrentMap<StatusCode, Counter> responseStats =
            new ConcurrentHashMap<StatusCode, Counter>();
    private final StatsReceiver exceptionStatsReceiver;
    private final ConcurrentMap<Class<?>, Counter> exceptionStats =
            new ConcurrentHashMap<Class<?>, Counter>();

    private final Stat proxySuccessLatencyStat;
    private final Stat proxyFailureLatencyStat;

    public ClientStatsLogger(StatsReceiver statsReceiver) {
        this.statsReceiver = statsReceiver;
        responseStatsReceiver = statsReceiver.scope("responses");
        exceptionStatsReceiver = statsReceiver.scope("exceptions");
        StatsReceiver proxyLatencyStatReceiver = statsReceiver.scope("proxy_request_latency");
        proxySuccessLatencyStat = proxyLatencyStatReceiver.stat0("success");
        proxyFailureLatencyStat = proxyLatencyStatReceiver.stat0("failure");
    }

    public StatsReceiver getStatsReceiver() {
        return statsReceiver;
    }

    private Counter getResponseCounter(StatusCode code) {
        Counter counter = responseStats.get(code);
        if (null == counter) {
            Counter newCounter = responseStatsReceiver.counter0(code.name());
            Counter oldCounter = responseStats.putIfAbsent(code, newCounter);
            counter = null != oldCounter ? oldCounter : newCounter;
        }
        return counter;
    }

    private Counter getExceptionCounter(Class<?> cls) {
        Counter counter = exceptionStats.get(cls);
        if (null == counter) {
            Counter newCounter = exceptionStatsReceiver.counter0(cls.getName());
            Counter oldCounter = exceptionStats.putIfAbsent(cls, newCounter);
            counter = null != oldCounter ? oldCounter : newCounter;
        }
        return counter;
    }

    public void completeProxyRequest(StatusCode code, long startTimeNanos) {
        getResponseCounter(code).incr();
        proxySuccessLatencyStat.add(elapsedMicroSec(startTimeNanos));
    }

    public void failProxyRequest(Throwable cause, long startTimeNanos) {
        getExceptionCounter(cause.getClass()).incr();
        proxyFailureLatencyStat.add(elapsedMicroSec(startTimeNanos));
    }

    static long elapsedMicroSec(long startNanoTime) {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanoTime);
    }
}
