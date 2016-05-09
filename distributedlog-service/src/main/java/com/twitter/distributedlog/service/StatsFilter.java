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
package com.twitter.distributedlog.service;

import com.google.common.base.Stopwatch;

import com.twitter.finagle.Service;
import com.twitter.finagle.SimpleFilter;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;

import java.util.concurrent.TimeUnit;

/**
 * Track distributedlog server finagle-service stats.
 */
class StatsFilter<Req, Rep> extends SimpleFilter<Req, Rep> {

    private final StatsLogger stats;
    private final Counter outstandingAsync;
    private final OpStatsLogger serviceExec;

    @Override
    public Future<Rep> apply(Req req, Service<Req, Rep> service) {
        Future<Rep> result = null;
        outstandingAsync.inc();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            result = service.apply(req);
            serviceExec.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
        } finally {
            outstandingAsync.dec();
            if (null == result) {
                serviceExec.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
        return result;
    }

    public StatsFilter(StatsLogger stats) {
        this.stats = stats;
        this.outstandingAsync = stats.getCounter("outstandingAsync");
        this.serviceExec = stats.getOpStatsLogger("serviceExec");
    }
}
