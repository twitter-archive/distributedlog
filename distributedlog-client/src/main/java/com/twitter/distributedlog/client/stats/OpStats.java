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

import com.twitter.distributedlog.client.resolver.RegionResolver;
import com.twitter.finagle.stats.StatsReceiver;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Op Stats
 */
public class OpStats {

    // Region Resolver
    private final RegionResolver regionResolver;

    // Stats
    private final StatsReceiver statsReceiver;
    private final OpStatsLogger opStatsLogger;
    private final boolean enableRegionStats;
    private final ConcurrentMap<String, OpStatsLogger> regionOpStatsLoggers;

    public OpStats(StatsReceiver statsReceiver,
                   boolean enableRegionStats,
                   RegionResolver regionResolver) {
        this.statsReceiver = statsReceiver;
        this.opStatsLogger = new OpStatsLogger(statsReceiver);
        this.enableRegionStats = enableRegionStats;
        this.regionOpStatsLoggers = new ConcurrentHashMap<String, OpStatsLogger>();
        this.regionResolver = regionResolver;
    }

    private OpStatsLogger getRegionOpStatsLogger(SocketAddress address) {
        String region = regionResolver.resolveRegion(address);
        return getRegionOpStatsLogger(region);
    }

    private OpStatsLogger getRegionOpStatsLogger(String region) {
        OpStatsLogger statsLogger = regionOpStatsLoggers.get(region);
        if (null == statsLogger) {
            OpStatsLogger newStatsLogger = new OpStatsLogger(statsReceiver.scope(region));
            OpStatsLogger oldStatsLogger = regionOpStatsLoggers.putIfAbsent(region, newStatsLogger);
            if (null == oldStatsLogger) {
                statsLogger = newStatsLogger;
            } else {
                statsLogger = oldStatsLogger;
            }
        }
        return statsLogger;
    }

    public void completeRequest(SocketAddress addr, long micros, int numTries) {
        opStatsLogger.completeRequest(micros, numTries);
        if (enableRegionStats && null != addr) {
            getRegionOpStatsLogger(addr).completeRequest(micros, numTries);
        }
    }

    public void failRequest(SocketAddress addr, long micros, int numTries) {
        opStatsLogger.failRequest(micros, numTries);
        if (enableRegionStats && null != addr) {
            getRegionOpStatsLogger(addr).failRequest(micros, numTries);
        }
    }
}
