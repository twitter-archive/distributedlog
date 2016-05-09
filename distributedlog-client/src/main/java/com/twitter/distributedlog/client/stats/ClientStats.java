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
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.stats.StatsReceiver;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client Stats
 */
public class ClientStats {

    // Region Resolver
    private final RegionResolver regionResolver;

    // Stats
    private final StatsReceiver statsReceiver;
    private final ClientStatsLogger clientStatsLogger;
    private final boolean enableRegionStats;
    private final ConcurrentMap<String, ClientStatsLogger> regionClientStatsLoggers;
    private final ConcurrentMap<String, OpStats> opStatsMap;

    public ClientStats(StatsReceiver statsReceiver,
                       boolean enableRegionStats,
                       RegionResolver regionResolver) {
        this.statsReceiver = statsReceiver;
        this.clientStatsLogger = new ClientStatsLogger(statsReceiver);
        this.enableRegionStats = enableRegionStats;
        this.regionClientStatsLoggers = new ConcurrentHashMap<String, ClientStatsLogger>();
        this.regionResolver = regionResolver;
        this.opStatsMap = new ConcurrentHashMap<String, OpStats>();
    }

    public OpStats getOpStats(String op) {
        OpStats opStats = opStatsMap.get(op);
        if (null != opStats) {
            return opStats;
        }
        OpStats newStats = new OpStats(statsReceiver.scope(op),
                enableRegionStats, regionResolver);
        OpStats oldStats = opStatsMap.putIfAbsent(op, newStats);
        if (null == oldStats) {
            return newStats;
        } else {
            return oldStats;
        }
    }

    private ClientStatsLogger getRegionClientStatsLogger(SocketAddress address) {
        String region = regionResolver.resolveRegion(address);
        return getRegionClientStatsLogger(region);
    }

    private ClientStatsLogger getRegionClientStatsLogger(String region) {
        ClientStatsLogger statsLogger = regionClientStatsLoggers.get(region);
        if (null == statsLogger) {
            ClientStatsLogger newStatsLogger = new ClientStatsLogger(statsReceiver.scope(region));
            ClientStatsLogger oldStatsLogger = regionClientStatsLoggers.putIfAbsent(region, newStatsLogger);
            if (null == oldStatsLogger) {
                statsLogger = newStatsLogger;
            } else {
                statsLogger = oldStatsLogger;
            }
        }
        return statsLogger;
    }

    public StatsReceiver getFinagleStatsReceiver(SocketAddress addr) {
        if (enableRegionStats && null != addr) {
            return getRegionClientStatsLogger(addr).getStatsReceiver();
        } else {
            return clientStatsLogger.getStatsReceiver();
        }
    }

    public void completeProxyRequest(SocketAddress addr, StatusCode code, long startTimeNanos) {
        clientStatsLogger.completeProxyRequest(code, startTimeNanos);
        if (enableRegionStats && null != addr) {
            getRegionClientStatsLogger(addr).completeProxyRequest(code, startTimeNanos);
        }
    }

    public void failProxyRequest(SocketAddress addr, Throwable cause, long startTimeNanos) {
        clientStatsLogger.failProxyRequest(cause, startTimeNanos);
        if (enableRegionStats && null != addr) {
            getRegionClientStatsLogger(addr).failProxyRequest(cause, startTimeNanos);
        }
    }
}
