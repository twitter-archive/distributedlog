package com.twitter.distributedlog.service.stats;

import com.twitter.distributedlog.service.RegionResolver;
import com.twitter.distributedlog.service.TwitterRegionResolver;
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

    public ClientStats(StatsReceiver statsReceiver, boolean enableRegionStats) {
        this.statsReceiver = statsReceiver;
        this.clientStatsLogger = new ClientStatsLogger(statsReceiver);
        this.enableRegionStats = enableRegionStats;
        this.regionClientStatsLoggers = new ConcurrentHashMap<String, ClientStatsLogger>();
        this.regionResolver = new TwitterRegionResolver();
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

    public void completeRequest(SocketAddress addr, long micros, int numTries) {
        clientStatsLogger.completeRequest(micros, numTries);
        if (enableRegionStats && null != addr) {
            getRegionClientStatsLogger(addr).completeRequest(micros, numTries);
        }
    }

    public void failRequest(SocketAddress addr, long micros, int numTries) {
        clientStatsLogger.failRequest(micros, numTries);
        if (enableRegionStats && null != addr) {
            getRegionClientStatsLogger(addr).failRequest(micros, numTries);
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
