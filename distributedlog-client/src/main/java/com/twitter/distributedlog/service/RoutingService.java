package com.twitter.distributedlog.service;

import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Routing Service provides mechanism how to route requests.
 */
public interface RoutingService {

    public static interface Builder {

        /**
         * @return built routing service
         */
        RoutingService build();

    }

    public static interface RoutingListener {
        void onServerLeft(SocketAddress address);
        void onServerJoin(SocketAddress address);
    }

    public static class RoutingContext {

        public static RoutingContext of(RegionResolver resolver) {
            return new RoutingContext(resolver);
        }

        final RegionResolver regionResolver;
        final Map<SocketAddress, StatusCode> triedHosts;
        final Set<String> unavailableRegions;

        private RoutingContext(RegionResolver regionResolver) {
            this.regionResolver = regionResolver;
            this.triedHosts = new HashMap<SocketAddress, StatusCode>();
            this.unavailableRegions = new HashSet<String>();
        }

        @Override
        public synchronized String toString() {
            return "(tried hosts=" + triedHosts + ")";
        }

        public synchronized RoutingContext addTriedHost(SocketAddress socketAddress, StatusCode code) {
            this.triedHosts.put(socketAddress, code);
            if (StatusCode.REGION_UNAVAILABLE == code) {
                unavailableRegions.add(regionResolver.resolveRegion(socketAddress));
            }
            return this;
        }

        public synchronized boolean isTriedHost(SocketAddress address) {
            return this.triedHosts.containsKey(address);
        }

        public synchronized boolean hasUnavailableRegions() {
            return !unavailableRegions.isEmpty();
        }

        public synchronized boolean isUnavailableRegion(String region) {
            return unavailableRegions.contains(region);
        }

    }

    void startService();

    void stopService();

    RoutingService registerListener(RoutingListener listener);

    RoutingService unregisterListener(RoutingListener listener);

    SocketAddress getHost(String key, RoutingContext rContext)
            throws NoBrokersAvailableException;

    void removeHost(SocketAddress address, Throwable reason);
}
