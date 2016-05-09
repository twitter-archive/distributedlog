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
package com.twitter.distributedlog.client.routing;

import com.twitter.distributedlog.client.resolver.RegionResolver;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.stats.StatsReceiver;

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
         * Build routing service with stats receiver
         *
         * @param statsReceiver
         *          stats receiver
         * @return built routing service
         */
        Builder statsReceiver(StatsReceiver statsReceiver);

        /**
         * @return built routing service
         */
        RoutingService build();

    }

    /**
     * Listener for server changes on routing service
     */
    public static interface RoutingListener {
        /**
         * Trigger when server left.
         *
         * @param address left server.
         */
        void onServerLeft(SocketAddress address);

        /**
         * Trigger when server joint.
         *
         * @param address joint server.
         */
        void onServerJoin(SocketAddress address);
    }

    /**
     * Routing Context of a request.
     */
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

        /**
         * Add tried host to routing context.
         *
         * @param socketAddress
         *          socket address of tried host.
         * @param code
         *          status code returned from tried host.
         * @return routing context.
         */
        public synchronized RoutingContext addTriedHost(SocketAddress socketAddress, StatusCode code) {
            this.triedHosts.put(socketAddress, code);
            if (StatusCode.REGION_UNAVAILABLE == code) {
                unavailableRegions.add(regionResolver.resolveRegion(socketAddress));
            }
            return this;
        }

        /**
         * Is the host <i>address</i> already tried.
         *
         * @param address
         *          socket address to check
         * @return true if the address is already tried, otherwise false.
         */
        public synchronized boolean isTriedHost(SocketAddress address) {
            return this.triedHosts.containsKey(address);
        }

        /**
         * Whether encountered unavailable regions.
         *
         * @return true if encountered unavailable regions, otherwise false.
         */
        public synchronized boolean hasUnavailableRegions() {
            return !unavailableRegions.isEmpty();
        }

        /**
         * Whether the <i>region</i> is unavailable.
         *
         * @param region
         *          region
         * @return true if the region is unavailable, otherwise false.
         */
        public synchronized boolean isUnavailableRegion(String region) {
            return unavailableRegions.contains(region);
        }

    }

    /**
     * Start routing service.
     */
    void startService();

    /**
     * Stop routing service.
     */
    void stopService();

    /**
     * Register routing listener.
     *
     * @param listener routing listener.
     * @return routing service.
     */
    RoutingService registerListener(RoutingListener listener);

    /**
     * Unregister routing listener.
     *
     * @param listener routing listener.
     * @return routing service.
     */
    RoutingService unregisterListener(RoutingListener listener);

    /**
     * Get all the hosts that available in routing service.
     *
     * @return all the hosts
     */
    Set<SocketAddress> getHosts();

    /**
     * Get the host to route the request by <i>key</i>.
     *
     * @param key
     *          key to route the request.
     * @param rContext
     *          routing context.
     * @return host to route the request
     * @throws NoBrokersAvailableException
     */
    SocketAddress getHost(String key, RoutingContext rContext)
            throws NoBrokersAvailableException;

    /**
     * Remove the host <i>address</i> for a specific <i>reason</i>.
     *
     * @param address
     *          host address to remove
     * @param reason
     *          reason to remove the host
     */
    void removeHost(SocketAddress address, Throwable reason);
}
