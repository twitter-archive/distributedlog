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

import com.google.common.collect.Sets;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.stats.StatsReceiver;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalRoutingService implements RoutingService {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements RoutingService.Builder {

        private Builder() {}

        @Override
        public RoutingService.Builder statsReceiver(StatsReceiver statsReceiver) {
            return this;
        }

        @Override
        public LocalRoutingService build() {
            return new LocalRoutingService();
        }
    }

    private final Map<String, LinkedHashSet<SocketAddress>> localAddresses =
            new HashMap<String, LinkedHashSet<SocketAddress>>();
    private final CopyOnWriteArrayList<RoutingListener> listeners =
            new CopyOnWriteArrayList<RoutingListener>();

    boolean allowRetrySameHost = true;

    @Override
    public void startService() {
        // nop
    }

    @Override
    public void stopService() {
        // nop
    }

    @Override
    public synchronized Set<SocketAddress> getHosts() {
        Set<SocketAddress> hosts = Sets.newHashSet();
        for (LinkedHashSet<SocketAddress> addresses : localAddresses.values()) {
            hosts.addAll(addresses);
        }
        return hosts;
    }

    @Override
    public RoutingService registerListener(RoutingListener listener) {
        listeners.add(listener);
        return this;
    }

    @Override
    public RoutingService unregisterListener(RoutingListener listener) {
        listeners.remove(listener);
        return this;
    }

    public LocalRoutingService setAllowRetrySameHost(boolean enabled) {
        allowRetrySameHost = enabled;
        return this;
    }

    public void addHost(String stream, SocketAddress address) {
        boolean notify = false;
        synchronized (this) {
            LinkedHashSet<SocketAddress> addresses = localAddresses.get(stream);
            if (null == addresses) {
                addresses = new LinkedHashSet<SocketAddress>();
                localAddresses.put(stream, addresses);
            }
            if (addresses.add(address)) {
                notify = true;
            }
        }
        if (notify) {
            for (RoutingListener listener : listeners) {
                listener.onServerJoin(address);
            }
        }
    }

    @Override
    public synchronized SocketAddress getHost(String key, RoutingContext rContext)
            throws NoBrokersAvailableException {
        LinkedHashSet<SocketAddress> addresses = localAddresses.get(key);

        SocketAddress candidate = null;
        if (null != addresses) {
            for (SocketAddress host : addresses) {
                if (rContext.isTriedHost(host) && !allowRetrySameHost) {
                    continue;
                } else {
                    candidate = host;
                    break;
                }
            }
        }
        if (null != candidate) {
            return candidate;
        }
        throw new NoBrokersAvailableException("No host available");
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        // nop
    }
}
