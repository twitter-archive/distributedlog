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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.stats.StatsReceiver;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

class SingleHostRoutingService implements RoutingService {

    @Deprecated
    public static SingleHostRoutingService of(SocketAddress address) {
        return new SingleHostRoutingService(address);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements RoutingService.Builder {

        private SocketAddress _address;

        private Builder() {}

        public Builder address(SocketAddress address) {
            this._address = address;
            return this;
        }

        @Override
        public RoutingService.Builder statsReceiver(StatsReceiver statsReceiver) {
            return this;
        }

        @Override
        public RoutingService build() {
            Preconditions.checkNotNull(_address, "Host is null");
            return new SingleHostRoutingService(_address);
        }
    }

    private final SocketAddress address;
    private final CopyOnWriteArraySet<RoutingListener> listeners =
            new CopyOnWriteArraySet<RoutingListener>();

    SingleHostRoutingService(SocketAddress address) {
        this.address = address;
    }

    @Override
    public Set<SocketAddress> getHosts() {
        return Sets.newHashSet(address);
    }

    @Override
    public void startService() {
        // no-op
        for (RoutingListener listener : listeners) {
            listener.onServerJoin(address);
        }
    }

    @Override
    public void stopService() {
        // no-op
    }

    @Override
    public RoutingService registerListener(RoutingListener listener) {
        listeners.add(listener);
        return this;
    }

    @Override
    public RoutingService unregisterListener(RoutingListener listener) {
        listeners.remove(listener);
        return null;
    }

    @Override
    public SocketAddress getHost(String key, RoutingContext rContext)
            throws NoBrokersAvailableException {
        if (rContext.isTriedHost(address)) {
            throw new NoBrokersAvailableException("No hosts is available : routing context = " + rContext);
        }
        return address;
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        // no-op
    }
}
