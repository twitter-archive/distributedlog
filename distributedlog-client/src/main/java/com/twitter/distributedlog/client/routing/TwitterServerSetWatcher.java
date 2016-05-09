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

import com.google.common.collect.ImmutableSet;

import com.google.common.collect.Sets;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import java.net.InetSocketAddress;
import java.util.Set;

public class TwitterServerSetWatcher implements ServerSetWatcher {
    private final ServerSet serverSet;
    private final boolean resolvedFromName;

    public TwitterServerSetWatcher(ServerSet serverSet,
                                   boolean resolvedFromName) {
        this.serverSet = serverSet;
        this.resolvedFromName = resolvedFromName;
    }

    /**
     * Registers a monitor to receive change notices for this server set as long as this jvm process
     * is alive.  Blocks until the initial server set can be gathered and delivered to the monitor.
     * The monitor will be notified if the membership set or parameters of existing members have
     * changed.
     *
     * @param monitor the server set monitor to call back when the host set changes
     * @throws MonitorException if there is a problem monitoring the host set
     */
    public void watch(final ServerSetMonitor monitor)
            throws MonitorException {
        try {
            serverSet.watch(new DynamicHostSet.HostChangeMonitor<ServiceInstance>() {
                @Override
                public void onChange(ImmutableSet<ServiceInstance> serviceInstances) {
                    Set<DLSocketAddress> dlServers = Sets.newHashSet();
                    for (ServiceInstance serviceInstance : serviceInstances) {
                        Endpoint endpoint = serviceInstance.getAdditionalEndpoints().get("thrift");
                        InetSocketAddress inetAddr =
                                new InetSocketAddress(endpoint.getHost(), endpoint.getPort());
                        int shardId = resolvedFromName ? -1 : serviceInstance.getShard();
                        DLSocketAddress address = new DLSocketAddress(shardId, inetAddr);
                        dlServers.add(address);
                    }
                    monitor.onChange(ImmutableSet.copyOf(dlServers));
                }
            });
        } catch (DynamicHostSet.MonitorException me) {
            throw new MonitorException("Failed to monitor server set : ", me);
        }
    }

}
