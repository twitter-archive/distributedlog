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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.twitter.distributedlog.client.resolver.DefaultRegionResolver;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.finagle.ChannelWriteException;
import com.twitter.finagle.Address;
import com.twitter.finagle.Addresses;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.stats.NullStatsReceiver;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestConsistentHashRoutingService {

    @Test(timeout = 60000)
    public void testBlackoutHost() throws Exception {
        TestName name = new TestName();
        RoutingService routingService = ConsistentHashRoutingService.newBuilder()
                .serverSet(new NameServerSet(name))
                .resolveFromName(true)
                .numReplicas(997)
                .blackoutSeconds(2)
                .build();

        InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", 3181);
        Address address = Addresses.newInetAddress(inetAddress);
        List<Address> addresses = new ArrayList<Address>(1);
        addresses.add(address);
        name.changeAddrs(addresses);

        routingService.startService();

        RoutingService.RoutingContext routingContext =
                RoutingService.RoutingContext.of(new DefaultRegionResolver());

        String streamName = "test-blackout-host";
        assertEquals(inetAddress, routingService.getHost(streamName, routingContext));
        routingService.removeHost(inetAddress, new ChannelWriteException(new IOException("test exception")));
        try {
            routingService.getHost(streamName, routingContext);
            fail("Should fail to get host since no brokers are available");
        } catch (NoBrokersAvailableException nbae) {
            // expected
        }

        TimeUnit.SECONDS.sleep(3);
        assertEquals(inetAddress, routingService.getHost(streamName, routingContext));

        routingService.stopService();
    }

    @Test(timeout = 60000)
    public void testPerformServerSetChangeOnName() throws Exception {
        TestName name = new TestName();
        ConsistentHashRoutingService routingService = (ConsistentHashRoutingService)
                ConsistentHashRoutingService.newBuilder()
                        .serverSet(new NameServerSet(name))
                        .resolveFromName(true)
                        .numReplicas(997)
                        .build();

        int basePort = 3180;
        int numHosts = 4;
        List<Address> addresses1 = Lists.newArrayListWithExpectedSize(4);
        List<Address> addresses2 = Lists.newArrayListWithExpectedSize(4);
        List<Address> addresses3 = Lists.newArrayListWithExpectedSize(4);

        // fill up the addresses1
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + i);
            Address address = Addresses.newInetAddress(inetAddress);
            addresses1.add(address);
        }
        // fill up the addresses2 - overlap with addresses1
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + 2 + i);
            Address address = Addresses.newInetAddress(inetAddress);
            addresses2.add(address);
        }
        // fill up the addresses3 - not overlap with addresses2
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + 10 + i);
            Address address = Addresses.newInetAddress(inetAddress);
            addresses3.add(address);
        }

        final List<SocketAddress> leftAddresses = Lists.newArrayList();
        final List<SocketAddress> joinAddresses = Lists.newArrayList();

        RoutingService.RoutingListener routingListener = new RoutingService.RoutingListener() {
            @Override
            public void onServerLeft(SocketAddress address) {
                synchronized (leftAddresses) {
                    leftAddresses.add(address);
                    leftAddresses.notifyAll();
                }
            }

            @Override
            public void onServerJoin(SocketAddress address) {
                synchronized (joinAddresses) {
                    joinAddresses.add(address);
                    joinAddresses.notifyAll();
                }
            }
        };

        routingService.registerListener(routingListener);
        name.changeAddrs(addresses1);

        routingService.startService();

        synchronized (joinAddresses) {
            while (joinAddresses.size() < numHosts) {
                joinAddresses.wait();
            }
        }

        // validate 4 nodes joined
        synchronized (joinAddresses) {
            assertEquals(numHosts, joinAddresses.size());
        }
        synchronized (leftAddresses) {
            assertEquals(0, leftAddresses.size());
        }
        assertEquals(numHosts, routingService.shardId2Address.size());
        assertEquals(numHosts, routingService.address2ShardId.size());
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }

        // update addresses2 - 2 new hosts joined, 2 old hosts left
        name.changeAddrs(addresses2);
        synchronized (joinAddresses) {
            while (joinAddresses.size() < numHosts + 2) {
                joinAddresses.wait();
            }
        }
        synchronized (leftAddresses) {
            while (leftAddresses.size() < numHosts - 2) {
                leftAddresses.wait();
            }
        }
        assertEquals(numHosts, routingService.shardId2Address.size());
        assertEquals(numHosts, routingService.address2ShardId.size());

        // first 2 shards should leave
        for (int i = 0; i < 2; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + i);
            assertFalse(routingService.address2ShardId.containsKey(inetAddress));
        }

        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + 2 + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }

        // update addresses3 - 2 new hosts joined, 2 old hosts left
        name.changeAddrs(addresses3);
        synchronized (joinAddresses) {
            while (joinAddresses.size() < numHosts + 2 + numHosts) {
                joinAddresses.wait();
            }
        }
        synchronized (leftAddresses) {
            while (leftAddresses.size() < numHosts - 2 + numHosts) {
                leftAddresses.wait();
            }
        }
        assertEquals(numHosts, routingService.shardId2Address.size());
        assertEquals(numHosts, routingService.address2ShardId.size());

        // first 6 shards should leave
        for (int i = 0; i < 2 + numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + i);
            assertFalse(routingService.address2ShardId.containsKey(inetAddress));
        }
        // new 4 shards should exist
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + 10 + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }

    }

    private static class TestServerSetWatcher implements ServerSetWatcher {

        final LinkedBlockingQueue<ImmutableSet<DLSocketAddress>> changeQueue =
                new LinkedBlockingQueue<ImmutableSet<DLSocketAddress>>();
        final CopyOnWriteArrayList<ServerSetMonitor> monitors =
                new CopyOnWriteArrayList<ServerSetMonitor>();

        @Override
        public void watch(ServerSetMonitor monitor) throws MonitorException {
            monitors.add(monitor);
            ImmutableSet<DLSocketAddress> change;
            while ((change = changeQueue.poll()) != null) {
                notifyChanges(change);
            }
        }

        void notifyChanges(ImmutableSet<DLSocketAddress> addresses) {
            if (monitors.isEmpty()) {
                changeQueue.add(addresses);
            } else {
                for (ServerSetMonitor monitor : monitors) {
                    monitor.onChange(addresses);
                }
            }
        }
    }

    @Test(timeout = 60000)
    public void testPerformServerSetChangeOnServerSet() throws Exception {
        TestServerSetWatcher serverSetWatcher = new TestServerSetWatcher();
        ConsistentHashRoutingService routingService = new ConsistentHashRoutingService(
                serverSetWatcher, 997, Integer.MAX_VALUE, NullStatsReceiver.get());

        int basePort = 3180;
        int numHosts = 4;
        Set<DLSocketAddress> addresses1 = Sets.newConcurrentHashSet();
        Set<DLSocketAddress> addresses2 = Sets.newConcurrentHashSet();
        Set<DLSocketAddress> addresses3 = Sets.newConcurrentHashSet();

        // fill up the addresses1
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + i);
            DLSocketAddress dsa = new DLSocketAddress(i, inetAddress);
            addresses1.add(dsa);
        }
        // fill up the addresses2 - overlap with addresses1
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + numHosts + i);
            DLSocketAddress dsa = new DLSocketAddress(i+2, inetAddress);
            addresses2.add(dsa);
        }
        // fill up the addresses3 - not overlap with addresses2
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + 10 + i);
            DLSocketAddress dsa = new DLSocketAddress(i, inetAddress);
            addresses3.add(dsa);
        }

        final List<SocketAddress> leftAddresses = Lists.newArrayList();
        final List<SocketAddress> joinAddresses = Lists.newArrayList();

        RoutingService.RoutingListener routingListener = new RoutingService.RoutingListener() {
            @Override
            public void onServerLeft(SocketAddress address) {
                synchronized (leftAddresses) {
                    leftAddresses.add(address);
                    leftAddresses.notifyAll();
                }
            }

            @Override
            public void onServerJoin(SocketAddress address) {
                synchronized (joinAddresses) {
                    joinAddresses.add(address);
                    joinAddresses.notifyAll();
                }
            }
        };

        routingService.registerListener(routingListener);
        serverSetWatcher.notifyChanges(ImmutableSet.copyOf(addresses1));

        routingService.startService();

        synchronized (joinAddresses) {
            while (joinAddresses.size() < numHosts) {
                joinAddresses.wait();
            }
        }

        // validate 4 nodes joined
        synchronized (joinAddresses) {
            assertEquals(numHosts, joinAddresses.size());
        }
        synchronized (leftAddresses) {
            assertEquals(0, leftAddresses.size());
        }
        assertEquals(numHosts, routingService.shardId2Address.size());
        assertEquals(numHosts, routingService.address2ShardId.size());
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            assertEquals(i, shardId);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }

        // update addresses2 - 2 new hosts joined, 2 old hosts left
        serverSetWatcher.notifyChanges(ImmutableSet.copyOf(addresses2));
        synchronized (joinAddresses) {
            while (joinAddresses.size() < numHosts + 2) {
                joinAddresses.wait();
            }
        }
        synchronized (leftAddresses) {
            while (leftAddresses.size() < 2) {
                leftAddresses.wait();
            }
        }

        assertEquals(numHosts + 2, routingService.shardId2Address.size());
        assertEquals(numHosts + 2, routingService.address2ShardId.size());
        // first 2 shards should not leave
        for (int i = 0; i < 2; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            assertEquals(i, shardId);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + numHosts + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            assertEquals(i+2, shardId);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }

        // update addresses3
        serverSetWatcher.notifyChanges(ImmutableSet.copyOf(addresses3));
        synchronized (joinAddresses) {
            while (joinAddresses.size() < numHosts + 2 + numHosts) {
                joinAddresses.wait();
            }
        }
        synchronized (leftAddresses) {
            while (leftAddresses.size() < 2 + numHosts) {
                leftAddresses.wait();
            }
        }
        assertEquals(numHosts + 2, routingService.shardId2Address.size());
        assertEquals(numHosts + 2, routingService.address2ShardId.size());

        // first 4 shards should leave
        for (int i = 0; i < numHosts; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + 10 + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            assertEquals(i, shardId);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }
        // the other 2 shards should be still there
        for (int i = 0; i < 2; i++) {
            InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", basePort + numHosts + 2 + i);
            assertTrue(routingService.address2ShardId.containsKey(inetAddress));
            int shardId = routingService.address2ShardId.get(inetAddress);
            assertEquals(numHosts + i, shardId);
            SocketAddress sa = routingService.shardId2Address.get(shardId);
            assertNotNull(sa);
            assertEquals(inetAddress, sa);
        }

    }
}
