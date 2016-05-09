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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.twitter.distributedlog.client.resolver.DefaultRegionResolver;
import com.twitter.finagle.Address;
import com.twitter.finagle.Addresses;
import com.twitter.finagle.addr.WeightedAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestRoutingService {

    static final Logger LOG = LoggerFactory.getLogger(TestRoutingService.class);

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();
        for (int i = 0; i <= 1; i++) {
            for (int j = 0; j <= 1; j++) {
                for (int k = 0; k <= 1; k++) {
                    list.add(new Boolean[] {i == 1, j == 1, k == 1});
                }
            }
        }
        return list;
    }

    final private boolean consistentHash;
    final private boolean weightedAddresses;
    final private boolean asyncResolution;

    public TestRoutingService(boolean consistentHash, boolean weightedAddresses, boolean asyncResolution) {
        this.consistentHash = consistentHash;
        this.weightedAddresses = weightedAddresses;
        this.asyncResolution = asyncResolution;
    }

    private List<Address> getAddresses(boolean weightedAddresses) {
        ArrayList<Address> addresses = new ArrayList<Address>();
        addresses.add(Addresses.newInetAddress(new InetSocketAddress("127.0.0.1", 3181)));
        addresses.add(Addresses.newInetAddress(new InetSocketAddress("127.0.0.2", 3181)));
        addresses.add(Addresses.newInetAddress(new InetSocketAddress("127.0.0.3", 3181)));
        addresses.add(Addresses.newInetAddress(new InetSocketAddress("127.0.0.4", 3181)));
        addresses.add(Addresses.newInetAddress(new InetSocketAddress("127.0.0.5", 3181)));
        addresses.add(Addresses.newInetAddress(new InetSocketAddress("127.0.0.6", 3181)));
        addresses.add(Addresses.newInetAddress(new InetSocketAddress("127.0.0.7", 3181)));

        if (weightedAddresses) {
            ArrayList<Address> wAddresses = new ArrayList<Address>();
            for (Address address: addresses) {
                wAddresses.add(WeightedAddress.apply(address, 1.0));
            }
            return wAddresses;
        } else {
            return addresses;
        }
    }

    private void testRoutingServiceHelper(boolean consistentHash, boolean weightedAddresses, boolean asyncResolution) throws Exception {
        ExecutorService executorService = null;
        final List<Address> addresses = getAddresses(weightedAddresses);
        final TestName name = new TestName();
        RoutingService routingService;
        if (consistentHash) {
            routingService = ConsistentHashRoutingService.newBuilder()
                    .serverSet(new NameServerSet(name))
                    .resolveFromName(true)
                    .numReplicas(997)
                    .build();
        } else {
            routingService = ServerSetRoutingService.newServerSetRoutingServiceBuilder()
                    .serverSetWatcher(new TwitterServerSetWatcher(new NameServerSet(name), true)).build();
        }

        if (asyncResolution) {
            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    name.changeAddrs(addresses);
                }
            });
        } else {
            name.changeAddrs(addresses);
        }
        routingService.startService();

        HashSet<SocketAddress> mapping = new HashSet<SocketAddress>();

        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 5; j++) {
                String stream = "TestStream-" + i + "-" + j;
                mapping.add(routingService.getHost(stream,
                        RoutingService.RoutingContext.of(new DefaultRegionResolver())));
            }
        }

        assert(mapping.size() == addresses.size());

        if (null != executorService) {
            executorService.shutdown();
        }

    }

    @Test(timeout = 5000)
    public void testRoutingService() throws Exception {
        testRoutingServiceHelper(this.consistentHash, this.weightedAddresses, this.asyncResolution);
    }
}
