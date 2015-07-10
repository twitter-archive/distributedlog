package com.twitter.distributedlog.client.routing;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.client.resolver.TwitterRegionResolver;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.NoBrokersAvailableException;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestRegionsRoutingService {

    @Test(timeout = 60000)
    public void testRoutingListener() throws Exception {
        int numRoutingServices = 5;
        RoutingService.Builder[] routingServiceBuilders = new RoutingService.Builder[numRoutingServices];
        Set<SocketAddress> hosts = new HashSet<SocketAddress>();
        Map<SocketAddress, String> regionMap = new HashMap<SocketAddress, String>();
        for (int i = 0; i < numRoutingServices; i++) {
            String finagleNameStr = "inet!127.0.0.1:" + (3181 + i);
            routingServiceBuilders[i] = RoutingUtils.buildRoutingService(finagleNameStr);
            SocketAddress address = new InetSocketAddress("127.0.0.1", 3181 + i);
            hosts.add(address);
            regionMap.put(address, "region-" + i);
        }

        final CountDownLatch doneLatch = new CountDownLatch(numRoutingServices);
        final AtomicInteger numHostsLeft = new AtomicInteger(0);
        final Set<SocketAddress> jointHosts = new HashSet<SocketAddress>();
        RegionsRoutingService regionsRoutingService =
                RegionsRoutingService.newBuilder()
                    .routingServiceBuilders(routingServiceBuilders)
                    .resolver(new TwitterRegionResolver(regionMap))
                    .build();
        regionsRoutingService.registerListener(new RoutingService.RoutingListener() {
            @Override
            public void onServerLeft(SocketAddress address) {
                numHostsLeft.incrementAndGet();
            }

            @Override
            public void onServerJoin(SocketAddress address) {
                jointHosts.add(address);
                doneLatch.countDown();
            }
        });

        regionsRoutingService.startService();

        doneLatch.await();

        assertEquals(numRoutingServices, jointHosts.size());
        assertEquals(0, numHostsLeft.get());
        assertTrue(Sets.difference(hosts, jointHosts).immutableCopy().isEmpty());
    }

    @Test(timeout = 60000)
    public void testGetHost() throws Exception {
        int numRoutingServices = 3;
        RoutingService.Builder[] routingServiceBuilders = new RoutingService.Builder[numRoutingServices];
        Map<SocketAddress, String> regionMap = new HashMap<SocketAddress, String>();
        for (int i = 0; i < numRoutingServices; i++) {
            String finagleNameStr = "inet!127.0.0.1:" + (3181 + i);
            routingServiceBuilders[i] = RoutingUtils.buildRoutingService(finagleNameStr);
            SocketAddress address = new InetSocketAddress("127.0.0.1", 3181 + i);
            regionMap.put(address, "region-" + i);
        }

        RegionsRoutingService regionsRoutingService =
                RegionsRoutingService.newBuilder()
                    .resolver(new TwitterRegionResolver(regionMap))
                    .routingServiceBuilders(routingServiceBuilders)
                    .build();
        regionsRoutingService.startService();

        RoutingService.RoutingContext routingContext =
                RoutingService.RoutingContext.of(new TwitterRegionResolver())
                        .addTriedHost(new InetSocketAddress("127.0.0.1", 3183), StatusCode.WRITE_EXCEPTION);
        assertEquals(new InetSocketAddress("127.0.0.1", 3181),
                regionsRoutingService.getHost("any", routingContext));

        routingContext =
                RoutingService.RoutingContext.of(new TwitterRegionResolver())
                        .addTriedHost(new InetSocketAddress("127.0.0.1", 3181), StatusCode.WRITE_EXCEPTION);
        assertEquals(new InetSocketAddress("127.0.0.1", 3182),
                regionsRoutingService.getHost("any", routingContext));

        // add 3182 to routing context as tried host
        routingContext.addTriedHost(new InetSocketAddress("127.0.0.1", 3182), StatusCode.WRITE_EXCEPTION);
        assertEquals(new InetSocketAddress("127.0.0.1", 3183),
                regionsRoutingService.getHost("any", routingContext));

        // add 3183 to routing context as tried host
        routingContext.addTriedHost(new InetSocketAddress("127.0.0.1", 3183), StatusCode.WRITE_EXCEPTION);
        try {
            regionsRoutingService.getHost("any", routingContext);
            fail("Should fail to get host since all regions are tried.");
        } catch (NoBrokersAvailableException nbae) {
            // expected
        }
    }

}
