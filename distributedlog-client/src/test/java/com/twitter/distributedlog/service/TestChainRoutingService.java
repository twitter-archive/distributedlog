package com.twitter.distributedlog.service;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestChainRoutingService {

    @Test(timeout = 60000)
    public void testRoutingListener() throws Exception {
        int numRoutingServices = 5;
        RoutingService[] routingServices = new RoutingService[numRoutingServices];
        Set<SocketAddress> hosts = new HashSet<SocketAddress>();
        for (int i = 0; i < numRoutingServices; i++) {
            String finagleNameStr = "inet!127.0.0.1:" + (3181 + i);
            routingServices[i] = DistributedLogClientBuilder.buildRoutingService(finagleNameStr);
            hosts.add(new InetSocketAddress("127.0.0.1", 3181 + i));
        }

        final CountDownLatch doneLatch = new CountDownLatch(numRoutingServices);
        final AtomicInteger numHostsLeft = new AtomicInteger(0);
        final Set<SocketAddress> jointHosts = new HashSet<SocketAddress>();
        ChainRoutingService chainRoutingService = ChainRoutingService.of(routingServices);
        chainRoutingService.registerListener(new RoutingService.RoutingListener() {
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

        chainRoutingService.startService();

        doneLatch.await();

        assertEquals(numRoutingServices, jointHosts.size());
        assertEquals(0, numHostsLeft.get());
        assertTrue(Sets.difference(hosts, jointHosts).immutableCopy().isEmpty());
    }

    @Test(timeout = 60000)
    public void testGetHost() throws Exception {
        int numRoutingServices = 3;
        RoutingService[] routingServices = new RoutingService[numRoutingServices];
        for (int i = 0; i < numRoutingServices; i++) {
            String finagleNameStr = "inet!127.0.0.1:" + (3181 + i);
            routingServices[i] = DistributedLogClientBuilder.buildRoutingService(finagleNameStr);
        }

        ChainRoutingService chainRoutingService = ChainRoutingService.of(routingServices);
        chainRoutingService.startService();

        assertEquals(new InetSocketAddress("127.0.0.1", 3181),
                chainRoutingService.getHost("any", new InetSocketAddress("127.0.0.1", 3183)));
        assertEquals(new InetSocketAddress("127.0.0.1", 3182),
                chainRoutingService.getHost("any", new InetSocketAddress("127.0.0.1", 3181)));
    }

}
