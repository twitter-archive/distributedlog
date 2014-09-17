package com.twitter.distributedlog.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRoutingService {

    static final Logger LOG = LoggerFactory.getLogger(TestRoutingService.class);

    private ArrayList<SocketAddress> getAddresses() {
        ArrayList<SocketAddress> addresses = new ArrayList<SocketAddress>();
        addresses.add(new InetSocketAddress("127.0.0.1", 3181));
        addresses.add(new InetSocketAddress("127.0.0.2", 3181));
        addresses.add(new InetSocketAddress("127.0.0.3", 3181));
        addresses.add(new InetSocketAddress("127.0.0.4", 3181));
        addresses.add(new InetSocketAddress("127.0.0.5", 3181));
        addresses.add(new InetSocketAddress("127.0.0.6", 3181));
        addresses.add(new InetSocketAddress("127.0.0.7", 3181));
        return addresses;
    }

    @Test
    public void testServerSetRoutingService() throws Exception {
        final ArrayList<SocketAddress> addresses = getAddresses();
        TestName name = new TestName();
        ServerSetRoutingService routingService = new ServerSetRoutingService(new DLServerSetWatcher(new NameServerSet(name), true));
        routingService.startService();

        name.changeAddrs(addresses);

        HashSet<SocketAddress> mapping = new HashSet<SocketAddress>();

        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 5; j++) {
                String stream = "TestStream-" + i + "-" + j;
                mapping.add(routingService.getHost(stream, null));
            }
        }

        assert(mapping.size() == addresses.size());

    }

    @Test
    public void testConsistentHashRoutingService() throws Exception {
        final ArrayList<SocketAddress> addresses = getAddresses();
        TestName name = new TestName();
        ConsistentHashRoutingService routingService = ConsistentHashRoutingService.of(new DLServerSetWatcher(new NameServerSet(name), true),
            997);
        routingService.startService();

        name.changeAddrs(addresses);

        HashSet<SocketAddress> mapping = new HashSet<SocketAddress>();

        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 5; j++) {
                String stream = "TestStream-" + i + "-" + j;
                mapping.add(routingService.getHost(stream, null));
            }
        }

        assert(mapping.size() == addresses.size());

    }
}
