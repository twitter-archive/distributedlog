package com.twitter.distributedlog.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.finagle.WeightedSocketAddress;

public class TestRoutingService {

    static final Logger LOG = LoggerFactory.getLogger(TestRoutingService.class);

    private List<SocketAddress> getAddresses(boolean weightedAddresses) {
        ArrayList<SocketAddress> addresses = new ArrayList<SocketAddress>();
        addresses.add(new InetSocketAddress("127.0.0.1", 3181));
        addresses.add(new InetSocketAddress("127.0.0.2", 3181));
        addresses.add(new InetSocketAddress("127.0.0.3", 3181));
        addresses.add(new InetSocketAddress("127.0.0.4", 3181));
        addresses.add(new InetSocketAddress("127.0.0.5", 3181));
        addresses.add(new InetSocketAddress("127.0.0.6", 3181));
        addresses.add(new InetSocketAddress("127.0.0.7", 3181));

        if (weightedAddresses) {
            ArrayList<SocketAddress> wAddresses = new ArrayList<SocketAddress>();
            for (SocketAddress address: addresses) {
                wAddresses.add(WeightedSocketAddress.apply(address, 1.0));
            }
            return wAddresses;
        } else {
            return addresses;
        }
    }

    private void testRoutingServiceHelper(boolean consistentHash, boolean weightedAddresses) throws Exception {
        final List<SocketAddress> addresses = getAddresses(weightedAddresses);
        TestName name = new TestName();
        RoutingService routingService;
        if (consistentHash) {
            routingService = ConsistentHashRoutingService.of(new DLServerSetWatcher(new NameServerSet(name), true),
                997);
        } else {
            routingService = new ServerSetRoutingService(new DLServerSetWatcher(new NameServerSet(name), true));
        }
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
    public void testServerSetRoutingService() throws Exception {
        testRoutingServiceHelper(false, false);
    }

    @Test
    public void testServerSetRoutingServiceWeightedAddresses() throws Exception {
        testRoutingServiceHelper(false, true);
    }


    @Test
    public void testConsistentHashRoutingService() throws Exception {
        testRoutingServiceHelper(true, false);
    }

    @Test
    public void testConsistentHashRoutingServiceWeightedAddresses() throws Exception {
        testRoutingServiceHelper(true, true);
    }
}
