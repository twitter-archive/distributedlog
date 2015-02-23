package com.twitter.distributedlog.service;

import com.twitter.finagle.ChannelWriteException;
import com.twitter.finagle.NoBrokersAvailableException;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
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
                .blackoutSeconds(5)
                .build();

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 3181);
        List<SocketAddress> addresses = new ArrayList<SocketAddress>(1);
        addresses.add(address);
        name.changeAddrs(addresses);

        routingService.startService();

        String streamName = "test-blackout-host";
        assertEquals(address, routingService.getHost(streamName, null));
        routingService.removeHost(address, new ChannelWriteException(new IOException("test exception")));
        try {
            routingService.getHost(streamName, null);
            fail("Should fail to get host since no brokers are available");
        } catch (NoBrokersAvailableException nbae) {
            // expected
        }

        TimeUnit.SECONDS.sleep(8);
        assertEquals(address, routingService.getHost(streamName, null));

        routingService.stopService();
    }
}
