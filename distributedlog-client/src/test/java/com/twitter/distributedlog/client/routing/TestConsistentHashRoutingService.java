package com.twitter.distributedlog.client.routing;

import com.twitter.distributedlog.client.resolver.TwitterRegionResolver;
import com.twitter.finagle.ChannelWriteException;
import com.twitter.finagle.Address;
import com.twitter.finagle.Addresses;
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
                .blackoutSeconds(2)
                .build();

        InetSocketAddress inetAddress = new InetSocketAddress("127.0.0.1", 3181);
        Address address = Addresses.newInetAddress(inetAddress);
        List<Address> addresses = new ArrayList<Address>(1);
        addresses.add(address);
        name.changeAddrs(addresses);

        routingService.startService();

        RoutingService.RoutingContext routingContext =
                RoutingService.RoutingContext.of(new TwitterRegionResolver());

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
}
