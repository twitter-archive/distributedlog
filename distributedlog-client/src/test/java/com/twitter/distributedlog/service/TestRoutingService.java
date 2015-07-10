package com.twitter.distributedlog.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.twitter.distributedlog.client.resolver.TwitterRegionResolver;
import com.twitter.finagle.WeightedSocketAddress;
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
                wAddresses.add(new WeightedSocketAddress(address, 1.0));
            }
            return wAddresses;
        } else {
            return addresses;
        }
    }

    private void testRoutingServiceHelper(boolean consistentHash, boolean weightedAddresses, boolean asyncResolution) throws Exception {
        ExecutorService executorService = null;
        final List<SocketAddress> addresses = getAddresses(weightedAddresses);
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
                    .serverSetWatcher(new DLServerSetWatcher(new NameServerSet(name), true)).build();
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
                        RoutingService.RoutingContext.of(new TwitterRegionResolver())));
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
