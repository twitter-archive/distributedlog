package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogServerTestCase;
import com.twitter.distributedlog.service.MonitorServiceClient;
import com.twitter.util.Await;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestClusterBalancer extends DistributedLogServerTestCase {

    static final Logger logger = LoggerFactory.getLogger(TestClusterBalancer.class);

    private final int numServers = 5;
    private final List<DLServer> cluster;
    private DLClient client;

    public TestClusterBalancer() {
        this.cluster = new ArrayList<DLServer>();
    }

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        int initPort = 9001;
        for (int i = 0; i < numServers; i++) {
            cluster.add(createDistributedLogServer(initPort + i));
        }
        client = createDistributedLogClient("cluster_client");
    }

    @After
    @Override
    public void teardown() throws Exception {
        super.teardown();
        if (null != client) {
            client.shutdown();
        }
        for (DLServer server: cluster) {
            server.shutdown();
        }
    }

    private void initStreams(String namePrefix) {
        logger.info("Init streams with prefix {}", namePrefix);
        // Stream Distribution: 5, 4, 3, 2, 1
        initStreams(namePrefix, 5, 1, 0);
        initStreams(namePrefix, 4, 6, 1);
        initStreams(namePrefix, 3, 10, 2);
        initStreams(namePrefix, 2, 13, 3);
        initStreams(namePrefix, 1, 15, 4);
    }

    private void initStreams(String namePrefix, int numStreams, int streamId, int proxyId) {
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + (streamId++);
            client.routingService.addHost(name, cluster.get(proxyId).getAddress());
        }
    }

    private void writeStreams(String namePrefix) throws Exception {
        logger.info("Write streams with prefix {}", namePrefix);
        writeStreams(namePrefix, 5, 1);
        writeStreams(namePrefix, 4, 6);
        writeStreams(namePrefix, 3, 10);
        writeStreams(namePrefix, 2, 13);
        writeStreams(namePrefix, 1, 15);
    }

    private void writeStreams(String namePrefix, int numStreams, int streamId) throws Exception {
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + (streamId++);
            try {
                Await.result(((DistributedLogClient) client.dlClient).write(name, ByteBuffer.wrap(name.getBytes(UTF_8))));
            } catch (Exception e) {
                logger.error("Error writing stream {} : ", name, e);
                throw e;
            }
        }
    }

    private void validateStreams(String namePrefix) throws Exception {
        logger.info("Validate streams with prefix {}", namePrefix);
        validateStreams(namePrefix, 5, 1, 0);
        validateStreams(namePrefix, 4, 6, 1);
        validateStreams(namePrefix, 3, 10, 2);
        validateStreams(namePrefix, 2, 13, 3);
        validateStreams(namePrefix, 1, 15, 4);
    }

    private void validateStreams(String namePrefix, int numStreams, int streamId, int proxyIdx) {
        Set<String> expectedStreams = new HashSet<String>();
        for (int i = 0; i < numStreams; i++) {
            expectedStreams.add(namePrefix + (streamId++));
        }
        checkStreams(expectedStreams, cluster.get(proxyIdx));
    }

    @Test(timeout = 60000)
    public void testBalanceAll() throws Exception {
        String namePrefix = "clusterbalancer-balance-all-";

        initStreams(namePrefix);
        writeStreams(namePrefix);
        validateStreams(namePrefix);

        Optional<RateLimiter> rateLimiter = Optional.absent();

        Balancer balancer = new ClusterBalancer(client.dlClientBuilder,
                Pair.of((DistributedLogClient)client.dlClient, (MonitorServiceClient)client.dlClient));
        logger.info("Rebalancing from 'unknown' target");
        try {
            balancer.balanceAll("unknown", 10, rateLimiter);
            fail("Should fail on balanceAll from 'unknown' target.");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        validateStreams(namePrefix);

        logger.info("Rebalancing from 'unexisted' host");
        String addr = DLSocketAddress.toString(DLSocketAddress.getSocketAddress(9999));
        balancer.balanceAll(addr, 10, rateLimiter);
        validateStreams(namePrefix);

        addr = DLSocketAddress.toString(cluster.get(0).getAddress());
        logger.info("Rebalancing from host {}.", addr);
        balancer.balanceAll(addr, 10, rateLimiter);
        checkStreams(0, cluster.get(0));
        checkStreams(4, cluster.get(1));
        checkStreams(3, cluster.get(2));
        checkStreams(4, cluster.get(3));
        checkStreams(4, cluster.get(4));

        addr = DLSocketAddress.toString(cluster.get(2).getAddress());
        logger.info("Rebalancing from host {}.", addr);
        balancer.balanceAll(addr, 10, rateLimiter);
        checkStreams(3, cluster.get(0));
        checkStreams(4, cluster.get(1));
        checkStreams(0, cluster.get(2));
        checkStreams(4, cluster.get(3));
        checkStreams(4, cluster.get(4));

        logger.info("Rebalancing the cluster");
        balancer.balance(0, 0.0f, 10, rateLimiter);
        for (int i = 0; i < 5; i++) {
            checkStreams(3, cluster.get(i));
        }
    }
}
