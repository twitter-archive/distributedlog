package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogServerTestCase;
import com.twitter.util.Await;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Charsets.*;
import static org.junit.Assert.*;

public class TestSimpleBalancer extends DistributedLogServerTestCase {

    static final Logger logger = LoggerFactory.getLogger(TestSimpleBalancer.class);

    DLClient targetClient;
    DLServer targetServer;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        targetServer = createDistributedLogServer(7003);
        targetClient = createDistributedLogClient("target");
    }

    @After
    @Override
    public void teardown() throws Exception {
        super.teardown();
        if (null != targetClient) {
            targetClient.shutdown();
        }
        if (null != targetServer) {
            targetServer.shutdown();
        }
    }

    @Test(timeout = 60000)
    public void testBalanceAll() throws Exception {
        String namePrefix = "simplebalancer-balance-all-";
        int numStreams = 10;

        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            // src client
            dlClient.routingService.addHost(name, dlServer.getAddress());
            // target client
            targetClient.routingService.addHost(name, targetServer.getAddress());
        }

        // write to multiple streams
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            Await.result(((DistributedLogClient) dlClient.dlClient).write(name, ByteBuffer.wrap(("" + i).getBytes(UTF_8))));
        }

        // validation
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            checkStream(name, dlClient, dlServer, 1, numStreams, numStreams, true, true);
            checkStream(name, targetClient, targetServer, 0, 0, 0, false, false);
        }

        Optional<RateLimiter> rateLimiter = Optional.absent();

        Balancer balancer = new SimpleBalancer("source", dlClient.dlClient, dlClient.dlClient,
                                               "target", targetClient.dlClient, targetClient.dlClient);
        logger.info("Rebalancing from 'unknown' target");
        try {
            balancer.balanceAll("unknown", 10, rateLimiter);
            fail("Should fail on balanceAll from 'unknown' target.");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // nothing to balance from 'target'
        logger.info("Rebalancing from 'target' target");
        balancer.balanceAll("target", 1, rateLimiter);
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            checkStream(name, dlClient, dlServer, 1, numStreams, numStreams, true, true);
            checkStream(name, targetClient, targetServer, 0, 0, 0, false, false);
        }

        // balance all streams from 'source'
        logger.info("Rebalancing from 'source' target");
        balancer.balanceAll("source", 10, rateLimiter);
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            checkStream(name, targetClient, targetServer, 1, numStreams, numStreams, true, true);
            checkStream(name, dlClient, dlServer, 0, 0, 0, false, false);
        }
    }

    @Test(timeout = 60000)
    public void testBalanceStreams() throws Exception {
        String namePrefix = "simplebalancer-balance-streams-";
        int numStreams = 10;

        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            // src client
            dlClient.routingService.addHost(name, dlServer.getAddress());
            // target client
            targetClient.routingService.addHost(name, targetServer.getAddress());
        }

        // write to multiple streams
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            Await.result(((DistributedLogClient) dlClient.dlClient).write(name, ByteBuffer.wrap(("" + i).getBytes(UTF_8))));
        }

        // validation
        for (int i = 0; i < numStreams; i++) {
            String name = namePrefix + i;
            checkStream(name, dlClient, dlServer, 1, numStreams, numStreams, true, true);
            checkStream(name, targetClient, targetServer, 0, 0, 0, false, false);
        }

        Optional<RateLimiter> rateLimiter = Optional.absent();

        Balancer balancer = new SimpleBalancer("source", dlClient.dlClient, dlClient.dlClient,
                                               "target", targetClient.dlClient, targetClient.dlClient);

        // balance all streams from 'source'
        logger.info("Rebalancing streams between targets");
        balancer.balance(0, 0, 10, rateLimiter);

        Set<String> sourceStreams = getAllStreamsFromDistribution(getStreamOwnershipDistribution(dlClient));
        Set<String> targetStreams = getAllStreamsFromDistribution(getStreamOwnershipDistribution(targetClient));

        assertEquals(numStreams / 2, sourceStreams.size());
        assertEquals(numStreams / 2, targetStreams.size());

        for (String name : sourceStreams) {
            checkStream(name, dlClient, dlServer, 1, numStreams / 2, numStreams / 2, true, true);
            checkStream(name, targetClient, targetServer, 1, numStreams / 2, numStreams / 2, false, false);
        }

        for (String name : targetStreams) {
            checkStream(name, targetClient, targetServer, 1, numStreams / 2, numStreams / 2, true, true);
            checkStream(name, dlClient, dlServer, 1, numStreams / 2, numStreams / 2, false, false);
        }
    }

}
