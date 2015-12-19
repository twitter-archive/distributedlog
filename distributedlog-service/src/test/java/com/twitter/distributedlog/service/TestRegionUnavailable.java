package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.service.DistributedLogCluster.DLServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestRegionUnavailable extends DistributedLogServerTestCase {

    private final int numServersPerDC = 3;
    private final List<DLServer> localCluster;
    private final List<DLServer> remoteCluster;
    private TwoRegionDLClient client;

    public TestRegionUnavailable() {
        this.localCluster = new ArrayList<DLServer>();
        this.remoteCluster = new ArrayList<DLServer>();
    }

    @Before
    @Override
    public void setup() throws Exception {
        DistributedLogConfiguration localConf = new DistributedLogConfiguration();
        localConf.addConfiguration(conf);
        localConf.setDeciderBaseConfigPath("server_decider_1.yml");
        DistributedLogConfiguration remoteConf = new DistributedLogConfiguration();
        remoteConf.addConfiguration(conf);
        super.setup();
        int localPort = 9010;
        int remotePort = 9020;
        for (int i = 0; i < numServersPerDC; i++) {
            localCluster.add(createDistributedLogServer(localConf, localPort + i));
            remoteCluster.add(createDistributedLogServer(remoteConf, remotePort + i));
        }
        Map<SocketAddress, String> regionMap = new HashMap<SocketAddress, String>();
        for (DLServer server : localCluster) {
            regionMap.put(server.getAddress(), "local");
        }
        for (DLServer server : remoteCluster) {
            regionMap.put(server.getAddress(), "remote");
        }
        client = createTwoRegionDLClient("two_regions_client", regionMap);

    }

    private void registerStream(String streamName) {
        for (DLServer server : localCluster) {
            client.localRoutingService.addHost(streamName, server.getAddress());
        }
        client.remoteRoutingService.addHost(streamName, remoteCluster.get(0).getAddress());
    }

    @After
    @Override
    public void teardown() throws Exception {
        super.teardown();
        if (null != client) {
            client.shutdown();
        }
        for (DLServer server : localCluster) {
            server.shutdown();
        }
        for (DLServer server : remoteCluster) {
            server.shutdown();
        }
    }

    @Test(timeout = 60000)
    public void testRegionUnavailable() throws Exception {
        String name = "dlserver-region-unavailable";
        registerStream(name);

        for (long i = 1; i <= 10; i++) {
            client.dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        // check local region
        for (DLServer server : localCluster) {
            checkStreams(0, server);
        }
    }
}
