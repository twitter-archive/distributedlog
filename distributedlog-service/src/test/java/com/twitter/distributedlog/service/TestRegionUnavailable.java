/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.feature.DefaultFeatureProvider;
import com.twitter.distributedlog.service.DistributedLogCluster.DLServer;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.StatsLogger;
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

    public static class TestFeatureProvider extends DefaultFeatureProvider {

        public TestFeatureProvider(String rootScope,
                                   DistributedLogConfiguration conf,
                                   StatsLogger statsLogger) {
            super(rootScope, conf, statsLogger);
        }

        @Override
        protected Feature makeFeature(String featureName) {
            if (featureName.contains(ServerFeatureKeys.REGION_STOP_ACCEPT_NEW_STREAM.name().toLowerCase())) {
                return new SettableFeature(featureName, 10000);
            }
            return super.makeFeature(featureName);
        }

        @Override
        protected FeatureProvider makeProvider(String fullScopeName) {
            return super.makeProvider(fullScopeName);
        }
    }

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
        localConf.setFeatureProviderClass(TestFeatureProvider.class);
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
