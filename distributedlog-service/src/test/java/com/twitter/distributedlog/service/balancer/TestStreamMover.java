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
package com.twitter.distributedlog.service.balancer;

import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogCluster.DLServer;
import com.twitter.distributedlog.service.DistributedLogServerTestCase;
import com.twitter.util.Await;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestStreamMover extends DistributedLogServerTestCase {

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
    public void testMoveStream() throws Exception {
        String name = "dlserver-move-stream";

        // src client
        dlClient.routingService.addHost(name, dlServer.getAddress());
        // target client
        targetClient.routingService.addHost(name, targetServer.getAddress());

        // src client write a record to that stream
        Await.result(((DistributedLogClient) dlClient.dlClient).write(name, ByteBuffer.wrap("1".getBytes(UTF_8))));
        checkStream(name, dlClient, dlServer, 1, 1, 1, true, true);
        checkStream(name, targetClient, targetServer, 0, 0, 0, false, false);

        StreamMover streamMover = new StreamMoverImpl("source", dlClient.dlClient, dlClient.dlClient,
                                                      "target", targetClient.dlClient, targetClient.dlClient);
        assertTrue(streamMover.moveStream(name));
        checkStream(name, dlClient, dlServer, 0, 0, 0, false, false);
        checkStream(name, targetClient, targetServer, 1, 1, 1, true, true);
    }

}
