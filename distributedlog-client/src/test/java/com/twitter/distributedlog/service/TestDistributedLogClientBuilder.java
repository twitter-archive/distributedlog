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

import com.twitter.finagle.thrift.ClientId$;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.util.Duration;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Case of {@link com.twitter.distributedlog.service.DistributedLogClientBuilder}
 */
public class TestDistributedLogClientBuilder {

    @Test(timeout = 60000)
    public void testBuildClientsFromSameBuilder() throws Exception {
        DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder()
                .name("build-clients-from-same-builder")
                .clientId(ClientId$.MODULE$.apply("test-builder"))
                .finagleNameStr("inet!127.0.0.1:7001")
                .streamNameRegex(".*")
                .handshakeWithClientInfo(true)
                .clientBuilder(ClientBuilder.get()
                    .hostConnectionLimit(1)
                    .connectTimeout(Duration.fromSeconds(1))
                    .tcpConnectTimeout(Duration.fromSeconds(1))
                    .requestTimeout(Duration.fromSeconds(10)));
        DistributedLogClient client1 = builder.build();
        DistributedLogClient client2 = builder.build();
        assertFalse(client1 == client2);
    }
}
