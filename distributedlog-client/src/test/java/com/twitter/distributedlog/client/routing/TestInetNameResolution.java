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
package com.twitter.distributedlog.client.routing;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

public class TestInetNameResolution {
    static final Logger logger = LoggerFactory.getLogger(TestRoutingService.class);

    @Test(timeout = 10000)
    public void testInetNameResolution() throws Exception {
        String nameStr = "inet!127.0.0.1:3181";
        final CountDownLatch resolved = new CountDownLatch(1);
        final AtomicBoolean validationFailed = new AtomicBoolean(false);

        NameServerSet serverSet = new NameServerSet(nameStr);
        serverSet.watch(new DynamicHostSet.HostChangeMonitor<ServiceInstance>() {
            @Override
            public void onChange(ImmutableSet<ServiceInstance> hostSet) {
                if (hostSet.size() > 1) {
                    logger.error("HostSet has more elements than expected {}", hostSet);
                    validationFailed.set(true);
                    resolved.countDown();
                } else if (hostSet.size() == 1) {
                    ServiceInstance serviceInstance = hostSet.iterator().next();
                    Endpoint endpoint = serviceInstance.getAdditionalEndpoints().get("thrift");
                    InetSocketAddress address = new InetSocketAddress(endpoint.getHost(), endpoint.getPort());
                    if (endpoint.getPort() != 3181) {
                        logger.error("Port does not match the expected port {}", endpoint.getPort());
                        validationFailed.set(true);
                    } else if (!address.getAddress().getHostAddress().equals("127.0.0.1")) {
                        logger.error("Host address does not match the expected address {}", address.getAddress().getHostAddress());
                        validationFailed.set(true);
                    }
                    resolved.countDown();
                }
            }
        });

        resolved.await();
        Assert.assertEquals(false, validationFailed.get());
    }
}
