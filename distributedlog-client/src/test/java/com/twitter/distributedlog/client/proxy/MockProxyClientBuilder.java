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
package com.twitter.distributedlog.client.proxy;

import com.twitter.distributedlog.thrift.service.DistributedLogService;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Mock Proxy Client Builder
 */
class MockProxyClientBuilder implements ProxyClient.Builder {

    static class MockProxyClient extends ProxyClient {
        MockProxyClient(SocketAddress address,
                        DistributedLogService.ServiceIface service) {
            super(address, new MockThriftClient(), service);
        }
    }

    private final ConcurrentMap<SocketAddress, MockProxyClient> clients =
            new ConcurrentHashMap<SocketAddress, MockProxyClient>();

    public void provideProxyClient(SocketAddress address,
                                   MockProxyClient proxyClient) {
        clients.put(address, proxyClient);
    }

    @Override
    public ProxyClient build(SocketAddress address) {
        return clients.get(address);
    }
}
