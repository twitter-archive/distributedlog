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

import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.client.stats.ClientStats;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.finagle.Service;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.apache.thrift.protocol.TBinaryProtocol;
import scala.Option;
import scala.runtime.BoxedUnit;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Client talks to a single proxy.
 */
public class ProxyClient {

    public static interface Builder {
        /**
         * Build a proxy client to <code>address</code>.
         *
         * @param address
         *          proxy address
         * @return proxy client
         */
        ProxyClient build(SocketAddress address);
    }

    public static Builder newBuilder(String clientName,
                                     ClientId clientId,
                                     ClientBuilder clientBuilder,
                                     ClientConfig clientConfig,
                                     ClientStats clientStats) {
        return new DefaultBuilder(clientName, clientId, clientBuilder, clientConfig, clientStats);
    }

    public static class DefaultBuilder implements Builder {

        private final String clientName;
        private final ClientId clientId;
        private final ClientBuilder clientBuilder;
        private final ClientStats clientStats;

        private DefaultBuilder(String clientName,
                               ClientId clientId,
                               ClientBuilder clientBuilder,
                               ClientConfig clientConfig,
                               ClientStats clientStats) {
            this.clientName = clientName;
            this.clientId = clientId;
            this.clientStats = clientStats;
            // client builder
            ClientBuilder builder = setDefaultSettings(null == clientBuilder ? getDefaultClientBuilder() : clientBuilder);
            if (clientConfig.getThriftMux()) {
                builder = enableThriftMux(builder, clientId);
            }
            this.clientBuilder = builder;
        }

        @SuppressWarnings("unchecked")
        private ClientBuilder enableThriftMux(ClientBuilder builder, ClientId clientId) {
            return builder.stack(ThriftMux.client().withClientId(clientId));
        }

        private ClientBuilder getDefaultClientBuilder() {
            return ClientBuilder.get()
                .hostConnectionLimit(1)
                .tcpConnectTimeout(Duration.fromMilliseconds(200))
                .connectTimeout(Duration.fromMilliseconds(200))
                .requestTimeout(Duration.fromSeconds(1));
        }

        @SuppressWarnings("unchecked")
        private ClientBuilder setDefaultSettings(ClientBuilder builder) {
            return builder.name(clientName)
                   .codec(ThriftClientFramedCodec.apply(Option.apply(clientId)))
                   .failFast(false)
                   .noFailureAccrual()
                   // disable retries on finagle client builder, as there is only one host per finagle client
                   // we should throw exception immediately on first failure, so DL client could quickly detect
                   // failures and retry other proxies.
                   .retries(1)
                   .keepAlive(true);
        }

        @Override
        @SuppressWarnings("unchecked")
        public ProxyClient build(SocketAddress address) {
            Service<ThriftClientRequest, byte[]> client =
                ClientBuilder.safeBuildFactory(
                        clientBuilder
                                .hosts((InetSocketAddress) address)
                                .reportTo(clientStats.getFinagleStatsReceiver(address))
                ).toService();
            DistributedLogService.ServiceIface service =
                    new DistributedLogService.ServiceToClient(client, new TBinaryProtocol.Factory());
            return new ProxyClient(address, client, service);
        }

    }

    private final SocketAddress address;
    private final Service<ThriftClientRequest, byte[]> client;
    private final DistributedLogService.ServiceIface service;

    protected ProxyClient(SocketAddress address,
                          Service<ThriftClientRequest, byte[]> client,
                          DistributedLogService.ServiceIface service) {
        this.address = address;
        this.client  = client;
        this.service = service;
    }

    public SocketAddress getAddress() {
        return address;
    }

    public Service<ThriftClientRequest, byte[]> getClient() {
        return client;
    }

    public DistributedLogService.ServiceIface getService() {
        return service;
    }

    public Future<BoxedUnit> close() {
        return client.close();
    }
}
