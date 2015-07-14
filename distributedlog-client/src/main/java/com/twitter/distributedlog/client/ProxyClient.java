package com.twitter.distributedlog.client;

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

import java.net.SocketAddress;

/**
 * Client talks to a single proxy.
 */
public class ProxyClient {

    public static Builder newBuilder(String clientName,
                                     ClientId clientId,
                                     ClientBuilder clientBuilder,
                                     ClientConfig clientConfig,
                                     ClientStats clientStats) {
        return new Builder(clientName, clientId, clientBuilder, clientConfig, clientStats);
    }

    public static class Builder {

        private final String clientName;
        private final ClientId clientId;
        private final ClientBuilder clientBuilder;
        private final ClientStats clientStats;

        private Builder(String clientName,
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
                builder = builder.stack(ThriftMux.client().withClientId(clientId));
            }
            this.clientBuilder = builder;
        }

        private ClientBuilder getDefaultClientBuilder() {
            return ClientBuilder.get()
                .hostConnectionLimit(1)
                .connectionTimeout(Duration.fromSeconds(1))
                .requestTimeout(Duration.fromSeconds(1));
        }

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

        public ProxyClient build(SocketAddress address) {
            Service<ThriftClientRequest, byte[]> client =
                ClientBuilder.safeBuildFactory(
                        clientBuilder
                                .hosts(address)
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

    private ProxyClient(SocketAddress address,
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
