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
