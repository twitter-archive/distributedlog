package com.twitter.distributedlog.client.proxy;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.client.proxy.MockDistributedLogServices.MockBasicService;
import com.twitter.distributedlog.client.proxy.MockDistributedLogServices.MockServerInfoService;
import com.twitter.distributedlog.client.proxy.MockProxyClientBuilder.MockProxyClient;
import com.twitter.distributedlog.client.resolver.TwitterRegionResolver;
import com.twitter.distributedlog.client.stats.ClientStats;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.finagle.stats.NullStatsReceiver;
import org.apache.commons.lang3.tuple.Pair;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Test Proxy Client Manager
 */
public class TestProxyClientManager {

    @Rule
    public TestName runtime = new TestName();

    private static ProxyClientManager createProxyClientManager(ProxyClient.Builder builder,
                                                               long periodicHandshakeIntervalMs) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setPeriodicHandshakeIntervalMs(periodicHandshakeIntervalMs);
        HashedWheelTimer dlTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestProxyClientManager-timer-%d").build(),
                clientConfig.getRedirectBackoffStartMs(),
                TimeUnit.MILLISECONDS);
        return new ProxyClientManager(clientConfig, builder, dlTimer,
                new ClientStats(NullStatsReceiver.get(), false, new TwitterRegionResolver()));
    }

    private static SocketAddress createSocketAddress(int port) {
        return new InetSocketAddress("127.0.0.1", port);
    }

    private static MockProxyClient createMockProxyClient(SocketAddress address) {
        return new MockProxyClient(address, new MockBasicService());
    }

    private static Pair<MockProxyClient, MockServerInfoService> createMockProxyClient(
            SocketAddress address, ServerInfo serverInfo) {
        MockServerInfoService service = new MockServerInfoService();
        MockProxyClient proxyClient = new MockProxyClient(address, service);
        service.updateServerInfo(serverInfo);
        return Pair.of(proxyClient, service);
    }

    @Test(timeout = 60000)
    public void testBasicCreateRemove() throws Exception {
        SocketAddress address = createSocketAddress(1000);
        MockProxyClientBuilder builder = new MockProxyClientBuilder();
        MockProxyClient mockProxyClient = createMockProxyClient(address);
        builder.provideProxyClient(address, mockProxyClient);

        ProxyClientManager clientManager = createProxyClientManager(builder, 0L);
        assertEquals("There should be no clients in the manager",
                0, clientManager.getNumProxies());
        ProxyClient proxyClient =  clientManager.createClient(address);
        assertEquals("Create client should build the proxy client",
                1, clientManager.getNumProxies());
        assertTrue("The client returned should be the same client that builder built",
                mockProxyClient == proxyClient);
    }

    @Test(timeout = 60000)
    public void testGetShouldCreateClient() throws Exception {
        SocketAddress address = createSocketAddress(2000);
        MockProxyClientBuilder builder = new MockProxyClientBuilder();
        MockProxyClient mockProxyClient = createMockProxyClient(address);
        builder.provideProxyClient(address, mockProxyClient);

        ProxyClientManager clientManager = createProxyClientManager(builder, 0L);
        assertEquals("There should be no clients in the manager",
                0, clientManager.getNumProxies());
        ProxyClient proxyClient =  clientManager.getClient(address);
        assertEquals("Get client should build the proxy client",
                1, clientManager.getNumProxies());
        assertTrue("The client returned should be the same client that builder built",
                mockProxyClient == proxyClient);
    }

    @Test(timeout = 60000)
    public void testConditionalRemoveClient() throws Exception {
        SocketAddress address = createSocketAddress(3000);
        MockProxyClientBuilder builder = new MockProxyClientBuilder();
        MockProxyClient mockProxyClient = createMockProxyClient(address);
        MockProxyClient anotherMockProxyClient = createMockProxyClient(address);
        builder.provideProxyClient(address, mockProxyClient);

        ProxyClientManager clientManager = createProxyClientManager(builder, 0L);
        assertEquals("There should be no clients in the manager",
                0, clientManager.getNumProxies());
        clientManager.createClient(address);
        assertEquals("Create client should build the proxy client",
                1, clientManager.getNumProxies());
        clientManager.removeClient(address, anotherMockProxyClient);
        assertEquals("Conditional remove should not remove proxy client",
                1, clientManager.getNumProxies());
        clientManager.removeClient(address, mockProxyClient);
        assertEquals("Conditional remove should remove proxy client",
                0, clientManager.getNumProxies());
    }

    @Test(timeout = 60000)
    public void testRemoveClient() throws Exception {
        SocketAddress address = createSocketAddress(3000);
        MockProxyClientBuilder builder = new MockProxyClientBuilder();
        MockProxyClient mockProxyClient = createMockProxyClient(address);
        builder.provideProxyClient(address, mockProxyClient);

        ProxyClientManager clientManager = createProxyClientManager(builder, 0L);
        assertEquals("There should be no clients in the manager",
                0, clientManager.getNumProxies());
        clientManager.createClient(address);
        assertEquals("Create client should build the proxy client",
                1, clientManager.getNumProxies());
        clientManager.removeClient(address);
        assertEquals("Remove should remove proxy client",
                0, clientManager.getNumProxies());
    }

    @Test(timeout = 60000)
    public void testCreateClientShouldHandshake() throws Exception {
        SocketAddress address = createSocketAddress(3000);
        MockProxyClientBuilder builder = new MockProxyClientBuilder();
        ServerInfo serverInfo = new ServerInfo();
        serverInfo.putToOwnerships(runtime.getMethodName() + "_stream",
                runtime.getMethodName() + "_owner");
        Pair<MockProxyClient, MockServerInfoService> mockProxyClient =
                createMockProxyClient(address, serverInfo);
        builder.provideProxyClient(address, mockProxyClient.getLeft());

        final AtomicReference<ServerInfo> resultHolder = new AtomicReference<ServerInfo>(null);
        final CountDownLatch doneLatch = new CountDownLatch(1);
        ProxyListener listener = new ProxyListener() {
            @Override
            public void onHandshakeSuccess(SocketAddress address, ServerInfo serverInfo) {
                resultHolder.set(serverInfo);
                doneLatch.countDown();
            }
            @Override
            public void onHandshakeFailure(SocketAddress address, ProxyClient client, Throwable cause) {
            }
        };

        ProxyClientManager clientManager = createProxyClientManager(builder, 0L);
        clientManager.registerProxyListener(listener);
        assertEquals("There should be no clients in the manager",
                0, clientManager.getNumProxies());
        clientManager.createClient(address);
        assertEquals("Create client should build the proxy client",
                1, clientManager.getNumProxies());

        // When a client is created, it would handshake with that proxy
        doneLatch.await();
        assertEquals("Handshake should return server info",
                serverInfo, resultHolder.get());
    }

    @Test(timeout = 60000)
    public void testHandshake() throws Exception {
        final int numHosts = 3;
        final int numStreamsPerHost = 3;
        final int initialPort = 4000;

        MockProxyClientBuilder builder = new MockProxyClientBuilder();
        Map<SocketAddress, ServerInfo> serverInfoMap =
                new HashMap<SocketAddress, ServerInfo>();
        for (int i = 0; i < numHosts; i++) {
            SocketAddress address = createSocketAddress(initialPort + i);
            ServerInfo serverInfo = new ServerInfo();
            for (int j = 0; j < numStreamsPerHost; j++) {
                serverInfo.putToOwnerships(runtime.getMethodName() + "_stream_" + j,
                        address.toString());
            }
            Pair<MockProxyClient, MockServerInfoService> mockProxyClient =
                    createMockProxyClient(address, serverInfo);
            builder.provideProxyClient(address, mockProxyClient.getLeft());
            serverInfoMap.put(address, serverInfo);
        }


        final Map<SocketAddress, ServerInfo> results = new HashMap<SocketAddress, ServerInfo>();
        final CountDownLatch doneLatch = new CountDownLatch(2 * numHosts);
        ProxyListener listener = new ProxyListener() {
            @Override
            public void onHandshakeSuccess(SocketAddress address, ServerInfo serverInfo) {
                synchronized (results) {
                    results.put(address, serverInfo);
                }
                doneLatch.countDown();
            }

            @Override
            public void onHandshakeFailure(SocketAddress address, ProxyClient client, Throwable cause) {
            }
        };

        ProxyClientManager clientManager = createProxyClientManager(builder, 0L);
        clientManager.registerProxyListener(listener);
        assertEquals("There should be no clients in the manager",
                0, clientManager.getNumProxies());
        for (int i = 0; i < numHosts; i++) {
            clientManager.createClient(createSocketAddress(initialPort + i));
        }
        // handshake would handshake with 3 hosts again
        clientManager.handshake();
        doneLatch.await();
        assertEquals("Handshake should return server info",
                numHosts, results.size());
        assertTrue("Handshake should get all server infos",
                Maps.difference(serverInfoMap, results).areEqual());
    }

    @Test(timeout = 60000)
    public void testPeriodicHandshake() throws Exception {
        final int numHosts = 3;
        final int numStreamsPerHost = 3;
        final int initialPort = 5000;

        MockProxyClientBuilder builder = new MockProxyClientBuilder();
        Map<SocketAddress, ServerInfo> serverInfoMap =
                new HashMap<SocketAddress, ServerInfo>();
        Map<SocketAddress, MockServerInfoService> mockServiceMap =
                new HashMap<SocketAddress, MockServerInfoService>();
        final Map<SocketAddress, CountDownLatch> hostDoneLatches =
                new HashMap<SocketAddress, CountDownLatch>();
        for (int i = 0; i < numHosts; i++) {
            SocketAddress address = createSocketAddress(initialPort + i);
            ServerInfo serverInfo = new ServerInfo();
            for (int j = 0; j < numStreamsPerHost; j++) {
                serverInfo.putToOwnerships(runtime.getMethodName() + "_stream_" + j,
                        address.toString());
            }
            Pair<MockProxyClient, MockServerInfoService> mockProxyClient =
                    createMockProxyClient(address, serverInfo);
            builder.provideProxyClient(address, mockProxyClient.getLeft());
            serverInfoMap.put(address, serverInfo);
            mockServiceMap.put(address, mockProxyClient.getRight());
            hostDoneLatches.put(address, new CountDownLatch(2));
        }

        final Map<SocketAddress, ServerInfo> results = new HashMap<SocketAddress, ServerInfo>();
        final CountDownLatch doneLatch = new CountDownLatch(numHosts);
        ProxyListener listener = new ProxyListener() {
            @Override
            public void onHandshakeSuccess(SocketAddress address, ServerInfo serverInfo) {
                synchronized (results) {
                    results.put(address, serverInfo);
                    CountDownLatch latch = hostDoneLatches.get(address);
                    if (null != latch) {
                        latch.countDown();
                    }
                }
                doneLatch.countDown();
            }

            @Override
            public void onHandshakeFailure(SocketAddress address, ProxyClient client, Throwable cause) {
            }
        };

        ProxyClientManager clientManager = createProxyClientManager(builder, 50L);
        clientManager.setPeriodicHandshakeEnabled(false);
        clientManager.registerProxyListener(listener);

        assertEquals("There should be no clients in the manager",
                0, clientManager.getNumProxies());
        for (int i = 0; i < numHosts; i++) {
            clientManager.createClient(createSocketAddress(initialPort + i));
        }

        // make sure the first 3 handshakes going through
        doneLatch.await();

        assertEquals("Handshake should return server info",
                numHosts, results.size());
        assertTrue("Handshake should get all server infos",
                Maps.difference(serverInfoMap, results).areEqual());

        // update server info
        for (int i = 0; i < numHosts; i++) {
            SocketAddress address = createSocketAddress(initialPort + i);
            ServerInfo serverInfo = new ServerInfo();
            for (int j = 0; j < numStreamsPerHost; j++) {
                serverInfo.putToOwnerships(runtime.getMethodName() + "_new_stream_" + j,
                        address.toString());
            }
            MockServerInfoService service = mockServiceMap.get(address);
            serverInfoMap.put(address, serverInfo);
            service.updateServerInfo(serverInfo);
        }

        clientManager.setPeriodicHandshakeEnabled(true);
        for (int i = 0; i < numHosts; i++) {
            SocketAddress address = createSocketAddress(initialPort + i);
            CountDownLatch latch = hostDoneLatches.get(address);
            latch.await();
        }

        assertTrue("Periodic handshake should update all server infos",
                Maps.difference(serverInfoMap, results).areEqual());
    }

}
