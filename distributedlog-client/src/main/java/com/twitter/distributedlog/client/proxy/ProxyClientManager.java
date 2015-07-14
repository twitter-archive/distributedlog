package com.twitter.distributedlog.client.proxy;

import com.google.common.collect.ImmutableMap;
import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.thrift.service.ClientInfo;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.util.FutureEventListener;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Manager manages clients (channels) to proxies.
 */
public class ProxyClientManager implements TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(ProxyClientManager.class);

    private final ClientConfig clientConfig;
    private final ProxyClient.Builder clientBuilder;
    private final HashedWheelTimer timer;
    private volatile Timeout periodicHandshakeTask;
    private final ConcurrentHashMap<SocketAddress, ProxyClient> address2Services =
            new ConcurrentHashMap<SocketAddress, ProxyClient>();
    private final CopyOnWriteArraySet<ProxyListener> proxyListeners =
            new CopyOnWriteArraySet<ProxyListener>();
    private volatile boolean closed = false;
    private volatile boolean periodicHandshakeEnabled = true;

    public ProxyClientManager(ClientConfig clientConfig,
                              ProxyClient.Builder clientBuilder,
                              HashedWheelTimer timer) {
        this.clientConfig = clientConfig;
        this.clientBuilder = clientBuilder;
        this.timer = timer;
        scheduleHandshake();
    }

    private void scheduleHandshake() {
        if (clientConfig.getPeriodicHandshakeIntervalMs() > 0) {
            periodicHandshakeTask = timer.newTimeout(this,
                    clientConfig.getPeriodicHandshakeIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    void setPeriodicHandshakeEnabled(boolean enabled) {
        this.periodicHandshakeEnabled = enabled;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled() || closed) {
            return;
        }
        if (periodicHandshakeEnabled) {
            for (Map.Entry<SocketAddress, ProxyClient> entry : address2Services.entrySet()) {
                final SocketAddress address = entry.getKey();
                final ProxyClient client = entry.getValue();
                handshake(address, client, new FutureEventListener<ServerInfo>() {
                    @Override
                    public void onSuccess(ServerInfo serverInfo) {
                        notifyHandshakeSuccess(address, serverInfo);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        notifyHandshakeFailure(address, client, cause);
                    }
                });
            }
        }
        scheduleHandshake();
    }

    /**
     * Register a proxy <code>listener</code> on proxy related changes.
     *
     * @param listener
     *          proxy listener
     */
    public void registerProxyListener(ProxyListener listener) {
        proxyListeners.add(listener);
    }

    private void notifyHandshakeSuccess(SocketAddress address, ServerInfo serverInfo) {
        for (ProxyListener listener : proxyListeners) {
            listener.onHandshakeSuccess(address, serverInfo);
        }
    }

    private void notifyHandshakeFailure(SocketAddress address,
                                        ProxyClient client,
                                        Throwable cause) {
        for (ProxyListener listener : proxyListeners) {
            listener.onHandshakeFailure(address, client, cause);
        }
    }

    /**
     * Retrieve a client to proxy <code>address</code>.
     *
     * @param address
     *          proxy address
     * @return proxy client
     */
    public ProxyClient getClient(final SocketAddress address) {
        ProxyClient sc = address2Services.get(address);
        if (null != sc) {
            return sc;
        }
        return createClient(address);
    }

    /**
     * Remove the client to proxy <code>address</code>.
     *
     * @param address
     *          proxy address
     */
    public void removeClient(SocketAddress address) {
        ProxyClient sc = address2Services.remove(address);
        if (null != sc) {
            logger.info("Removed host {}.", address);
            sc.close();
        }
    }

    /**
     * Remove the client <code>sc</code> to proxy <code>address</code>.
     *
     * @param address
     *          proxy address
     * @param sc
     *          proxy client
     */
    public void removeClient(SocketAddress address, ProxyClient sc) {
        if (address2Services.remove(address, sc)) {
            logger.info("Remove client {} to host {}.", sc, address);
            sc.close();
        }
    }

    /**
     * Create a client to proxy <code>address</code>.
     *
     * @param address
     *          proxy address
     * @return proxy client
     */
    public ProxyClient createClient(final SocketAddress address) {
        final ProxyClient sc = clientBuilder.build(address);
        ProxyClient oldSC = address2Services.putIfAbsent(address, sc);
        if (null != oldSC) {
            sc.close();
            return oldSC;
        } else {
            FutureEventListener<ServerInfo> listener = new FutureEventListener<ServerInfo>() {
                @Override
                public void onSuccess(ServerInfo serverInfo) {
                    notifyHandshakeSuccess(address, serverInfo);
                }
                @Override
                public void onFailure(Throwable cause) {
                    notifyHandshakeFailure(address, sc, cause);
                }
            };
            // send a ping messaging after creating connections.
            handshake(address, sc, listener);
            return sc;
        }
    }

    /**
     * Handshake with a given proxy
     *
     * @param address
     *          proxy address
     * @param sc
     *          proxy client
     * @param listener
     *          listener on handshake result
     */
    private void handshake(SocketAddress address, ProxyClient sc,
                           FutureEventListener<ServerInfo> listener) {
        if (clientConfig.getHandshakeWithClientInfo()) {
            ClientInfo clientInfo = new ClientInfo();
            clientInfo.setStreamNameRegex(clientConfig.getStreamNameRegex());
            logger.info("Handshaking with {} : {}", address, clientInfo);
            sc.getService().handshakeWithClientInfo(clientInfo)
                    .addEventListener(listener);
        } else {
            logger.info("Handshaking with {}", address);
            sc.getService().handshake().addEventListener(listener);
        }
    }

    /**
     * Handshake with all proxies.
     *
     * NOTE: this is a synchronous call.
     */
    public void handshake() {
        Map<SocketAddress, ProxyClient> snapshot =
                new HashMap<SocketAddress, ProxyClient>(address2Services);
        logger.info("Handshaking with {} hosts.", snapshot.size());
        final CountDownLatch latch = new CountDownLatch(snapshot.size());
        for (Map.Entry<SocketAddress, ProxyClient> entry : snapshot.entrySet()) {
            final SocketAddress address = entry.getKey();
            final ProxyClient client = entry.getValue();
            handshake(address, client, new FutureEventListener<ServerInfo>() {
                @Override
                public void onSuccess(ServerInfo serverInfo) {
                    notifyHandshakeSuccess(address, serverInfo);
                    latch.countDown();
                }
                @Override
                public void onFailure(Throwable cause) {
                    notifyHandshakeFailure(address, client, cause);
                    latch.countDown();
                }
            });
        }
        try {
            latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.warn("Interrupted on handshaking with servers : ", e);
        }
    }

    /**
     * Return number of proxies managed by client manager.
     *
     * @return number of proxies managed by client manager.
     */
    public int getNumProxies() {
        return address2Services.size();
    }

    /**
     * Return all clients.
     *
     * @return all clients.
     */
    public Map<SocketAddress, ProxyClient> getAllClients() {
        return ImmutableMap.copyOf(address2Services);
    }

    public void close() {
        closed = true;
        Timeout task = periodicHandshakeTask;
        if (null != task) {
            task.cancel();
        }
        for (ProxyClient sc : address2Services.values()) {
            sc.close();
        }
    }
}
