package com.twitter.distributedlog.client.proxy;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.client.stats.ClientStats;
import com.twitter.distributedlog.client.stats.OpStats;
import com.twitter.distributedlog.thrift.service.ClientInfo;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.util.FutureEventListener;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manager manages clients (channels) to proxies.
 */
public class ProxyClientManager implements TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(ProxyClientManager.class);

    private final ClientConfig clientConfig;
    private final ProxyClient.Builder clientBuilder;
    private final HashedWheelTimer timer;
    private final HostProvider hostProvider;
    private volatile Timeout periodicHandshakeTask;
    private final ConcurrentHashMap<SocketAddress, ProxyClient> address2Services =
            new ConcurrentHashMap<SocketAddress, ProxyClient>();
    private final CopyOnWriteArraySet<ProxyListener> proxyListeners =
            new CopyOnWriteArraySet<ProxyListener>();
    private volatile boolean closed = false;
    private volatile boolean periodicHandshakeEnabled = true;
    private final Stopwatch lastOwnershipSyncStopwatch;

    private final OpStats handshakeStats;

    public ProxyClientManager(ClientConfig clientConfig,
                              ProxyClient.Builder clientBuilder,
                              HashedWheelTimer timer,
                              HostProvider hostProvider,
                              ClientStats clientStats) {
        this.clientConfig = clientConfig;
        this.clientBuilder = clientBuilder;
        this.timer = timer;
        this.hostProvider = hostProvider;
        this.handshakeStats = clientStats.getOpStats("handshake");
        scheduleHandshake();
        this.lastOwnershipSyncStopwatch = Stopwatch.createStarted();
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
            boolean syncOwnerships = true;
            if (lastOwnershipSyncStopwatch.elapsed(TimeUnit.MILLISECONDS) <
                            clientConfig.getPeriodicOwnershipSyncIntervalMs()) {
                syncOwnerships = false;
            }

            final Set<SocketAddress> hostsSnapshot = hostProvider.getHosts();
            final AtomicInteger numHosts = new AtomicInteger(hostsSnapshot.size());
            final AtomicInteger numStreams = new AtomicInteger(0);
            final Stopwatch stopwatch = Stopwatch.createStarted();
            for (SocketAddress host : hostsSnapshot) {
                final SocketAddress address = host;
                final ProxyClient client = getClient(address);
                handshake(address, client, new FutureEventListener<ServerInfo>() {
                    @Override
                    public void onSuccess(ServerInfo serverInfo) {
                        numStreams.addAndGet(serverInfo.getOwnershipsSize());
                        notifyHandshakeSuccess(address, client, serverInfo, false, stopwatch);
                        complete();
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        notifyHandshakeFailure(address, client, cause, stopwatch);
                        complete();
                    }

                    private void complete() {
                        if (0 == numHosts.decrementAndGet()) {
                            logger.info("Periodic handshaked with {} hosts : {} streams",
                                    hostsSnapshot.size(), numStreams.get());
                        }
                    }
                }, false, syncOwnerships);
            }

            lastOwnershipSyncStopwatch.reset();
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

    private void notifyHandshakeSuccess(SocketAddress address,
                                        ProxyClient client,
                                        ServerInfo serverInfo,
                                        boolean logging,
                                        Stopwatch stopwatch) {
        if (logging) {
            if (null != serverInfo && serverInfo.isSetOwnerships()) {
                logger.info("Handshaked with {} : {} ownerships returned.",
                        address, serverInfo.getOwnerships().size());
            } else {
                logger.info("Handshaked with {} : no ownerships returned", address);
            }
        }
        handshakeStats.completeRequest(address, stopwatch.elapsed(TimeUnit.MICROSECONDS), 1);
        for (ProxyListener listener : proxyListeners) {
            listener.onHandshakeSuccess(address, client, serverInfo);
        }
    }

    private void notifyHandshakeFailure(SocketAddress address,
                                        ProxyClient client,
                                        Throwable cause,
                                        Stopwatch stopwatch) {
        handshakeStats.failRequest(address, stopwatch.elapsed(TimeUnit.MICROSECONDS), 1);
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
            final Stopwatch stopwatch = Stopwatch.createStarted();
            FutureEventListener<ServerInfo> listener = new FutureEventListener<ServerInfo>() {
                @Override
                public void onSuccess(ServerInfo serverInfo) {
                    notifyHandshakeSuccess(address, sc, serverInfo, true, stopwatch);
                }
                @Override
                public void onFailure(Throwable cause) {
                    notifyHandshakeFailure(address, sc, cause, stopwatch);
                }
            };
            // send a ping messaging after creating connections.
            handshake(address, sc, listener, true, true);
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
    private void handshake(SocketAddress address,
                           ProxyClient sc,
                           FutureEventListener<ServerInfo> listener,
                           boolean logging,
                           boolean getOwnerships) {
        if (clientConfig.getHandshakeWithClientInfo()) {
            ClientInfo clientInfo = new ClientInfo();
            clientInfo.setGetOwnerships(getOwnerships);
            clientInfo.setStreamNameRegex(clientConfig.getStreamNameRegex());
            if (logging) {
                logger.info("Handshaking with {} : {}", address, clientInfo);
            }
            sc.getService().handshakeWithClientInfo(clientInfo)
                    .addEventListener(listener);
        } else {
            if (logging) {
                logger.info("Handshaking with {}", address);
            }
            sc.getService().handshake().addEventListener(listener);
        }
    }

    /**
     * Handshake with all proxies.
     *
     * NOTE: this is a synchronous call.
     */
    public void handshake() {
        Set<SocketAddress> hostsSnapshot = hostProvider.getHosts();
        logger.info("Handshaking with {} hosts.", hostsSnapshot.size());
        final CountDownLatch latch = new CountDownLatch(hostsSnapshot.size());
        final Stopwatch stopwatch = Stopwatch.createStarted();
        for (SocketAddress host: hostsSnapshot) {
            final SocketAddress address = host;
            final ProxyClient client = getClient(address);
            handshake(address, client, new FutureEventListener<ServerInfo>() {
                @Override
                public void onSuccess(ServerInfo serverInfo) {
                    notifyHandshakeSuccess(address, client, serverInfo, true, stopwatch);
                    latch.countDown();
                }
                @Override
                public void onFailure(Throwable cause) {
                    notifyHandshakeFailure(address, client, cause, stopwatch);
                    latch.countDown();
                }
            }, true, true);
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
