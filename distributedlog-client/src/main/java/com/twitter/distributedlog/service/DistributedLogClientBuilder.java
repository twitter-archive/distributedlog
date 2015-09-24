package com.twitter.distributedlog.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.client.DistributedLogClientImpl;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.client.resolver.RegionResolver;
import com.twitter.distributedlog.client.resolver.TwitterRegionResolver;
import com.twitter.distributedlog.client.routing.RegionsRoutingService;
import com.twitter.distributedlog.client.routing.RoutingService;
import com.twitter.distributedlog.client.routing.RoutingUtils;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId;

import java.net.SocketAddress;

public final class DistributedLogClientBuilder {

    private String _name = null;
    private ClientId _clientId = null;
    private RoutingService.Builder _routingServiceBuilder = null;
    private ClientBuilder _clientBuilder = null;
    private StatsReceiver _statsReceiver = new NullStatsReceiver();
    private StatsReceiver _streamStatsReceiver = new NullStatsReceiver();
    private ClientConfig _clientConfig = new ClientConfig();
    private boolean _enableRegionStats = false;
    private final RegionResolver _regionResolver = new TwitterRegionResolver();

    /**
     * Create a client builder
     *
     * @return client builder
     */
    public static DistributedLogClientBuilder newBuilder() {
        return new DistributedLogClientBuilder();
    }

    public static DistributedLogClientBuilder newBuilder(DistributedLogClientBuilder builder) {
        DistributedLogClientBuilder newBuilder = new DistributedLogClientBuilder();
        newBuilder._name = builder._name;
        newBuilder._clientId = builder._clientId;
        newBuilder._clientBuilder = builder._clientBuilder;
        newBuilder._routingServiceBuilder = builder._routingServiceBuilder;
        newBuilder._statsReceiver = builder._statsReceiver;
        newBuilder._streamStatsReceiver = builder._streamStatsReceiver;
        newBuilder._enableRegionStats = builder._enableRegionStats;
        newBuilder._clientConfig = ClientConfig.newConfig(builder._clientConfig);
        return newBuilder;
    }

    // private constructor
    private DistributedLogClientBuilder() {}

    /**
     * Client Name.
     *
     * @param name
     *          client name
     * @return client builder.
     */
    public DistributedLogClientBuilder name(String name) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._name = name;
        return newBuilder;
    }

    /**
     * Client ID.
     *
     * @param clientId
     *          client id
     * @return client builder.
     */
    public DistributedLogClientBuilder clientId(ClientId clientId) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientId = clientId;
        return newBuilder;
    }

    /**
     * Serverset to access proxy services.
     *
     * @param serverSet
     *          server set.
     * @return client builder.
     */
    public DistributedLogClientBuilder serverSet(ServerSet serverSet) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._routingServiceBuilder = RoutingUtils.buildRoutingService(serverSet);
        newBuilder._enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Server Sets to access proxy services. The <i>local</i> server set will be tried first,
     * then <i>remotes</i>.
     *
     * @param local local server set.
     * @param remotes remote server sets.
     * @return client builder.
     */
    public DistributedLogClientBuilder serverSets(ServerSet local, ServerSet...remotes) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        RoutingService.Builder[] builders = new RoutingService.Builder[remotes.length + 1];
        builders[0] = RoutingUtils.buildRoutingService(local);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = RoutingUtils.buildRoutingService(remotes[i-1]);
        }
        newBuilder._routingServiceBuilder = RegionsRoutingService.newBuilder()
                .resolver(_regionResolver)
                .routingServiceBuilders(builders);
        newBuilder._enableRegionStats = remotes.length > 0;
        return newBuilder;
    }

    /**
     * Name to access proxy services.
     *
     * @param finagleNameStr
     *          finagle name string.
     * @return client builder.
     */
    public DistributedLogClientBuilder finagleNameStr(String finagleNameStr) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._routingServiceBuilder = RoutingUtils.buildRoutingService(finagleNameStr);
        newBuilder._enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Finagle name strs to access proxy services. The <i>local</i> finalge name str will be tried first,
     * then <i>remotes</i>.
     *
     * @param local local server set.
     * @param remotes remote server sets.
     * @return client builder.
     */
    public DistributedLogClientBuilder finagleNameStrs(String local, String...remotes) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        RoutingService.Builder[] builders = new RoutingService.Builder[remotes.length + 1];
        builders[0] = RoutingUtils.buildRoutingService(local);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = RoutingUtils.buildRoutingService(remotes[i - 1]);
        }
        newBuilder._routingServiceBuilder = RegionsRoutingService.newBuilder()
                .routingServiceBuilders(builders)
                .resolver(_regionResolver);
        newBuilder._enableRegionStats = remotes.length > 0;
        return newBuilder;
    }

    /**
     * Address of write proxy to connect.
     *
     * @param address
     *          write proxy address.
     * @return client builder.
     */
    public DistributedLogClientBuilder host(SocketAddress address) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._routingServiceBuilder = RoutingUtils.buildRoutingService(address);
        newBuilder._enableRegionStats = false;
        return newBuilder;
    }

    private DistributedLogClientBuilder routingServiceBuilder(RoutingService.Builder builder) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._routingServiceBuilder = builder;
        newBuilder._enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Routing Service to access proxy services.
     *
     * @param routingService
     *          routing service
     * @return client builder.
     */
    @VisibleForTesting
    public DistributedLogClientBuilder routingService(RoutingService routingService) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._routingServiceBuilder = RoutingUtils.buildRoutingService(routingService);
        newBuilder._enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Stats receiver to expose client stats.
     *
     * @param statsReceiver
     *          stats receiver.
     * @return client builder.
     */
    public DistributedLogClientBuilder statsReceiver(StatsReceiver statsReceiver) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._statsReceiver = statsReceiver;
        return newBuilder;
    }

    /**
     * Stream Stats Receiver to expose per stream stats.
     *
     * @param streamStatsReceiver
     *          stream stats receiver
     * @return client builder.
     */
    public DistributedLogClientBuilder streamStatsReceiver(StatsReceiver streamStatsReceiver) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._streamStatsReceiver = streamStatsReceiver;
        return newBuilder;
    }

    /**
     * Set underlying finagle client builder.
     *
     * @param builder
     *          finagle client builder.
     * @return client builder.
     */
    public DistributedLogClientBuilder clientBuilder(ClientBuilder builder) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientBuilder = builder;
        return newBuilder;
    }

    /**
     * Backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public DistributedLogClientBuilder redirectBackoffStartMs(int ms) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setRedirectBackoffStartMs(ms);
        return newBuilder;
    }

    /**
     * Max backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public DistributedLogClientBuilder redirectBackoffMaxMs(int ms) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setRedirectBackoffMaxMs(ms);
        return newBuilder;
    }

    /**
     * Max redirects that is allowed per request. If <i>redirects</i> are
     * exhausted, fail the request immediately.
     *
     * @param redirects
     *          max redirects allowed before failing a request.
     * @return client builder.
     */
    public DistributedLogClientBuilder maxRedirects(int redirects) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setMaxRedirects(redirects);
        return newBuilder;
    }

    /**
     * Timeout per request in millis.
     *
     * @param timeoutMs
     *          timeout per request in millis.
     * @return client builder.
     */
    public DistributedLogClientBuilder requestTimeoutMs(int timeoutMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setRequestTimeoutMs(timeoutMs);
        return newBuilder;
    }

    /**
     * Set thriftmux enabled.
     *
     * @param enabled
     *          is thriftmux enabled
     * @return client builder.
     */
    public DistributedLogClientBuilder thriftmux(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setThriftMux(enabled);
        return newBuilder;
    }

    /**
     * Set failfast stream exception handling enabled.
     *
     * @param enabled
     *          is failfast exception handling enabled
     * @return client builder.
     */
    public DistributedLogClientBuilder streamFailfast(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setStreamFailfast(enabled);
        return newBuilder;
    }

    /**
     * Set the regex to match stream names that the client cares about.
     *
     * @param nameRegex
     *          stream name regex
     * @return client builder
     */
    public DistributedLogClientBuilder streamNameRegex(String nameRegex) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setStreamNameRegex(nameRegex);
        return newBuilder;
    }

    /**
     * Whether to use the new handshake endpoint to exchange ownership cache. Enable this
     * when the servers are updated to support handshaking with client info.
     *
     * @param enabled
     *          new handshake endpoint is enabled.
     * @return client builder.
     */
    public DistributedLogClientBuilder handshakeWithClientInfo(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setHandshakeWithClientInfo(enabled);
        return newBuilder;
    }

    /**
     * Set the periodic handshake interval in milliseconds. Every <code>intervalMs</code>,
     * the DL client will handshake with existing proxies again. If the interval is less than
     * ownership sync interval, the handshake won't sync ownerships. Otherwise, it will.
     *
     * @see #periodicOwnershipSyncIntervalMs(long)
     * @param intervalMs
     *          handshake interval
     * @return client builder.
     */
    public DistributedLogClientBuilder periodicHandshakeIntervalMs(long intervalMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setPeriodicHandshakeIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Set the periodic ownership sync interval in milliseconds. If periodic handshake is enabled,
     * the handshake will sync ownership if the elapsed time is larger than sync interval.
     *
     * @see #periodicHandshakeIntervalMs(long)
     * @param intervalMs
     *          interval that handshake should sync ownerships.
     * @return client builder
     */
    public DistributedLogClientBuilder periodicOwnershipSyncIntervalMs(long intervalMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setPeriodicOwnershipSyncIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Enable/Disable periodic dumping ownership cache.
     *
     * @param enabled
     *          flag to enable/disable periodic dumping ownership cache
     * @return client builder.
     */
    public DistributedLogClientBuilder periodicDumpOwnershipCache(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setPeriodicDumpOwnershipCacheEnabled(enabled);
        return newBuilder;
    }

    /**
     * Set periodic dumping ownership cache interval.
     *
     * @param intervalMs
     *          interval on dumping ownership cache, in millis.
     * @return client builder
     */
    public DistributedLogClientBuilder periodicDumpOwnershipCacheIntervalMs(long intervalMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setPeriodicDumpOwnershipCacheIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Enable handshake tracing.
     *
     * @param enabled
     *          flag to enable/disable handshake tracing
     * @return client builder
     */
    public DistributedLogClientBuilder handshakeTracing(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setHandshakeTracingEnabled(enabled);
        return newBuilder;
    }

    /**
     * Enable checksum on requests to the proxy.
     *
     * @param enabled
     *          flag to enable/disable checksum
     * @return client builder
     */
    public DistributedLogClientBuilder checksum(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig.setChecksumEnabled(enabled);
        return newBuilder;
    }

    DistributedLogClientBuilder clientConfig(ClientConfig clientConfig) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientConfig = ClientConfig.newConfig(clientConfig);
        return newBuilder;
    }

    /**
     * Build distributedlog client.
     *
     * @return distributedlog client.
     */
    public DistributedLogClient build() {
        return buildClient();
    }

    /**
     * Build monitor service client.
     *
     * @return monitor service client.
     */
    public MonitorServiceClient buildMonitorClient() {
        return buildClient();
    }

    DistributedLogClientImpl buildClient() {
        Preconditions.checkNotNull(_name, "No name provided.");
        Preconditions.checkNotNull(_clientId, "No client id provided.");
        Preconditions.checkNotNull(_routingServiceBuilder, "No routing service builder provided.");
        Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided.");
        if (null == _streamStatsReceiver) {
            _streamStatsReceiver = new NullStatsReceiver();
        }

        RoutingService routingService = _routingServiceBuilder
                .statsReceiver(_statsReceiver.scope("routing"))
                .build();
        DistributedLogClientImpl clientImpl =
                new DistributedLogClientImpl(
                        _name, _clientId, routingService, _clientBuilder, _clientConfig,
                        _statsReceiver, _streamStatsReceiver, _regionResolver, _enableRegionStats);
        routingService.startService();
        clientImpl.handshake();
        return clientImpl;
    }

}
