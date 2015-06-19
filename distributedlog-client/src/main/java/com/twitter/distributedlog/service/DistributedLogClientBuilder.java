package com.twitter.distributedlog.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.exceptions.DLClientClosedException;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.service.RoutingService.RoutingContext;
import com.twitter.distributedlog.service.stats.ClientStats;
import com.twitter.distributedlog.service.stats.OwnershipStatsLogger;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.ClientInfo;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.HeartbeatOptions;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.finagle.CancelledRequestException;
import com.twitter.finagle.ChannelException;
import com.twitter.finagle.ConnectionFailedException;
import com.twitter.finagle.Failure;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.RequestTimeoutException;
import com.twitter.finagle.Service;
import com.twitter.finagle.ServiceException;
import com.twitter.finagle.ServiceTimeoutException;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.WriteException;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistributedLogClientBuilder {

    private static final int NUM_CONSISTENT_HASH_REPLICAS = 997;

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
        newBuilder._routingServiceBuilder = ConsistentHashRoutingService.newBuilder()
                .serverSet(serverSet)
                .resolveFromName(false)
                .numReplicas(NUM_CONSISTENT_HASH_REPLICAS);
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
        builders[0] = ConsistentHashRoutingService.newBuilder()
                .serverSet(local)
                .resolveFromName(false)
                .numReplicas(NUM_CONSISTENT_HASH_REPLICAS);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = ConsistentHashRoutingService.newBuilder()
                    .serverSet(remotes[i-1])
                    .resolveFromName(false)
                    .numReplicas(NUM_CONSISTENT_HASH_REPLICAS);
        }
        newBuilder._routingServiceBuilder = RegionsRoutingService.newBuilder()
                .resolver(_regionResolver)
                .routingServiceBuilders(builders);
        newBuilder._enableRegionStats = remotes.length > 0;
        return newBuilder;
    }

    static RoutingService.Builder buildRoutingService(String finagleNameStr) {
        if (!finagleNameStr.startsWith("serverset!") && !finagleNameStr.startsWith("inet!")) {
            // We only support serverset based names at the moment
            throw new UnsupportedOperationException("Finagle Name format not supported for name: " + finagleNameStr);
        }
        return ConsistentHashRoutingService.newBuilder()
                .serverSet(new NameServerSet(finagleNameStr))
                .resolveFromName(true)
                .numReplicas(NUM_CONSISTENT_HASH_REPLICAS);
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
        newBuilder._routingServiceBuilder = buildRoutingService(finagleNameStr);
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
        builders[0] = buildRoutingService(local);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = buildRoutingService(remotes[i-1]);
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
        newBuilder._routingServiceBuilder = SingleHostRoutingService.newBuilder().address(address);
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
        newBuilder._routingServiceBuilder = new RoutingServiceProvider(routingService);
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

    DistributedLogClientImpl buildClient() {
        Preconditions.checkNotNull(_name, "No name provided.");
        Preconditions.checkNotNull(_clientId, "No client id provided.");
        Preconditions.checkNotNull(_routingServiceBuilder, "No routing service builder provided.");
        Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided.");
        if (null == _streamStatsReceiver) {
            _streamStatsReceiver = new NullStatsReceiver();
        }

        RoutingService routingService = _routingServiceBuilder.build();
        DistributedLogClientImpl clientImpl =
                new DistributedLogClientImpl(
                        _name, _clientId, routingService, _clientBuilder, _clientConfig,
                        _statsReceiver, _streamStatsReceiver, _regionResolver, _enableRegionStats);
        routingService.startService();
        clientImpl.handshake();
        return clientImpl;
    }

    static class ClientConfig {
        int redirectBackoffStartMs = 25;
        int redirectBackoffMaxMs = 100;
        int maxRedirects = -1;
        int requestTimeoutMs = -1;
        boolean thriftmux = false;
        boolean streamFailfast = false;
        String streamNameRegex = ".*";
        boolean handshakeWithClientInfo = false;

        ClientConfig setMaxRedirects(int maxRedirects) {
            this.maxRedirects = maxRedirects;
            return this;
        }

        int getMaxRedirects() {
            return this.maxRedirects;
        }

        ClientConfig setRequestTimeoutMs(int timeoutInMillis) {
            this.requestTimeoutMs = timeoutInMillis;
            return this;
        }

        int getRequestTimeoutMs() {
            return this.requestTimeoutMs;
        }

        ClientConfig setRedirectBackoffStartMs(int ms) {
            this.redirectBackoffStartMs = ms;
            return this;
        }

        int getRedirectBackoffStartMs() {
            return this.redirectBackoffStartMs;
        }

        ClientConfig setRedirectBackoffMaxMs(int ms) {
            this.redirectBackoffMaxMs = ms;
            return this;
        }

        int getRedirectBackoffMaxMs() {
            return this.redirectBackoffMaxMs;
        }

        ClientConfig setThriftMux(boolean enabled) {
            this.thriftmux = enabled;
            return this;
        }

        boolean getThriftMux() {
            return this.thriftmux;
        }

        ClientConfig setStreamFailfast(boolean enabled) {
            this.streamFailfast = enabled;
            return this;
        }

        boolean getStreamFailfast() {
            return this.streamFailfast;
        }

        ClientConfig setStreamNameRegex(String nameRegex) {
            Preconditions.checkNotNull(nameRegex);
            this.streamNameRegex = nameRegex;
            return this;
        }

        String getStreamNameRegex() {
            return this.streamNameRegex;
        }

        ClientConfig setHandshakeWithClientInfo(boolean enabled) {
            this.handshakeWithClientInfo = enabled;
            return this;
        }

        boolean getHandshakeWithClientInfo() {
            return this.handshakeWithClientInfo;
        }

        static ClientConfig newConfig(ClientConfig config) {
            ClientConfig newConfig = new ClientConfig();
            newConfig.setMaxRedirects(config.getMaxRedirects())
                     .setRequestTimeoutMs(config.getRequestTimeoutMs())
                     .setRedirectBackoffStartMs(config.getRedirectBackoffStartMs())
                     .setRedirectBackoffMaxMs(config.getRedirectBackoffMaxMs())
                     .setThriftMux(config.getThriftMux())
                     .setStreamFailfast(config.getStreamFailfast())
                     .setStreamNameRegex(config.getStreamNameRegex())
                     .setHandshakeWithClientInfo(config.getHandshakeWithClientInfo());
            return newConfig;
        }
    }

    static class DistributedLogClientImpl implements DistributedLogClient, MonitorServiceClient, RoutingService.RoutingListener {

        static final Logger logger = LoggerFactory.getLogger(DistributedLogClientImpl.class);

        private static class ServiceWithClient {
            final SocketAddress address;
            final Service<ThriftClientRequest, byte[]> client;
            final DistributedLogService.ServiceIface service;

            ServiceWithClient(SocketAddress address,
                              Service<ThriftClientRequest, byte[]> client,
                              DistributedLogService.ServiceIface service) {
                this.address = address;
                this.client = client;
                this.service = service;
            }
        }

        private final String clientName;
        private final ClientId clientId;
        private final ClientConfig clientConfig;
        private final RoutingService routingService;
        private final ClientBuilder clientBuilder;
        private final boolean streamFailfast;
        private final Pattern streamNameRegexPattern;

        // Timer
        private final HashedWheelTimer dlTimer;

        // region resolver
        private final RegionResolver regionResolver;

        // Ownership maintenance
        private final ConcurrentHashMap<String, SocketAddress> stream2Addresses =
                new ConcurrentHashMap<String, SocketAddress>();
        private final ConcurrentHashMap<SocketAddress, Set<String>> address2Streams =
                new ConcurrentHashMap<SocketAddress, Set<String>>();
        private final ConcurrentHashMap<SocketAddress, ServiceWithClient> address2Services =
                new ConcurrentHashMap<SocketAddress, ServiceWithClient>();

        // Close Status
        private boolean closed = false;
        private final ReentrantReadWriteLock closeLock =
                new ReentrantReadWriteLock();

        abstract class StreamOp implements TimerTask {
            final String stream;

            final AtomicInteger tries = new AtomicInteger(0);
            final RoutingContext routingContext = RoutingContext.of(regionResolver);
            final WriteContext ctx = new WriteContext();
            final Stopwatch stopwatch;
            SocketAddress nextAddressToSend;

            StreamOp(final String stream) {
                this.stream = stream;
                this.stopwatch = Stopwatch.createStarted();
            }

            void send(SocketAddress address) {
                long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                if (clientConfig.getMaxRedirects() > 0 &&
                        tries.get() >= clientConfig.getMaxRedirects()) {
                    fail(address, new RequestTimeoutException(Duration.fromMilliseconds(elapsedMs),
                            "Exhausted max redirects in " + elapsedMs + " ms"));
                    return;
                } else if (clientConfig.getRequestTimeoutMs() > 0 &&
                        elapsedMs >= clientConfig.getRequestTimeoutMs()) {
                    fail(address, new RequestTimeoutException(Duration.fromMilliseconds(elapsedMs),
                            "Exhausted max request timeout " + clientConfig.getRequestTimeoutMs()
                                    + " in " + elapsedMs + " ms"));
                    return;
                }
                synchronized (this) {
                    String addrStr = address.toString();
                    if (ctx.isSetTriedHosts() && ctx.getTriedHosts().contains(addrStr)) {
                        nextAddressToSend = address;
                        dlTimer.newTimeout(this,
                                Math.min(clientConfig.getRedirectBackoffMaxMs(),
                                        tries.get() * clientConfig.getRedirectBackoffStartMs()),
                                TimeUnit.MILLISECONDS);
                    } else {
                        doSend(address);
                    }
                }
            }

            abstract Future<ResponseHeader> sendRequest(ServiceWithClient sc);

            void doSend(SocketAddress address) {
                ctx.addToTriedHosts(address.toString());
                tries.incrementAndGet();
                sendWriteRequest(address, this);
            }

            void beforeComplete(ServiceWithClient sc, ResponseHeader responseHeader) {
                updateOwnership(stream, sc.address);
            }

            void complete(SocketAddress address) {
                stopwatch.stop();
                clientStats.completeRequest(address,
                        stopwatch.elapsed(TimeUnit.MICROSECONDS), tries.get());
            }

            void fail(SocketAddress address, Throwable t) {
                stopwatch.stop();
                clientStats.failRequest(address,
                        stopwatch.elapsed(TimeUnit.MICROSECONDS), tries.get());
            }

            @Override
            synchronized public void run(Timeout timeout) throws Exception {
                if (!timeout.isCancelled() && null != nextAddressToSend) {
                    doSend(nextAddressToSend);
                } else {
                    fail(null, new CancelledRequestException());
                }
            }
        }

        class BulkWriteOp extends StreamOp {

            final List<ByteBuffer> data;
            final ArrayList<Promise<DLSN>> results;

            BulkWriteOp(final String name, final List<ByteBuffer> data) {
                super(name);
                this.data = data;

                // This could take a while (relatively speaking) for very large inputs. We probably don't want
                // to go so large for other reasons though.
                this.results = new ArrayList<Promise<DLSN>>(data.size());
                for (int i = 0; i < data.size(); i++) {
                    this.results.add(new Promise<DLSN>());
                }
            }

            @Override
            Future<ResponseHeader> sendRequest(final ServiceWithClient sc) {
                return sc.service.writeBulkWithContext(stream, data, ctx).addEventListener(new FutureEventListener<BulkWriteResponse>() {
                    @Override
                    public void onSuccess(BulkWriteResponse response) {
                        // For non-success case, the ResponseHeader handler (the caller) will handle it.
                        // Note success in this case means no finagle errors have occurred (such as finagle connection issues).
                        // In general code != SUCCESS means there's some error reported by dlog service. The caller will handle such
                        // errors.
                        if (response.getHeader().getCode() == StatusCode.SUCCESS) {
                            beforeComplete(sc, response.getHeader());
                            BulkWriteOp.this.complete(sc.address, response);
                            if (response.getWriteResponses().size() == 0 && data.size() > 0) {
                                logger.error("non-empty bulk write got back empty response without failure for stream {}", stream);
                            }
                        }
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        // Handled by the ResponseHeader listener (attached by the caller).
                    }
                }).map(new AbstractFunction1<BulkWriteResponse, ResponseHeader>() {
                    @Override
                    public ResponseHeader apply(BulkWriteResponse response) {
                        // We need to return the ResponseHeader to the caller's listener to process DLOG errors.
                        return response.getHeader();
                    }
                });
            }

            void complete(SocketAddress address, BulkWriteResponse bulkWriteResponse) {
                super.complete(address);
                Iterator<WriteResponse> writeResponseIterator = bulkWriteResponse.getWriteResponses().iterator();
                Iterator<Promise<DLSN>> resultIterator = results.iterator();

                // Fill in errors from thrift responses.
                while (resultIterator.hasNext() && writeResponseIterator.hasNext()) {
                    Promise<DLSN> result = resultIterator.next();
                    WriteResponse writeResponse = writeResponseIterator.next();
                    if (StatusCode.SUCCESS == writeResponse.getHeader().getCode()) {
                        result.setValue(DLSN.deserialize(writeResponse.getDlsn()));
                    } else {
                        result.setException(DLException.of(writeResponse.getHeader()));
                    }
                }

                // Should never happen, but just in case so there's some record.
                if (bulkWriteResponse.getWriteResponses().size() != data.size()) {
                    logger.error("wrong number of results, response = {} records = ", bulkWriteResponse.getWriteResponses().size(), data.size());
                }
            }

            @Override
            void fail(SocketAddress address, Throwable t) {

                // StreamOp.fail is called to fail the overall request. In case of BulkWriteOp we take the request level
                // exception to apply to the first write. In fact for request level exceptions no request has ever been
                // attempted, but logically we associate the error with the first write.
                super.fail(address, t);
                Iterator<Promise<DLSN>> resultIterator = results.iterator();

                // Fail the first write with the batch level failure.
                if (resultIterator.hasNext()) {
                    Promise<DLSN> result = resultIterator.next();
                    result.setException(t);
                }

                // Fail the remaining writes as cancelled requests.
                while (resultIterator.hasNext()) {
                    Promise<DLSN> result = resultIterator.next();
                    result.setException(new CancelledRequestException());
                }
            }

            List<Future<DLSN>> result() {
                return (List) results;
            }
        }

        abstract class AbstractWriteOp extends StreamOp {

            final Promise<WriteResponse> result = new Promise<WriteResponse>();

            AbstractWriteOp(final String name) {
                super(name);
            }

            void complete(SocketAddress address, WriteResponse response) {
                super.complete(address);
                result.setValue(response);
            }

            @Override
            void fail(SocketAddress address, Throwable t) {
                super.fail(address, t);
                result.setException(t);
            }

            @Override
            Future<ResponseHeader> sendRequest(final ServiceWithClient sc) {
                return this.sendWriteRequest(sc).addEventListener(new FutureEventListener<WriteResponse>() {
                    @Override
                    public void onSuccess(WriteResponse response) {
                        if (response.getHeader().getCode() == StatusCode.SUCCESS) {
                            beforeComplete(sc, response.getHeader());
                            AbstractWriteOp.this.complete(sc.address, response);
                        }
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        // handled by the ResponseHeader listener
                    }
                }).map(new AbstractFunction1<WriteResponse, ResponseHeader>() {
                    @Override
                    public ResponseHeader apply(WriteResponse response) {
                        return response.getHeader();
                    }
                });
            }

            abstract Future<WriteResponse> sendWriteRequest(ServiceWithClient sc);
        }

        class WriteOp extends AbstractWriteOp {

            final ByteBuffer data;

            WriteOp(final String name, final ByteBuffer data) {
                super(name);
                this.data = data;
            }

            @Override
            Future<WriteResponse> sendWriteRequest(ServiceWithClient sc) {
                return sc.service.writeWithContext(stream, data, ctx);
            }

            Future<DLSN> result() {
                return result.map(new AbstractFunction1<WriteResponse, DLSN>() {
                    @Override
                    public DLSN apply(WriteResponse response) {
                        return DLSN.deserialize(response.getDlsn());
                    }
                });
            }
        }

        class TruncateOp extends AbstractWriteOp {
            final DLSN dlsn;

            TruncateOp(String name, DLSN dlsn) {
                super(name);
                this.dlsn = dlsn;
            }

            @Override
            Future<WriteResponse> sendWriteRequest(ServiceWithClient sc) {
                return sc.service.truncate(stream, dlsn.serialize(), ctx);
            }

            Future<Boolean> result() {
                return result.map(new AbstractFunction1<WriteResponse, Boolean>() {
                    @Override
                    public Boolean apply(WriteResponse response) {
                        return true;
                    }
                });
            }
        }

        class ReleaseOp extends AbstractWriteOp {

            ReleaseOp(String name) {
                super(name);
            }

            @Override
            Future<WriteResponse> sendWriteRequest(ServiceWithClient sc) {
                return sc.service.release(stream, ctx);
            }

            @Override
            void beforeComplete(ServiceWithClient sc, ResponseHeader header) {
                clearHostFromStream(stream, sc.address, "Stream Deleted");
            }

            Future<Void> result() {
                return result.map(new AbstractFunction1<WriteResponse, Void>() {
                    @Override
                    public Void apply(WriteResponse response) {
                        return null;
                    }
                });
            }
        }

        class DeleteOp extends AbstractWriteOp {

            DeleteOp(String name) {
                super(name);
            }

            @Override
            Future<WriteResponse> sendWriteRequest(ServiceWithClient sc) {
                return sc.service.delete(stream, ctx);
            }

            @Override
            void beforeComplete(ServiceWithClient sc, ResponseHeader header) {
                clearHostFromStream(stream, sc.address, "Stream Deleted");
            }

            Future<Void> result() {
                return result.map(new AbstractFunction1<WriteResponse, Void>() {
                    @Override
                    public Void apply(WriteResponse v1) {
                        return null;
                    }
                });
            }
        }

        class HeartbeatOp extends AbstractWriteOp {
            HeartbeatOptions options;

            HeartbeatOp(String name, boolean sendReaderHeartBeat) {
                super(name);
                options = new HeartbeatOptions();
                options.setSendHeartBeatToReader(sendReaderHeartBeat);
            }

            @Override
            Future<WriteResponse> sendWriteRequest(ServiceWithClient sc) {
                return sc.service.heartbeatWithOptions(stream, ctx, options);
            }

            Future<Void> result() {
                return result.map(new AbstractFunction1<WriteResponse, Void>() {
                    @Override
                    public Void apply(WriteResponse response) {
                        return null;
                    }
                });
            }
        }

        // Stats
        private final ClientStats clientStats;
        private final OwnershipStatsLogger ownershipStatsLogger;

        private DistributedLogClientImpl(String name,
                                         ClientId clientId,
                                         RoutingService routingService,
                                         ClientBuilder clientBuilder,
                                         ClientConfig clientConfig,
                                         StatsReceiver statsReceiver,
                                         StatsReceiver streamStatsReceiver,
                                         RegionResolver regionResolver,
                                         boolean enableRegionStats) {
            this.clientName = name;
            this.clientId = clientId;
            this.routingService = routingService;
            this.clientConfig = clientConfig;
            this.streamFailfast = clientConfig.getStreamFailfast();
            this.streamNameRegexPattern = Pattern.compile(clientConfig.getStreamNameRegex());
            this.regionResolver = regionResolver;
            // Build the timer
            this.dlTimer = new HashedWheelTimer(
                    new ThreadFactoryBuilder().setNameFormat("DLClient-" + name + "-timer-%d").build(),
                    this.clientConfig.getRedirectBackoffStartMs(),
                    TimeUnit.MILLISECONDS);
            // register routing listener
            this.routingService.registerListener(this);
            // Stats
            // Client Stats
            this.clientStats = new ClientStats(statsReceiver, enableRegionStats, regionResolver);
            // Ownership Stats
            this.ownershipStatsLogger = new OwnershipStatsLogger(statsReceiver, streamStatsReceiver);
            // Cache Stats
            StatsReceiver cacheStatReceiver = statsReceiver.scope("cache");
            Seq<String> numCachedStreamsGaugeName =
                    scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("num_streams")).toList();
            cacheStatReceiver.provideGauge(numCachedStreamsGaugeName, new Function0<Object>() {
                @Override
                public Object apply() {
                    return (float) stream2Addresses.size();
                }
            });
            Seq<String> numCachedHostsGaugeName =
                    scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("num_hosts")).toList();
            cacheStatReceiver.provideGauge(numCachedHostsGaugeName, new Function0<Object>() {
                @Override
                public Object apply() {
                    return (float) address2Services.size();
                }
            });

            // client builder
            ClientBuilder builder = setDefaultSettings(null == clientBuilder ? getDefaultClientBuilder() : clientBuilder);
            if (clientConfig.getThriftMux()) {
                builder = builder.stack(ThriftMux.client().withClientId(clientId));
            }
            this.clientBuilder = builder;
            logger.info("Build distributedlog client : name = {}, client_id = {}, routing_service = {}, stats_receiver = {}, thriftmux = {}",
                        new Object[] { name, clientId, routingService.getClass(), statsReceiver.getClass(), clientConfig.getThriftMux() });
        }

        private void handleServerInfo(SocketAddress address, ServerInfo value) {
            if (null != value && value.isSetOwnerships()) {
                Map<String, String> ownerships = value.getOwnerships();
                logger.info("Handshaked with {} : {} ownerships returned.", address, ownerships.size());
                for (Map.Entry<String, String> entry : ownerships.entrySet()) {
                    Matcher matcher = streamNameRegexPattern.matcher(entry.getKey());
                    if (!matcher.matches()) {
                        continue;
                    }
                    updateOwnership(entry.getKey(), entry.getValue());
                }
            } else {
                logger.info("Handshaked with {} : no ownerships returned", address);
            }
        }

        private void handshake(SocketAddress address, ServiceWithClient sc,
                               FutureEventListener<ServerInfo> listener) {
            if (clientConfig.getHandshakeWithClientInfo()) {
                ClientInfo clientInfo = new ClientInfo();
                clientInfo.setStreamNameRegex(clientConfig.getStreamNameRegex());
                logger.info("Handshaking with {} : {}", address, clientInfo);
                sc.service.handshakeWithClientInfo(clientInfo)
                        .addEventListener(listener);
            } else {
                logger.info("Handshaking with {}", address);
                sc.service.handshake().addEventListener(listener);
            }
        }

        @VisibleForTesting
        protected void handshake() {
            Map<SocketAddress, ServiceWithClient> snapshot =
                    new HashMap<SocketAddress, ServiceWithClient>(address2Services);
            logger.info("Handshaking with {} hosts.", snapshot.size());
            final CountDownLatch latch = new CountDownLatch(snapshot.size());
            for (Map.Entry<SocketAddress, ServiceWithClient> entry : snapshot.entrySet()) {
                final SocketAddress address = entry.getKey();
                handshake(entry.getKey(), entry.getValue(), new FutureEventListener<ServerInfo>() {
                    @Override
                    public void onSuccess(ServerInfo value) {
                        handleServerInfo(address, value);
                        latch.countDown();
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        latch.countDown();
                    }
                });
            }
            try {
                latch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.warn("Interrupted on handshaking with servers : ", e);
            }
            logger.info("Handshaked with {} hosts, cached {} streams", snapshot.size(), stream2Addresses.size());
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
                   .keepAlive(true);
        }

        @Override
        public void onServerLeft(SocketAddress address) {
            onServerLeft(address, null);
        }

        private void onServerLeft(SocketAddress address, ServiceWithClient sc) {
            clearAllStreamsForHost(address);
            if (null == sc) {
                removeClient(address);
            } else {
                removeClient(address, sc);
            }
        }

        @Override
        public void onServerJoin(SocketAddress address) {
            buildClient(address);
        }

        private ServiceWithClient buildClient(final SocketAddress address) {
            ServiceWithClient sc = address2Services.get(address);
            if (null != sc) {
                return sc;
            }
            // Build factory since DL proxy is kind of stateful service.
            Service<ThriftClientRequest, byte[]> client =
                    ClientBuilder.safeBuildFactory(
                            clientBuilder
                                    .hosts(address)
                                    .reportTo(clientStats.getFinagleStatsReceiver(address))
                    ).toService();
            DistributedLogService.ServiceIface service =
                    new DistributedLogService.ServiceToClient(client, new TBinaryProtocol.Factory());
            sc = new ServiceWithClient(address, client, service);
            ServiceWithClient oldSC = address2Services.putIfAbsent(address, sc);
            if (null != oldSC) {
                sc.client.close();
                return oldSC;
            } else {
                FutureEventListener<ServerInfo> listener = new FutureEventListener<ServerInfo>() {
                    @Override
                    public void onSuccess(ServerInfo value) {
                        handleServerInfo(address, value);
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        // nope
                    }
                };
                // send a ping messaging after creating connections.
                handshake(address, sc, listener);
                return sc;
            }
        }

        private void removeClient(SocketAddress address) {
            ServiceWithClient sc = address2Services.remove(address);
            if (null != sc) {
                logger.info("Removed host {}.", address);
                sc.client.close();
            }
        }

        private void removeClient(SocketAddress address, ServiceWithClient sc) {
            if (address2Services.remove(address, sc)) {
                logger.info("Remove client {} to host {}.", sc, address);
                sc.client.close();
            }
        }

        public void close() {
            closeLock.writeLock().lock();
            try {
                if (closed) {
                    return;
                }
                closed = true;
            } finally {
                closeLock.writeLock().unlock();
            }
            for (ServiceWithClient sc: address2Services.values()) {
                sc.client.close();
            }
            routingService.unregisterListener(this);
            routingService.stopService();
            dlTimer.stop();
        }

        @Override
        public Future<Void> check(String stream) {
            final HeartbeatOp op = new HeartbeatOp(stream, false);
            sendRequest(op);
            return op.result();
        }

        @Override
        public Future<Void> heartbeat(String stream) {
            final HeartbeatOp op = new HeartbeatOp(stream, true);
            sendRequest(op);
            return op.result();
        }

        @Override
        public Map<SocketAddress, Set<String>> getStreamOwnershipDistribution() {
            return ImmutableMap.copyOf(address2Streams);
        }

        @Override
        public Future<Void> setAcceptNewStream(boolean enabled) {
            Map<SocketAddress, ServiceWithClient> snapshot =
                    new HashMap<SocketAddress, ServiceWithClient>(address2Services);
            List<Future<Void>> futures = new ArrayList<Future<Void>>(snapshot.size());
            for (Map.Entry<SocketAddress, ServiceWithClient> entry : snapshot.entrySet()) {
                futures.add(entry.getValue().service.setAcceptNewStream(enabled));
            }
            return Future.collect(futures).map(new Function<List<Void>, Void>() {
                @Override
                public Void apply(List<Void> list) {
                    return null;
                }
            });
        }

        @Override
        public Future<DLSN> write(String stream, ByteBuffer data) {
            final WriteOp op = new WriteOp(stream, data);
            sendRequest(op);
            return op.result();
        }

        @Override
        public List<Future<DLSN>> writeBulk(String stream, List<ByteBuffer> data) {
            if (data.size() > 0) {
                final BulkWriteOp op = new BulkWriteOp(stream, data);
                sendRequest(op);
                return op.result();
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public Future<Boolean> truncate(String stream, DLSN dlsn) {
            final TruncateOp op = new TruncateOp(stream, dlsn);
            sendRequest(op);
            return op.result();
        }

        @Override
        public Future<Void> delete(String stream) {
            final DeleteOp op = new DeleteOp(stream);
            sendRequest(op);
            return op.result();
        }

        @Override
        public Future<Void> release(String stream) {
            final ReleaseOp op = new ReleaseOp(stream);
            sendRequest(op);
            return op.result();
        }

        private void sendRequest(final StreamOp op) {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    op.fail(null, new DLClientClosedException("Client " + clientName + " is closed."));
                } else {
                    doSend(op, null);
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }

        /**
         * Send the stream operation by routing service, excluding previous address if it is not null.
         *
         * @param op
         *          stream operation.
         * @param previousAddr
         *          previous tried address.
         */
        private void doSend(final StreamOp op, final SocketAddress previousAddr) {
            if (null != previousAddr) {
                op.routingContext.addTriedHost(previousAddr, StatusCode.WRITE_EXCEPTION);
            }
            // Get host first
            SocketAddress address = stream2Addresses.get(op.stream);
            if (null == address || op.routingContext.isTriedHost(address)) {
                ownershipStatsLogger.onMiss(op.stream);
                // pickup host by hashing
                try {
                    address = routingService.getHost(op.stream, op.routingContext);
                } catch (NoBrokersAvailableException nbae) {
                    op.fail(null, nbae);
                    return;
                }
            } else {
                ownershipStatsLogger.onHit(op.stream);
            }
            op.send(address);
        }

        private void sendWriteRequest(final SocketAddress addr, final StreamOp op) {
            // Get corresponding finagle client
            final ServiceWithClient sc = buildClient(addr);
            final long startTimeNanos = System.nanoTime();
            // write the request to that host.
            op.sendRequest(sc).addEventListener(new FutureEventListener<ResponseHeader>() {
                @Override
                public void onSuccess(ResponseHeader header) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received response; header: {}", header);
                    }
                    clientStats.completeProxyRequest(addr, header.getCode(), startTimeNanos);
                    // update routing context
                    op.routingContext.addTriedHost(addr, header.getCode());
                    switch (header.getCode()) {
                        case SUCCESS:
                            // success handling is done per stream op
                            break;
                        case FOUND:
                            handleRedirectResponse(header, op, addr);
                            break;
                        // for responses that indicate the requests definitely failed,
                        // we should fail them immediately (e.g. TOO_LARGE_RECORD, METADATA_EXCEPTION)
                        case NOT_IMPLEMENTED:
                        case METADATA_EXCEPTION:
                        case LOG_EMPTY:
                        case LOG_NOT_FOUND:
                        case TRUNCATED_TRANSACTION:
                        case END_OF_STREAM:
                        case TRANSACTION_OUT_OF_ORDER:
                        case INVALID_STREAM_NAME:
                        case OVER_CAPACITY:
                        case REQUEST_DENIED:
                        case TOO_LARGE_RECORD:
                        // status code NOT_READY is returned if failfast is enabled in the server. don't redirect
                        // since the proxy may still own the stream.
                        case STREAM_NOT_READY:
                            logger.error("Failed to write request to {} : {}", op.stream, header);
                            op.fail(addr, DLException.of(header));
                            break;
                        case SERVICE_UNAVAILABLE:
                            // service is unavailable, remove it out of routing service
                            routingService.removeHost(addr, new ServiceUnavailableException(addr + " is unavailable now."));
                            onServerLeft(addr);
                            clearHostFromStream(op.stream, addr, addr + " is unavailable now.");
                            // redirect the request to other host.
                            redirect(op, null);
                            break;
                        case REGION_UNAVAILABLE:
                            // region is unavailable, redirect the request to hosts in other region
                            redirect(op, null);
                            break;
                        case STREAM_UNAVAILABLE:
                        case ZOOKEEPER_ERROR:
                        case LOCKING_EXCEPTION:
                        case UNEXPECTED:
                        case INTERRUPTED:
                        case BK_TRANSMIT_ERROR:
                        case FLUSH_TIMEOUT:
                        default:
                            // when we are receiving these exceptions from proxy, it means proxy or the stream is closed
                            // redirect the request.
                            clearHostFromStream(op.stream, addr, header.getCode().name());
                            if (streamFailfast) {
                                logger.error("Failed to write request to {} : {}; skipping redirect to fail fast", op.stream, header);
                                op.fail(addr, DLException.of(header));
                            } else {
                                redirect(op, null);
                            }
                            break;
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    if (cause instanceof Failure) {
                        Failure failure = (Failure) cause;
                        if (failure.isFlagged(Failure.Wrapped())) {
                            try {
                                // if it is a wrapped failure, unwrap it first
                                cause = failure.show();
                            } catch (IllegalArgumentException iae) {
                                logger.warn("Failed to unwrap finagle failure of stream {} : ", op.stream, iae);
                            }
                        }
                    }
                    clientStats.failProxyRequest(addr, cause, startTimeNanos);
                    if (cause instanceof ConnectionFailedException || cause instanceof java.net.ConnectException) {
                        routingService.removeHost(addr, cause);
                        onServerLeft(addr, sc);
                        clearHostFromStream(op.stream, addr, cause.getMessage());
                        // redirect the request to other host.
                        doSend(op, addr);
                    } else if (cause instanceof ChannelException) {
                        // java.net.ConnectException typically means connection is refused remotely
                        // no process listening on remote address/port.
                        String reason;
                        if (cause.getCause() instanceof java.net.ConnectException) {
                            routingService.removeHost(addr, cause.getCause());
                            onServerLeft(addr);
                            reason = cause.getCause().getMessage();
                        } else {
                            routingService.removeHost(addr, cause);
                            reason = cause.getMessage();
                        }
                        clearHostFromStream(op.stream, addr, reason);
                        // redirect the request to other host.
                        doSend(op, addr);
                    } else if (cause instanceof ServiceTimeoutException) {
                        // redirect the request to itself again, which will backoff for a while
                        doSend(op, null);
                    } else if (cause instanceof WriteException) {
                        // redirect the request to other host.
                        doSend(op, addr);
                    } else if (cause instanceof ServiceException) {
                        // redirect the request to other host.
                        removeClient(addr, sc);
                        doSend(op, addr);
                    } else if (cause instanceof TApplicationException) {
                        handleTApplicationException(cause, op, addr, sc);
                    } else if (cause instanceof Failure) {
                        handleFinagleFailure((Failure) cause, op, addr);
                    } else {
                        // Default handler
                        handleException(cause, op, addr);
                    }
                }
            });
        }

        // Response Handlers

        /**
         * Redirect the request to new proxy <i>newAddr</i>. If <i>newAddr</i> is null,
         * it would pick up a host from routing service.
         *
         * @param op
         *          stream operation
         * @param newAddr
         *          new proxy address
         */
        void redirect(StreamOp op, SocketAddress newAddr) {
            ownershipStatsLogger.onRedirect(op.stream);
            if (null != newAddr) {
                logger.debug("Redirect request {} to new owner {}.", op, newAddr);
                op.send(newAddr);
            } else {
                doSend(op, null);
            }
        }

        void handleFinagleFailure(Failure failure, StreamOp op, SocketAddress addr) {
            if (failure.isFlagged(Failure.Restartable())) {
                // redirect the request to other host
                doSend(op, addr);
            } else {
                // fail the request if it is other types of failures
                handleException(failure, op, addr);
            }
        }

        void handleException(Throwable cause, StreamOp op, SocketAddress addr) {
            // RequestTimeoutException: fail it and let client decide whether to retry or not.

            // FailedFastException:
            // We don't actually know when FailedFastException will be thrown
            // so properly we just throw it back to application to let application
            // handle it.

            // Other Exceptions: as we don't know how to handle them properly so throw them to client
            logger.error("Failed to write request to {} @ {} : {}",
                    new Object[]{op.stream, addr, cause.toString()});
            op.fail(addr, cause);
        }

        void handleTApplicationException(Throwable cause, StreamOp op, SocketAddress addr, ServiceWithClient sc) {
            TApplicationException ex = (TApplicationException) cause;
            if (ex.getType() == TApplicationException.UNKNOWN_METHOD) {
                // remove client and redirect the request to other host
                removeClient(addr, sc);
                doSend(op, addr);
            } else {
                handleException(cause, op, addr);
            }
        }

        void handleRedirectResponse(ResponseHeader header, StreamOp op, SocketAddress curAddr) {
            SocketAddress ownerAddr = null;
            if (header.isSetLocation()) {
                String owner = header.getLocation();
                try {
                    ownerAddr = DLSocketAddress.deserialize(owner).getSocketAddress();
                    // if we are receiving a direct request to same host, we won't try the same host.
                    // as the proxy will shut itself down if it redirects client to itself.
                    if (curAddr.equals(ownerAddr)) {
                        logger.warn("Request to stream {} is redirected to same server {}!", op.stream, curAddr);
                        ownerAddr = null;
                    } else {
                        // update ownership when redirects.
                        updateOwnership(op.stream, ownerAddr);
                    }
                } catch (IOException e) {
                    ownerAddr = null;
                }
            }
            redirect(op, ownerAddr);
        }

        void updateOwnership(String stream, String location) {
            try {
                SocketAddress ownerAddr = DLSocketAddress.deserialize(location).getSocketAddress();
                // update ownership
                updateOwnership(stream, ownerAddr);
            } catch (IOException e) {
                logger.warn("Invalid ownership {} found for stream {} : ",
                            new Object[] { location, stream, e });
            }
        }

        // Ownership Operations

        /**
         * Update ownership of <i>stream</i> to <i>addr</i>.
         *
         * @param stream
         *          Stream Name.
         * @param addr
         *          Owner Address.
         */
        void updateOwnership(String stream, SocketAddress addr) {
            // update ownership
            SocketAddress oldAddr = stream2Addresses.putIfAbsent(stream, addr);
            if (null != oldAddr && oldAddr.equals(addr)) {
                return;
            }
            if (null != oldAddr) {
                if (stream2Addresses.replace(stream, oldAddr, addr)) {
                    // Store the relevant mappings for this topic and host combination
                    logger.info("Storing ownership for stream : {}, old host : {}, new host : {}.",
                            new Object[] { stream, oldAddr, addr });
                    StringBuilder sb = new StringBuilder();
                    sb.append("Ownership changed '")
                      .append(oldAddr).append("' -> '").append(addr).append("'");
                    clearHostFromStream(stream, oldAddr, sb.toString());

                    // update stats
                    ownershipStatsLogger.onRemove(stream);
                    ownershipStatsLogger.onAdd(stream);
                } else {
                    logger.warn("Ownership of stream : {} has been changed from {} to {} when storing host : {}.",
                            new Object[] { stream, oldAddr, stream2Addresses.get(stream), addr });
                    return;
                }
            } else {
                logger.info("Storing ownership for stream : {}, host : {}.", stream, addr);
                // update stats
                ownershipStatsLogger.onAdd(stream);
            }

            Set<String> streamsForHost = address2Streams.get(addr);
            if (null == streamsForHost) {
                Set<String> newStreamsForHost = new HashSet<String>();
                streamsForHost = address2Streams.putIfAbsent(addr, newStreamsForHost);
                if (null == streamsForHost) {
                    streamsForHost = newStreamsForHost;
                }
            }
            synchronized (streamsForHost) {
                // check whether the ownership changed, since it might happend after replace succeed
                if (addr.equals(stream2Addresses.get(stream))) {
                    streamsForHost.add(stream);
                }
            }
        }

        /**
         * If a server host goes down or the channel to it gets disconnected, we need to clear out
         * all relevant cached information.
         *
         * @param addr
         *          host goes down
         */
        void clearAllStreamsForHost(SocketAddress addr) {
            logger.info("Remove streams mapping for host {}", addr);
            Set<String> streamsForHost = address2Streams.get(addr);
            if (null != streamsForHost) {
                synchronized (streamsForHost) {
                    for (String s : streamsForHost) {
                        if (stream2Addresses.remove(s, addr)) {
                            logger.info("Removing mapping for stream : {} from host : {}", s, addr);
                            ownershipStatsLogger.onRemove(s);
                        }
                    }
                    address2Streams.remove(addr, streamsForHost);
                }
            }
        }

        /**
         * If a stream moved, we only clear out that stream for the host and not all cached information.
         *
         * @param stream
         *          Stream Name.
         * @param addr
         *          Owner Address.
         * @param reason
         *          reason to remove ownership
         */
        void clearHostFromStream(String stream, SocketAddress addr, String reason) {
            if (stream2Addresses.remove(stream, addr)) {
                logger.info("Removed stream to host mapping for (stream: {} -> host: {}) : reason = '{}'.",
                        new Object[] { stream, addr, reason });
            }
            Set<String> streamsForHost = address2Streams.get(addr);
            if (null != streamsForHost) {
                synchronized (streamsForHost) {
                    if (streamsForHost.remove(stream)) {
                        logger.info("Removed stream ({}) from host {} : reason = '{}'.",
                                new Object[] { stream, addr, reason });
                        if (streamsForHost.isEmpty()) {
                            address2Streams.remove(addr, streamsForHost);
                        }
                        ownershipStatsLogger.onRemove(stream);
                    }
                }
            }
        }
    }

    static long elapsedMicroSec(long startNanoTime) {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanoTime);
    }
}
