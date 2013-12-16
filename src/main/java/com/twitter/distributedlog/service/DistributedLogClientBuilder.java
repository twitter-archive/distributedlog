package com.twitter.distributedlog.service;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.exceptions.DLClientClosedException;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.finagle.CancelledRequestException;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.RequestTimeoutException;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.Counter;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DistributedLogClientBuilder {

    private String _name = null;
    private ClientId _clientId = null;
    private RoutingService _routingService = null;
    private ClientBuilder _clientBuilder = null;
    private StatsReceiver _statsReceiver = new NullStatsReceiver();
    private final ClientConfig _clientConfig = new ClientConfig();

    /**
     * Create a client builder
     *
     * @return client builder
     */
    public static DistributedLogClientBuilder newBuilder() {
        return new DistributedLogClientBuilder();
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
        this._name = name;
        return this;
    }

    /**
     * Client ID.
     *
     * @param clientId
     *          client id
     * @return client builder.
     */
    public DistributedLogClientBuilder clientId(ClientId clientId) {
        this._clientId = clientId;
        return this;
    }

    /**
     * Serverset to access proxy services.
     *
     * @param serverSet
     *          server set.
     * @return client builder.
     */
    public DistributedLogClientBuilder serverSet(ServerSet serverSet) {
        this._routingService = new ServerSetRoutingService(serverSet);
        return this;
    }

    /**
     * Stats receiver to expose client stats.
     *
     * @param statsReceiver
     *          stats receiver.
     * @return client builder.
     */
    public DistributedLogClientBuilder statsReceiver(StatsReceiver statsReceiver) {
        this._statsReceiver = statsReceiver;
        return this;
    }

    public DistributedLogClientBuilder clientBuilder(ClientBuilder builder) {
        this._clientBuilder = builder;
        return this;
    }

    /**
     * Backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public DistributedLogClientBuilder redirectBackoffStartMs(int ms) {
        this._clientConfig.setRedirectBackoffStartMs(ms);
        return this;
    }

    /**
     * Max backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public DistributedLogClientBuilder redirectBackoffMaxMs(int ms) {
        this._clientConfig.setRedirectBackoffMaxMs(ms);
        return this;
    }

    public DistributedLogClient build() {
        Preconditions.checkNotNull(_name, "No name provided.");
        Preconditions.checkNotNull(_clientId, "No client id provided.");
        Preconditions.checkNotNull(_routingService, "No routing service provided.");
        Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided.");

        DistributedLogClientImpl clientImpl =
                new DistributedLogClientImpl(_name, _clientId, _routingService, _clientBuilder, _clientConfig, _statsReceiver);
        _routingService.startService();
        clientImpl.handshake();
        return clientImpl;
    }

    static class ClientConfig {
        int redirectBackoffStartMs = 25;
        int redirectBackoffMaxMs = 100;

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
    }

    static private class DistributedLogClientImpl implements DistributedLogClient, RoutingService.RoutingListener {

        static final Logger logger = LoggerFactory.getLogger(DistributedLogClientImpl.class);

        private static class ServiceWithClient {
            final Service<ThriftClientRequest, byte[]> client;
            final DistributedLogService.ServiceIface service;

            ServiceWithClient(Service<ThriftClientRequest, byte[]> client,
                              DistributedLogService.ServiceIface service) {
                this.client = client;
                this.service = service;
            }
        }

        private final String name;
        private final ClientId clientId;
        private final ClientConfig clientConfig;
        private final RoutingService routingService;
        private final ClientBuilder clientBuilder;

        // Timer
        private final HashedWheelTimer dlTimer;

        // Ownership maintenance
        private final ConcurrentHashMap<String, SocketAddress> stream2Addresses =
                new ConcurrentHashMap<String, SocketAddress>();
        private final ConcurrentHashMap<SocketAddress, ServiceWithClient> address2Services =
                new ConcurrentHashMap<SocketAddress, ServiceWithClient>();

        // Close Status
        private boolean closed = false;
        private final ReentrantReadWriteLock closeLock =
                new ReentrantReadWriteLock();

        // Stats
        private static class OwnershipStat {
            private final Counter hits;
            private final Counter misses;
            private final Counter removes;
            private final Counter redirects;
            private final Counter adds;

            OwnershipStat(StatsReceiver ownershipStats) {
                hits = ownershipStats.counter0("hits");
                misses = ownershipStats.counter0("misses");
                adds = ownershipStats.counter0("adds");
                removes = ownershipStats.counter0("removes");
                redirects = ownershipStats.counter0("redirects");
            }

            void onHit() {
                hits.incr();
            }

            void onMiss() {
                misses.incr();
            }

            void onAdd() {
                adds.incr();
            }

            void onRemove() {
                removes.incr();
            }

            void onRedirect() {
                redirects.incr();
            }

        }

        private final StatsReceiver statsReceiver;
        private final Stat redirectStat;
        private final StatsReceiver responseStatsReceiver;
        private final ConcurrentMap<StatusCode, Counter> responseStats =
                new ConcurrentHashMap<StatusCode, Counter>();
        private final StatsReceiver exceptionStatsReceiver;
        private final ConcurrentMap<Class<?>, Counter> exceptionStats =
                new ConcurrentHashMap<Class<?>, Counter>();
        private final OwnershipStat ownershipStat;
        private final StatsReceiver ownershipStatsReceiver;
        private final ConcurrentMap<String, OwnershipStat> ownershipStats =
                new ConcurrentHashMap<String, OwnershipStat>();

        private Counter getResponseCounter(StatusCode code) {
            Counter counter = responseStats.get(code);
            if (null == counter) {
                Counter newCounter = responseStatsReceiver.counter0(code.name());
                Counter oldCounter = responseStats.putIfAbsent(code, newCounter);
                counter = null != oldCounter ? oldCounter : newCounter;
            }
            return counter;
        }

        private Counter getExceptionCounter(Class<?> cls) {
            Counter counter = exceptionStats.get(cls);
            if (null == counter) {
                Counter newCounter = exceptionStatsReceiver.counter0(cls.getName());
                Counter oldCounter = exceptionStats.putIfAbsent(cls, newCounter);
                counter = null != oldCounter ? oldCounter : newCounter;
            }
            return counter;
        }

        private OwnershipStat getOwnershipStat(String stream) {
            OwnershipStat stat = ownershipStats.get(stream);
            if (null == stat) {
                OwnershipStat newStat = new OwnershipStat(ownershipStatsReceiver.scope(stream));
                OwnershipStat oldStat = ownershipStats.putIfAbsent(stream, newStat);
                stat = null != oldStat ? oldStat : newStat;
            }
            return stat;
        }

        class WriteOp implements TimerTask {
            final String stream;
            final ByteBuffer data;
            final Promise<DLSN> result = new Promise<DLSN>();
            final AtomicInteger tries = new AtomicInteger(0);
            final WriteContext ctx = new WriteContext();
            SocketAddress nextAddressToSend;

            WriteOp(final String stream, final ByteBuffer data) {
                this.stream = stream;
                this.data = data;
            }

            synchronized void send(SocketAddress address) {
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

            void doSend(SocketAddress address) {
                ctx.addToTriedHosts(address.toString());
                tries.incrementAndGet();
                sendWriteRequest(address, this);
            }

            void complete(DLSN dlsn) {
                redirectStat.add(tries.get());
                result.setValue(dlsn);
            }

            void fail(Throwable t) {
                redirectStat.add(tries.get());
                result.setException(t);
            }

            @Override
            synchronized public void run(Timeout timeout) throws Exception {
                if (!timeout.isCancelled() && null != nextAddressToSend) {
                    doSend(nextAddressToSend);
                } else {
                    fail(new CancelledRequestException());
                }
            }
        }

        private DistributedLogClientImpl(String name,
                                         ClientId clientId,
                                         RoutingService routingService,
                                         ClientBuilder clientBuilder,
                                         ClientConfig clientConfig,
                                         StatsReceiver statsReceiver) {
            this.name = name;
            this.clientId = clientId;
            this.routingService = routingService;
            this.clientConfig = clientConfig;
            this.statsReceiver = statsReceiver;
            // Build the timer
            this.dlTimer = new HashedWheelTimer(
                    new ThreadFactoryBuilder().setNameFormat("DLClient-" + name + "-timer-%d").build(),
                    this.clientConfig.getRedirectBackoffStartMs(),
                    TimeUnit.MILLISECONDS);
            // register routing listener
            this.routingService.registerListener(this);
            // Stats
            ownershipStat = new OwnershipStat(statsReceiver.scope("ownership"));
            ownershipStatsReceiver = statsReceiver.scope("perstream_ownership");
            StatsReceiver redirectStatReceiver = statsReceiver.scope("redirects");
            redirectStat = redirectStatReceiver.stat0("times");
            responseStatsReceiver = statsReceiver.scope("responses");
            exceptionStatsReceiver = statsReceiver.scope("exceptions");

            // client builder
            if (null == clientBuilder) {
                this.clientBuilder = getDefaultClientBuilder();
            } else {
                this.clientBuilder = setDefaultSettings(clientBuilder);
            }
        }

        private ServerInfo handshake() {
            Map<SocketAddress, ServiceWithClient> snapshot =
                    new HashMap<SocketAddress, ServiceWithClient>(address2Services);
            final CountDownLatch latch = new CountDownLatch(snapshot.size());
            final AtomicReference<ServerInfo> result = new AtomicReference<ServerInfo>(null);
            FutureEventListener<ServerInfo> listener = new FutureEventListener<ServerInfo>() {
                @Override
                public void onSuccess(ServerInfo value) {
                    result.compareAndSet(null, value);
                    latch.countDown();
                }
                @Override
                public void onFailure(Throwable cause) {
                    latch.countDown();
                }
            };
            for (Map.Entry<SocketAddress, ServiceWithClient> entry : snapshot.entrySet()) {
                logger.info("Handshaking with {}", entry.getKey());
                entry.getValue().service.handshake().addEventListener(listener);
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warn("Interrupted on handshaking with servers : ", e);
            }
            return result.get();
        }

        private ClientBuilder getDefaultClientBuilder() {
            return ClientBuilder.get()
                .name(name)
                .codec(ThriftClientFramedCodec.apply(Option.apply(clientId)))
                .hostConnectionLimit(10)
                .connectionTimeout(Duration.fromSeconds(600))
                .requestTimeout(Duration.fromSeconds(600))
                .reportTo(statsReceiver);
        }

        private ClientBuilder setDefaultSettings(ClientBuilder builder) {
            return builder.name(name)
                   .codec(ThriftClientFramedCodec.apply(Option.apply(clientId)))
                   .reportTo(statsReceiver);
        }

        @Override
        public void onServerLeft(SocketAddress address) {
            removeClient(address);
        }

        @Override
        public void onServerJoin(SocketAddress address) {
            buildClient(address);
        }

        private ServiceWithClient buildClient(SocketAddress address) {
            ServiceWithClient sc = address2Services.get(address);
            if (null != sc) {
                return sc;
            }
            Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(clientBuilder.hosts(address));
            DistributedLogService.ServiceIface service =
                    new DistributedLogService.ServiceToClient(client, new TBinaryProtocol.Factory());
            sc = new ServiceWithClient(client, service);
            ServiceWithClient oldSC = address2Services.putIfAbsent(address, sc);
            if (null != oldSC) {
                sc.client.close();
                return oldSC;
            } else {
                // send a ping messaging after creating connections.
                sc.service.handshake();
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
        public Future<DLSN> write(String stream, ByteBuffer data) {
            final WriteOp op = new WriteOp(stream, data);
            closeLock.readLock().lock();
            try {
                if (closed) {
                    op.fail(new DLClientClosedException("Client " + name + " is closed."));
                } else {
                    doWrite(op, null);
                }
            } finally {
                closeLock.readLock().unlock();
            }
            return op.result;
        }

        private void doWrite(final WriteOp op, SocketAddress previousAddr) {
            // Get host first
            SocketAddress address = stream2Addresses.get(op.stream);
            if (null == address || address.equals(previousAddr)) {
                ownershipStat.onMiss();
                getOwnershipStat(op.stream).onMiss();
                // pickup host by hashing
                try {
                    address = routingService.getHost(op.stream, previousAddr);
                } catch (NoBrokersAvailableException nbae) {
                    op.fail(nbae);
                    return;
                }
            } else {
                ownershipStat.onHit();
                getOwnershipStat(op.stream).onHit();
            }
            op.send(address);
        }

        private void sendWriteRequest(final SocketAddress addr, final WriteOp op) {
            // Get corresponding finagle client
            final ServiceWithClient sc = buildClient(addr);
            // write the request to that host.
            sc.service.writeWithContext(op.stream, op.data, op.ctx).addEventListener(new FutureEventListener<WriteResponse>() {
                @Override
                public void onSuccess(WriteResponse response) {
                    getResponseCounter(response.getHeader().getCode()).incr();
                    switch (response.getHeader().getCode()) {
                    case SUCCESS:
                        // update ownership
                        SocketAddress oldAddr = stream2Addresses.putIfAbsent(op.stream, addr);
                        if (null == oldAddr) {
                            ownershipStat.onAdd();
                            getOwnershipStat(op.stream).onAdd();
                        } else if (!oldAddr.equals(addr)) {
                            if (stream2Addresses.replace(op.stream, oldAddr, addr)) {
                                // ownership changed
                                ownershipStat.onRemove();
                                ownershipStat.onAdd();
                                getOwnershipStat(op.stream).onRemove();
                                getOwnershipStat(op.stream).onAdd();
                            }
                        }
                        op.complete(DLSN.deserialize(response.getDlsn()));
                        break;
                    case FOUND:
                        if (response.getHeader().isSetLocation()) {
                            String owner = response.getHeader().getLocation();
                            SocketAddress ownerAddr;
                            try {
                                ownerAddr = DLSocketAddress.deserialize(owner).getSocketAddress();
                            } catch (IOException e) {
                                // invalid owner
                                ownershipStat.onRedirect();
                                getOwnershipStat(op.stream).onRedirect();
                                doWrite(op, addr);
                                return;
                            }
                            if (addr.equals(ownerAddr)) {
                                // throw the exception to the client
                                op.fail(new ServiceUnavailableException(
                                        String.format("Request to stream %s is redirected to same server %s!", op.stream, addr)));
                                return;
                            }
                            ownershipStat.onRedirect();
                            getOwnershipStat(op.stream).onRedirect();
                            // redirect the request.
                            if (!stream2Addresses.replace(op.stream, addr, ownerAddr)) {
                                // ownership already changed
                                SocketAddress newOwner = stream2Addresses.get(op.stream);
                                if (null == newOwner) {
                                    SocketAddress newOwner2 = stream2Addresses.putIfAbsent(op.stream, ownerAddr);
                                    if (null != newOwner2) {
                                        ownerAddr = newOwner2;
                                    }
                                } else {
                                    ownerAddr = newOwner;
                                }
                            }

                            logger.debug("Redirect the request to new owner {}.", ownerAddr);
                            op.send(ownerAddr);
                        } else {
                            // no owner found
                            doWrite(op, addr);
                        }
                        break;
                    case SERVICE_UNAVAILABLE:
                        // we are receiving SERVICE_UNAVAILABLE exception from proxy, it means proxy or the stream is closed
                        // redirect the request.
                        if (stream2Addresses.remove(op.stream, addr)) {
                            ownershipStat.onRemove();
                            getOwnershipStat(op.stream).onRemove();
                        }
                        ownershipStat.onRedirect();
                        getOwnershipStat(op.stream).onRedirect();
                        doWrite(op, addr);
                        break;
                    default:
                        logger.error("Failed to write request to {} : {}", op.stream, response);
                        // server side exceptions, throw to the client.
                        // remove ownership to that host
                        if (stream2Addresses.remove(op.stream, addr)) {
                            ownershipStat.onRemove();
                            getOwnershipStat(op.stream).onRemove();
                        }
                        // throw the exception to the client
                        op.fail(DLException.of(response.getHeader()));
                        break;
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    getExceptionCounter(cause.getClass()).incr();
                    if (cause instanceof RequestTimeoutException) {
                        op.fail(cause);
                    } else {
                        logger.error("Failed to write request to {} @ {} : {}",
                                new Object[] { op.stream, addr, null != cause.getMessage() ? cause.getMessage() : cause.getClass() });
                        /** NOTE: we don't need to change ownership here. since it might be due to transient issue.
                         *        we don't need to remove the client, as retry will exclude this. if this is an transient issue,
                         *        it would retry the client again. if it is due to server is down, serverset will remove the client.
                        // remove from host list
                        routingService.removeHost(addr, cause);
                        // remove ownership to that host
                        if (stream2Addresses.remove(op.stream, addr)) {
                            ownershipRemoves.incr();
                        }
                        **/
                        // remove this client
                        removeClient(addr, sc);
                        // do a retry
                        doWrite(op, addr);
                    }
                }
            });
        }
    }
}
