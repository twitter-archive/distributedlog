package com.twitter.distributedlog.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.RequestTimeoutException;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.Counter;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DistributedLogClientBuilder {

    private String _name = null;
    private ClientId _clientId = null;
    private ServerSet _serverSet = null;
    private StatsReceiver _statsReceiver = new NullStatsReceiver();

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

    public DistributedLogClientBuilder name(String name) {
        this._name = name;
        return this;
    }

    public DistributedLogClientBuilder clientId(ClientId clientId) {
        this._clientId = clientId;
        return this;
    }

    public DistributedLogClientBuilder serverSet(ServerSet serverSet) {
        this._serverSet = serverSet;
        return this;
    }

    public DistributedLogClientBuilder statsReceiver(StatsReceiver statsReceiver) {
        this._statsReceiver = statsReceiver;
        return this;
    }

    public DistributedLogClient build() {
        Preconditions.checkNotNull(_name, "No name provided.");
        Preconditions.checkNotNull(_clientId, "No client id provided.");
        Preconditions.checkNotNull(_serverSet, "No cluster provided.");
        Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided.");

        DistributedLogClientImpl clientImpl =
                new DistributedLogClientImpl(_name, _clientId, _serverSet, _statsReceiver);
        clientImpl.start();
        clientImpl.waitForFirstChange();
        return clientImpl;
    }

    static private class DistributedLogClientImpl extends Thread implements DistributedLogClient {

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
        private final ServerSet serverSet;

        private final ConcurrentHashMap<String, SocketAddress> stream2Addresses =
                new ConcurrentHashMap<String, SocketAddress>();
        private final Set<SocketAddress> hostSet = new HashSet<SocketAddress>();
        private List<SocketAddress> hostList = new ArrayList<SocketAddress>();
        private final ConcurrentHashMap<SocketAddress, ServiceWithClient> address2Services =
                new ConcurrentHashMap<SocketAddress, ServiceWithClient>();
        private final HashFunction hasher = Hashing.md5();

        private boolean closed = false;
        private final ReentrantReadWriteLock closeLock =
                new ReentrantReadWriteLock();

        // Server Set Changes
        private final AtomicReference<ImmutableSet<ServiceInstance>> serverSetChange =
                new AtomicReference<ImmutableSet<ServiceInstance>>(null);
        private final CountDownLatch changeLatch = new CountDownLatch(1);

        // Stats
        private final StatsReceiver statsReceiver;
        private final Counter ownershipHits;
        private final Counter ownershipMisses;
        private final Counter ownershipRemoves;
        private final Counter ownershipRedirects;
        private final Counter ownershipAdds;

        private static class HostComparator implements Comparator<SocketAddress> {

            static final HostComparator instance = new HostComparator();

            @Override
            public int compare(SocketAddress o1, SocketAddress o2) {
                return o1.toString().compareTo(o2.toString());
            }
        }

        private DistributedLogClientImpl(String name,
                                         ClientId clientId,
                                         ServerSet serverSet,
                                         StatsReceiver statsReceiver) {
            super("DistributedLogClient-" + name);
            this.name = name;
            this.clientId = clientId;
            this.serverSet = serverSet;
            this.statsReceiver = statsReceiver;
            StatsReceiver ownershipStats = statsReceiver.scope("ownership");
            ownershipHits = ownershipStats.counter0("hits");
            ownershipMisses = ownershipStats.counter0("misses");
            ownershipAdds = ownershipStats.counter0("adds");
            ownershipRemoves = ownershipStats.counter0("removes");
            ownershipRedirects = ownershipStats.counter0("redirects");
        }

        private ServiceWithClient buildClient(SocketAddress address) {
            ServiceWithClient sc = address2Services.get(address);
            if (null != sc) {
                return sc;
            }
            Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get()
                    .name(name)
                    .codec(ThriftClientFramedCodec.apply(Option.apply(clientId)))
                    .hosts(address)
                    .hostConnectionLimit(10)
                    .connectionTimeout(Duration.fromSeconds(600))
                    .requestTimeout(Duration.fromSeconds(600))
                    .reportTo(statsReceiver));
            DistributedLogService.ServiceIface service =
                    new DistributedLogService.ServiceToClient(client, new TBinaryProtocol.Factory());
            sc = new ServiceWithClient(client, service);
            ServiceWithClient oldSC = address2Services.putIfAbsent(address, sc);
            if (null != oldSC) {
                sc.client.close();
                return oldSC;
            } else {
                address2Services.put(address, sc);
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

        void waitForFirstChange() {
            try {
                if (!changeLatch.await(60, TimeUnit.MINUTES)) {
                    logger.warn("No serverset change received in 1 minute.");
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted waiting first serverset change : ", e);
            }
        }

        @Override
        public void run() {
            try {
                serverSet.monitor(new DynamicHostSet.HostChangeMonitor<ServiceInstance>() {
                    @Override
                    public void onChange(ImmutableSet<ServiceInstance> serviceInstances) {
                        ImmutableSet<ServiceInstance> lastValue = serverSetChange.getAndSet(serviceInstances);
                        if (null == lastValue) {
                            ImmutableSet<ServiceInstance> mostRecentValue;
                            do {
                                mostRecentValue = serverSetChange.get();
                                performServerSetChange(mostRecentValue);
                                changeLatch.countDown();
                            } while (!serverSetChange.compareAndSet(mostRecentValue, null));
                        }
                    }
                });
            } catch (DynamicHostSet.MonitorException e) {
                logger.error("Fail to monitor server set : ", e);
                Runtime.getRuntime().exit(-1);
            }
        }

        private synchronized void performServerSetChange(ImmutableSet<ServiceInstance> serverSet) {
            Set<SocketAddress> newSet = new HashSet<SocketAddress>();
            for (ServiceInstance serviceInstance : serverSet) {
                Endpoint endpoint = serviceInstance.getAdditionalEndpoints().get("thrift");
                SocketAddress address = new InetSocketAddress(endpoint.getHost(), endpoint.getPort());
                newSet.add(address);
            }

            Set<SocketAddress> removed;
            Set<SocketAddress> added;
            synchronized (hostSet) {
                removed = Sets.difference(hostSet, newSet).immutableCopy();
                added = Sets.difference(newSet, hostSet).immutableCopy();
                for (SocketAddress node: removed) {
                    if (hostSet.remove(node)) {
                        logger.info("Node {} left.", node);
                    }
                }
                for (SocketAddress node: added) {
                    if (hostSet.add(node)) {
                        logger.info("Node {} joined.", node);
                    }
                }
            }

            for (SocketAddress addr : removed) {
                removeClient(addr);
            }

            for (SocketAddress addr : added) {
                buildClient(addr);
            }

            synchronized (hostSet) {
                hostList = new ArrayList<SocketAddress>(hostSet);
                Collections.sort(hostList, HostComparator.instance);
                logger.info("Host list becomes : {}.", hostList);
            }

        }

        private void removeSocketAddress(SocketAddress host, Throwable t) {
            synchronized (hostSet) {
                if (hostSet.remove(host)) {
                    logger.info("Node {} left due to : ", host, t);
                }
                hostList = new ArrayList<SocketAddress>(hostSet);
                Collections.sort(hostList, HostComparator.instance);
                logger.info("Host list becomes : {}.", hostList);
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
        }

        @Override
        public Future<DLSN> write(String stream, ByteBuffer data) {
            final Promise<DLSN> result = new Promise<DLSN>();
            closeLock.readLock().lock();
            try {
                if (closed) {
                    // TODO: more specific exception
                    result.setException(new IOException("Client is closed."));
                } else {
                    doWrite(stream, data, result, null);
                }
            } finally {
                closeLock.readLock().unlock();
            }
            return result;
        }

        private void doWrite(final String stream, final ByteBuffer data,
                             final Promise<DLSN> result, SocketAddress previousAddr) {
            // Get host first
            SocketAddress address = stream2Addresses.get(stream);
            if (null == address) {
                ownershipMisses.incr();
                // pickup host by hashing
                synchronized (hostSet) {
                    if (0 != hostList.size()) {
                        int hashCode = hasher.hashString(stream).asInt();
                        int hostId = MathUtils.signSafeMod(hashCode, hostList.size());
                        address = hostList.get(hostId);
                        if (null != previousAddr && address.equals(previousAddr)) {
                            ArrayList<SocketAddress> newList = new ArrayList<SocketAddress>(hostList);
                            newList.remove(hostId);
                            // pickup a new host by rehashing it.
                            hostId = MathUtils.signSafeMod(hashCode, newList.size());
                            address = newList.get(hostId);
                            int i = hostId;
                            while (previousAddr.equals(address)) {
                                i = (i+1) % newList.size();
                                if (i == hostId) {
                                    result.setException(new NoBrokersAvailableException("No host is available."));
                                    return;
                                }
                                address = newList.get(i);
                            }
                        }
                    }
                }
            } else {
                ownershipHits.incr();
            }
            if (null == address) {
                result.setException(new NoBrokersAvailableException("No hosts found"));
                return;
            }
            sendWriteRequest(address, stream, data, result);
        }

        private void sendWriteRequest(final SocketAddress addr,
                                      final String stream, final ByteBuffer data, final Promise<DLSN> result) {
            // Get corresponding finagle client
            final ServiceWithClient sc = buildClient(addr);
            // write the request to that host.
            sc.service.write(stream, data).addEventListener(new FutureEventListener<WriteResponse>() {
                @Override
                public void onSuccess(WriteResponse response) {
                    switch (response.getHeader().getCode()) {
                    case SUCCESS:
                        // update ownership
                        if (null == stream2Addresses.putIfAbsent(stream, addr)) {
                            ownershipAdds.incr();
                        }
                        result.setValue(DLSN.deserialize(response.getDlsn()));
                        break;
                    case FOUND:
                        if (response.getHeader().isSetLocation()) {
                            String owner = response.getHeader().getLocation();
                            SocketAddress ownerAddr;
                            try {
                                ownerAddr = DLSocketAddress.deserialize(owner).getSocketAddress();
                            } catch (IOException e) {
                                // invalid owner
                                ownershipRedirects.incr();
                                doWrite(stream, data, result, addr);
                                return;
                            }
                            if (addr.equals(ownerAddr)) {
                                // throw the exception to the client
                                result.setException(new ServiceUnavailableException(
                                        String.format("Request to stream %s is redirected to same server %s!", stream, addr)));
                                return;
                            }
                            ownershipRedirects.incr();
                            // redirect the request.
                            if (!stream2Addresses.replace(stream, addr, ownerAddr)) {
                                // ownership already changed
                                SocketAddress newOwner = stream2Addresses.get(stream);
                                if (null == newOwner) {
                                    SocketAddress newOwner2 = stream2Addresses.putIfAbsent(stream, ownerAddr);
                                    if (null != newOwner2) {
                                        ownerAddr = newOwner2;
                                    }
                                } else {
                                    ownerAddr = newOwner;
                                }
                            }

                            logger.debug("Redirect the request to new owner {}.", ownerAddr);
                            sendWriteRequest(ownerAddr, stream, data, result);
                        } else {
                            // no owner found
                            doWrite(stream, data, result, addr);
                        }
                        break;
                    case SERVICE_UNAVAILABLE:
                        // we are receiving SERVICE_UNAVAILABLE exception from proxy, it means proxy or the stream is closed
                        // redirect the request.
                        if (stream2Addresses.remove(stream, addr)) {
                            ownershipRemoves.incr();
                        }
                        ownershipRedirects.incr();
                        doWrite(stream, data, result, addr);
                        break;
                    default:
                        logger.error("Failed to write request to {} : {}", stream, response);
                        // server side exceptions, throw to the client.
                        // remove ownership to that host
                        if (stream2Addresses.remove(stream, addr)) {
                            ownershipRemoves.incr();
                        }
                        // throw the exception to the client
                        result.setException(DLException.of(response.getHeader()));
                        break;
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    if (cause instanceof RequestTimeoutException) {
                        result.setException(cause);
                    } else {
                        logger.error("Failed to write request to {} @ {} : {}",
                                new Object[] { stream, addr, null != cause.getMessage() ? cause.getMessage() : cause.getClass() });
                        // remove from host list
                        removeSocketAddress(addr, cause);
                        // remove this client
                        removeClient(addr, sc);
                        // remove ownership to that host
                        if (stream2Addresses.remove(stream, addr)) {
                            ownershipRemoves.incr();
                        }
                        // do a retry
                        doWrite(stream, data, result, addr);
                    }
                }
            });
        }
    }
}
