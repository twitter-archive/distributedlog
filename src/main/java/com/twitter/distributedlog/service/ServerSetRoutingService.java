package com.twitter.distributedlog.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Routing Service based on a given {@link com.twitter.common.zookeeper.ServerSet}.
 */
class ServerSetRoutingService extends Thread implements RoutingService {

    static final Logger logger = LoggerFactory.getLogger(ServerSetRoutingService.class);

    private static class HostComparator implements Comparator<SocketAddress> {

        static final HostComparator instance = new HostComparator();

        @Override
        public int compare(SocketAddress o1, SocketAddress o2) {
            return o1.toString().compareTo(o2.toString());
        }
    }

    private final ServerSet serverSet;

    private final Set<SocketAddress> hostSet = new HashSet<SocketAddress>();
    private List<SocketAddress> hostList = new ArrayList<SocketAddress>();
    private final HashFunction hasher = Hashing.md5();

    // Server Set Changes
    private final AtomicReference<ImmutableSet<ServiceInstance>> serverSetChange =
            new AtomicReference<ImmutableSet<ServiceInstance>>(null);
    private final CountDownLatch changeLatch = new CountDownLatch(1);

    // Listeners
    protected final CopyOnWriteArraySet<RoutingListener> listeners =
            new CopyOnWriteArraySet<RoutingListener>();

    ServerSetRoutingService(ServerSet serverSet) {
        super("ServerSetRoutingService");
        this.serverSet = serverSet;
    }

    @Override
    public void startService() {
        start();
        try {
            if (!changeLatch.await(1, TimeUnit.MINUTES)) {
                logger.warn("No serverset change received in 1 minute.");
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted waiting first serverset change : ", e);
        }
    }

    @Override
    public void stopService() {
        Thread.currentThread().interrupt();
        try {
            join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted on waiting serverset routing service to finish : ", e);
        }
    }

    @Override
    public RoutingService registerListener(RoutingListener listener) {
        listeners.add(listener);
        return this;
    }

    @Override
    public RoutingService unregisterListener(RoutingListener listener) {
        listeners.remove(listener);
        return this;
    }

    @Override
    public SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException {
        SocketAddress address = null;
        synchronized (hostSet) {
            if (0 != hostList.size()) {
                int hashCode = hasher.hashUnencodedChars(key).asInt();
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
                            address = null;
                            break;
                        }
                        address = newList.get(i);
                    }
                }
            }
        }
        if (null == address) {
            throw new NoBrokersAvailableException("No host is available.");
        }
        return address;
    }

    @Override
    public void removeHost(SocketAddress host, Throwable reason) {
        synchronized (hostSet) {
            if (hostSet.remove(host)) {
                logger.info("Node {} left due to : ", host, reason);
            }
            hostList = new ArrayList<SocketAddress>(hostSet);
            Collections.sort(hostList, HostComparator.instance);
            logger.info("Host list becomes : {}.", hostList);
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

    protected synchronized void performServerSetChange(ImmutableSet<ServiceInstance> serverSet) {
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
            for (RoutingListener listener : listeners) {
                listener.onServerLeft(addr);
            }
        }

        for (SocketAddress addr : added) {
            for (RoutingListener listener : listeners) {
                listener.onServerJoin(addr);
            }
        }

        synchronized (hostSet) {
            hostList = new ArrayList<SocketAddress>(hostSet);
            Collections.sort(hostList, HostComparator.instance);
            logger.info("Host list becomes : {}.", hostList);
        }

    }
}
