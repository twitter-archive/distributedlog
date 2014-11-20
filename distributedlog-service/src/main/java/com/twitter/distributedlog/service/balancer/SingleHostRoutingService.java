package com.twitter.distributedlog.service.balancer;

import com.twitter.distributedlog.service.RoutingService;
import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;
import java.util.concurrent.CopyOnWriteArraySet;

public class SingleHostRoutingService implements RoutingService {

    public static SingleHostRoutingService of(SocketAddress address) {
        return new SingleHostRoutingService(address);
    }

    private final SocketAddress address;
    private final CopyOnWriteArraySet<RoutingListener> listeners =
            new CopyOnWriteArraySet<RoutingListener>();

    SingleHostRoutingService(SocketAddress address) {
        this.address = address;
    }

    @Override
    public void startService() {
        // no-op
    }

    @Override
    public void stopService() {
        // no-op
    }

    @Override
    public RoutingService registerListener(RoutingListener listener) {
        listeners.add(listener);
        return this;
    }

    @Override
    public RoutingService unregisterListener(RoutingListener listener) {
        listeners.remove(listener);
        return null;
    }

    @Override
    public SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException {
        if (address.equals(previousAddr)) {
            throw new NoBrokersAvailableException("No hosts is available than " + previousAddr);
        }
        return address;
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        // no-op
    }
}
