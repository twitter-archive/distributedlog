package com.twitter.distributedlog.service;

import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalRoutingService implements RoutingService {
    private final Map<String, SocketAddress> localAddresses =
            new HashMap<String, SocketAddress>();
    private final CopyOnWriteArrayList<RoutingListener> listeners =
            new CopyOnWriteArrayList<RoutingListener>();

    @Override
    public void startService() {
        // nop
    }

    @Override
    public void stopService() {
        // nop
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

    void addHost(String stream, SocketAddress address) {
        boolean notify = false;
        synchronized (this) {
            if (!localAddresses.containsKey(stream)) {
                localAddresses.put(stream, address);
                notify = true;
            }
        }
        if (notify) {
            for (RoutingListener listener : listeners) {
                listener.onServerJoin(address);
            }
        }
    }

    @Override
    public synchronized SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException {
        SocketAddress address = localAddresses.get(key);
        if (null != address) {
            return address;
        }
        throw new NoBrokersAvailableException("No host available");
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        // nop
    }
}