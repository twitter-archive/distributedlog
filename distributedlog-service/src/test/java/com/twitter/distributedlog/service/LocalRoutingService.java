package com.twitter.distributedlog.service;

import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalRoutingService implements RoutingService {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements RoutingService.Builder {

        private Builder() {}

        @Override
        public LocalRoutingService build() {
            return new LocalRoutingService();
        }
    }

    private final Map<String, LinkedHashSet<SocketAddress>> localAddresses =
            new HashMap<String, LinkedHashSet<SocketAddress>>();
    private final CopyOnWriteArrayList<RoutingListener> listeners =
            new CopyOnWriteArrayList<RoutingListener>();

    boolean allowRetrySameHost = true;

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

    public LocalRoutingService setAllowRetrySameHost(boolean enabled) {
        allowRetrySameHost = enabled;
        return this;
    }

    public void addHost(String stream, SocketAddress address) {
        boolean notify = false;
        synchronized (this) {
            LinkedHashSet<SocketAddress> addresses = localAddresses.get(stream);
            if (null == addresses) {
                addresses = new LinkedHashSet<SocketAddress>();
                localAddresses.put(stream, addresses);
            }
            if (addresses.add(address)) {
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
    public synchronized SocketAddress getHost(String key, RoutingContext rContext)
            throws NoBrokersAvailableException {
        LinkedHashSet<SocketAddress> addresses = localAddresses.get(key);

        SocketAddress candidate = null;
        if (null != addresses) {
            for (SocketAddress host : addresses) {
                if (rContext.isTriedHost(host) && !allowRetrySameHost) {
                    continue;
                } else {
                    candidate = host;
                    break;
                }
            }
        }
        if (null != candidate) {
            return candidate;
        }
        throw new NoBrokersAvailableException("No host available");
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        // nop
    }
}
