package com.twitter.distributedlog.service;

import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
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
    public synchronized SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException {
        return getHost(key, previousAddr, StatusCode.FOUND);
    }

    public synchronized SocketAddress getHost(String key, SocketAddress previousAddr, StatusCode previousCode)
            throws NoBrokersAvailableException {
        LinkedHashSet<SocketAddress> addresses = localAddresses.get(key);

        SocketAddress candidate = null;
        if (null != addresses) {
            if (addresses.size() == 1) {
                if (!allowRetrySameHost && previousAddr != null && addresses.contains(previousAddr)) {
                    throw new NoBrokersAvailableException("No host available");
                }
                candidate = addresses.iterator().next();
            } else if (addresses.size() > 1) {
                Iterator<SocketAddress> iter = addresses.iterator();
                if (null == previousAddr || !addresses.contains(previousAddr)) {
                    candidate = iter.next();
                } else {
                    SocketAddress nextAddr;
                    while (iter.hasNext()) {
                        nextAddr = iter.next();
                        if (nextAddr.equals(previousAddr)) {
                            break;
                        }
                    }
                    if (iter.hasNext()) {
                        candidate = iter.next();
                    }
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
