package com.twitter.distributedlog.service;

import com.google.common.base.Preconditions;
import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;
import java.util.concurrent.CopyOnWriteArraySet;

class SingleHostRoutingService implements RoutingService {

    @Deprecated
    public static SingleHostRoutingService of(SocketAddress address) {
        return new SingleHostRoutingService(address);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements RoutingService.Builder {

        private SocketAddress _address;

        private Builder() {}

        public Builder address(SocketAddress address) {
            this._address = address;
            return this;
        }

        @Override
        public RoutingService build() {
            Preconditions.checkNotNull(_address, "Host is null");
            return new SingleHostRoutingService(_address);
        }
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
        for (RoutingListener listener : listeners) {
            listener.onServerJoin(address);
        }
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
