package com.twitter.distributedlog.service;

import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;

/**
 * Routing Service provides mechanism how to route requests.
 */
public interface RoutingService {

    public static interface Builder {

        /**
         * @return built routing service
         */
        RoutingService build();

    }

    public static interface RoutingListener {
        void onServerLeft(SocketAddress address);
        void onServerJoin(SocketAddress address);
    }

    void startService();

    void stopService();

    RoutingService registerListener(RoutingListener listener);

    RoutingService unregisterListener(RoutingListener listener);

    SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException;

    void removeHost(SocketAddress address, Throwable reason);
}
