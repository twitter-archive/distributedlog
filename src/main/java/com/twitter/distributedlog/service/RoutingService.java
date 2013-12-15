package com.twitter.distributedlog.service;

import com.twitter.finagle.NoBrokersAvailableException;

import java.net.SocketAddress;

/**
 * Routing Service provides mechanism how to route requests.
 */
interface RoutingService {

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
