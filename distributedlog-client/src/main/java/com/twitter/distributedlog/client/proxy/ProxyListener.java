package com.twitter.distributedlog.client.proxy;

import com.twitter.distributedlog.thrift.service.ServerInfo;

import java.net.SocketAddress;

/**
 * Listener on server changes
 */
public interface ProxyListener {
    /**
     * When a proxy's server info changed, it would be notified.
     *
     * @param address
     *          proxy address
     * @param serverInfo
     *          proxy's server info
     */
    void onServerInfoUpdated(SocketAddress address, ServerInfo serverInfo);
}
