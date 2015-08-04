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
     * @param client
     *          proxy client that executes handshaking
     * @param serverInfo
     *          proxy's server info
     */
    void onHandshakeSuccess(SocketAddress address, ProxyClient client, ServerInfo serverInfo);

    /**
     * Failed to handshake with a proxy.
     *
     * @param address
     *          proxy address
     * @param client
     *          proxy client
     * @param cause
     *          failure reason
     */
    void onHandshakeFailure(SocketAddress address, ProxyClient client, Throwable cause);
}
