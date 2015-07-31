package com.twitter.distributedlog.client.proxy;

import java.net.SocketAddress;
import java.util.Set;

/**
 * Provider to provider list of hosts for handshaking.
 */
public interface HostProvider {

    /**
     * Get the list of hosts for handshaking.
     *
     * @return list of hosts for handshaking.
     */
    Set<SocketAddress> getHosts();

}
