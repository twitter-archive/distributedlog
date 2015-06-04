package com.twitter.distributedlog.service;

import java.net.SocketAddress;

/**
 * Resolve address to region.
 */
public interface RegionResolver {

    /**
     * Resolve address to region.
     *
     * @param address
     *          socket address
     * @return region
     */
    String resolveRegion(SocketAddress address);

    /**
     * Remove cached host.
     *
     * @param address
     *          socket address.
     */
    void removeCachedHost(SocketAddress address);
}
