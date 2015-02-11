package com.twitter.distributedlog.service;

import java.net.SocketAddress;

/**
 * Resolve address to region.
 */
public interface RegionResolver {
    String resolveRegion(SocketAddress address);
    void removeCachedHost(SocketAddress address);
}
