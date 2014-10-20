package com.twitter.distributedlog.service;

import com.twitter.util.Future;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

public interface MonitorServiceClient {

    /**
     * Check a given stream.
     *
     * @param stream
     *          stream.
     * @return check result.
     */
    Future<Void> check(String stream);

    /**
     * Get current ownership distribution from current monitor service view.
     *
     * @return current ownership distribution
     */
    Map<SocketAddress, Set<String>> getStreamOwnershipDistribution();
}
