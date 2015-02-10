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
     * Send heartbeat to the stream and its readers
     *
     * @param stream
     *          stream.
     * @return check result.
     */
    Future<Void> heartbeat(String stream);

    /**
     * Get current ownership distribution from current monitor service view.
     *
     * @return current ownership distribution
     */
    Map<SocketAddress, Set<String>> getStreamOwnershipDistribution();

    /**
     * Enable/Disable accepting new stream on a given proxy
     *
     * @param enabled
     *          flag to enable/disable accepting new streams on a given proxy
     * @return void
     */
    Future<Void> setAcceptNewStream(boolean enabled);
}
