package com.twitter.distributedlog.service;

import com.twitter.util.Future;

public interface MonitorServiceClient {

    /**
     * Check a given stream.
     *
     * @param stream
     *          stream.
     * @return check result.
     */
    Future<Void> check(String stream);
}
