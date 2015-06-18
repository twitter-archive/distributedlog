package com.twitter.distributedlog.service.config;

/**
 * Map stream name to configuration of the same name.
 */
class IdentityConfigRouter implements StreamConfigRouter {
    public String getConfig(String streamName) {
        return streamName;
    }
}
