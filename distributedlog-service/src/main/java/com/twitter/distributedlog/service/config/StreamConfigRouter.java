package com.twitter.distributedlog.service.config;

/**
 * Map stream name to a configuration id. The config id is used by
 * StreamConfigProvider to construct a path to a config. StreamConfigProvider
 * will construct a config file path using the config id returned by
 * getConfig by prepending a config base path and appending a file extension.
 */
interface StreamConfigRouter {
    /**
     * Get config id to use for the stream identified by streamName.
     *
     * @param streamName denotes stream to map to a config
     * @return config id
     */
    String getConfig(String streamName);
}
