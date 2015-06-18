package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;

/**
 * Expose per-stream configs to dl proxy.
 */
public interface StreamConfigProvider {
    /**
     * Get dynamic per stream config overrides for a given stream.
     *
     * @param streamName stream name to return config for
     * @return Optional dynamic configuration instance
     */
    Optional<DynamicDistributedLogConfiguration> getDynamicStreamConfig(String streamName);
}
