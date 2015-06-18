package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For all streams return an absent configuration.
 */
public class NullStreamConfigProvider implements StreamConfigProvider {
    static final Logger LOG = LoggerFactory.getLogger(NullStreamConfigProvider.class);

    private static final Optional<DynamicDistributedLogConfiguration> nullConf =
            Optional.<DynamicDistributedLogConfiguration>absent();

    @Override
    public Optional<DynamicDistributedLogConfiguration> getDynamicStreamConfig(String streamName) {
        return nullConf;
    }
}
