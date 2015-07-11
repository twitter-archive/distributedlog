package com.twitter.distributedlog.config;

import com.google.common.base.Preconditions;

import org.apache.commons.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invariant thread-safe view of some configuration.
 */
public class ConcurrentConstConfiguration extends ConcurrentBaseConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(ConcurrentConstConfiguration.class);

    public ConcurrentConstConfiguration(Configuration conf) {
        Preconditions.checkNotNull(conf);
        copy(conf);
    }
}
