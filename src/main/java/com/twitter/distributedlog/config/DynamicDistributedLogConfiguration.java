package com.twitter.distributedlog.config;

import com.google.common.base.Preconditions;

import com.twitter.distributedlog.DistributedLogConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whitelist dynamic configuration by adding an accessor to this class.
 */
public class DynamicDistributedLogConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(DynamicDistributedLogConfiguration.class);

    private final ConcurrentBaseConfiguration config;

    public DynamicDistributedLogConfiguration(ConcurrentBaseConfiguration config) {
        this.config = config;
    }

    /**
     * Get retention period in hours
     *
     * @return retention period in hours
     */
    public int getRetentionPeriodHours() {
        return config.getInt(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS,
            DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT);
    }
}
