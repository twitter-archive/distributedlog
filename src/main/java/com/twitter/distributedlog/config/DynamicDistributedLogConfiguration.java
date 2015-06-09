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
    private final ConcurrentBaseConfiguration defaultConfig;

    public DynamicDistributedLogConfiguration(ConcurrentBaseConfiguration config,
                                              ConcurrentBaseConfiguration defaultConfig) {
        this.config = config;
        this.defaultConfig = defaultConfig;
    }

    /**
     * Get retention period in hours
     *
     * @return retention period in hours
     */
    public int getRetentionPeriodHours() {
        return config.getInt(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS,
                DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT));
    }

    /**
     * Get ensemble size
     *
     * @return ensemble size
     */
    public int getEnsembleSize() {
        return config.getInt(DistributedLogConfiguration.BKDL_BOOKKEEPER_ENSEMBLE_SIZE,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BOOKKEEPER_ENSEMBLE_SIZE,
                DistributedLogConfiguration.BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT));
    }

    /**
     * Get write quorum size.
     *
     * @return write quorum size
     */
    public int getWriteQuorumSize() {
        return config.getInt(DistributedLogConfiguration.BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE,
                DistributedLogConfiguration.BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT));
    }

    /**
     * Get ack quorum size.
     *
     * @return ack quorum size
     */
    public int getAckQuorumSize() {
        return config.getInt(DistributedLogConfiguration.BKDL_BOOKKEEPER_ACK_QUORUM_SIZE,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BOOKKEEPER_ACK_QUORUM_SIZE,
                DistributedLogConfiguration.BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT));
    }
}
