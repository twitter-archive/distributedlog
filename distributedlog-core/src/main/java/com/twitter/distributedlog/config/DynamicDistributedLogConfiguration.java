package com.twitter.distributedlog.config;

import com.twitter.distributedlog.DistributedLogConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whitelist dynamic configuration by adding an accessor to this class.
 */
public class DynamicDistributedLogConfiguration extends ConcurrentBaseConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(DynamicDistributedLogConfiguration.class);

    private final ConcurrentBaseConfiguration defaultConfig;

    public DynamicDistributedLogConfiguration(ConcurrentBaseConfiguration defaultConfig) {
        this.defaultConfig = defaultConfig;
    }

    /**
     * Get retention period in hours
     *
     * @return retention period in hours
     */
    public int getRetentionPeriodHours() {
        return getInt(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS,
                DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT));
    }

    /**
     * A lower threshold bytes per second limit on writes to the distributedlog proxy.
     *
     * @return Bytes per second write limit
     */
    public int getBpsSoftWriteLimit() {
        return getInt(DistributedLogConfiguration.BKDL_BPS_SOFT_WRITE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BPS_SOFT_WRITE_LIMIT,
                DistributedLogConfiguration.BKDL_BPS_SOFT_WRITE_LIMIT_DEFAULT));
    }

    /**
     * An upper threshold bytes per second limit on writes to the distributedlog proxy.
     *
     * @return Bytes per second write limit
     */
    public int getBpsHardWriteLimit() {
        return getInt(DistributedLogConfiguration.BKDL_BPS_HARD_WRITE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BPS_HARD_WRITE_LIMIT,
                DistributedLogConfiguration.BKDL_BPS_HARD_WRITE_LIMIT_DEFAULT));
    }

    /**
     * A lower threshold requests per second limit on writes to the distributedlog proxy.
     *
     * @return Requests per second write limit
     */
    public int getRpsSoftWriteLimit() {
        return getInt(DistributedLogConfiguration.BKDL_RPS_SOFT_WRITE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_RPS_SOFT_WRITE_LIMIT,
                DistributedLogConfiguration.BKDL_RPS_SOFT_WRITE_LIMIT_DEFAULT));
    }

    /**
     * An upper threshold requests per second limit on writes to the distributedlog proxy.
     *
     * @return Requests per second write limit
     */
    public int getRpsHardWriteLimit() {
        return getInt(DistributedLogConfiguration.BKDL_RPS_HARD_WRITE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_RPS_HARD_WRITE_LIMIT,
                DistributedLogConfiguration.BKDL_RPS_HARD_WRITE_LIMIT_DEFAULT));
    }

    /**
     * A lower threshold requests per second limit on writes to the distributedlog proxy globally.
     *
     * @return Requests per second write limit
     */
    public int getRpsSoftServiceLimit() {
        return getInt(DistributedLogConfiguration.BKDL_RPS_SOFT_SERVICE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_RPS_SOFT_SERVICE_LIMIT,
                DistributedLogConfiguration.BKDL_RPS_SOFT_SERVICE_LIMIT_DEFAULT));
    }

    /**
     * An upper threshold requests per second limit on writes to the distributedlog proxy globally.
     *
     * @return Requests per second write limit
     */
    public int getRpsHardServiceLimit() {
        return getInt(DistributedLogConfiguration.BKDL_RPS_HARD_SERVICE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_RPS_HARD_SERVICE_LIMIT,
                DistributedLogConfiguration.BKDL_RPS_HARD_SERVICE_LIMIT_DEFAULT));
    }

    /**
     * When 60min average rps for the entire service instance hits this value, new streams will be
     * rejected.
     *
     * @return Requests per second limit
     */
    public int getRpsStreamAcquireServiceLimit() {
        return getInt(DistributedLogConfiguration.BKDL_RPS_STREAM_ACQUIRE_SERVICE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_RPS_STREAM_ACQUIRE_SERVICE_LIMIT,
                DistributedLogConfiguration.BKDL_RPS_STREAM_ACQUIRE_SERVICE_LIMIT_DEFAULT));
    }

    /**
     * Get percent of write requests which should be delayed by BKDL_EI_INJECTED_WRITE_DELAY_MS.
     *
     * @return percent of writes to delay.
     */
    public double getEIInjectedWriteDelayPercent() {
        return getDouble(DistributedLogConfiguration.BKDL_EI_INJECTED_WRITE_DELAY_PERCENT,
            defaultConfig.getDouble(DistributedLogConfiguration.BKDL_EI_INJECTED_WRITE_DELAY_PERCENT,
                DistributedLogConfiguration.BKDL_EI_INJECTED_WRITE_DELAY_PERCENT_DEFAULT));
    }

    /**
     * Get amount of time to delay writes for in writer failure injection.
     *
     * @return millis to delay writes for.
     */
    public int getEIInjectedWriteDelayMs() {
        return getInt(DistributedLogConfiguration.BKDL_EI_INJECTED_WRITE_DELAY_MS,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_EI_INJECTED_WRITE_DELAY_MS,
                DistributedLogConfiguration.BKDL_EI_INJECTED_WRITE_DELAY_MS_DEFAULT));
    }

    /**
     * Get output buffer size
     *
     * @return buffer size
     */
    public int getOutputBufferSize() {
        return getInt(DistributedLogConfiguration.BKDL_OUTPUT_BUFFER_SIZE,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_OUTPUT_BUFFER_SIZE,
                DistributedLogConfiguration.BKDL_OUTPUT_BUFFER_SIZE_DEFAULT));
    }

    /**
     * Get Periodic Log Flush Frequency in seconds
     *
     * @return periodic flush frequency
     */
    public int getPeriodicFlushFrequencyMilliSeconds() {
        return getInt(DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS,
                DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT));
    }
}
