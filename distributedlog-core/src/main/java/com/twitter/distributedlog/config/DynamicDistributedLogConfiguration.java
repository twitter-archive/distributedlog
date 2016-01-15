package com.twitter.distributedlog.config;

import com.twitter.distributedlog.DistributedLogConfiguration;

import static com.twitter.distributedlog.DistributedLogConfiguration.*;

/**
 * Whitelist dynamic configuration by adding an accessor to this class.
 */
public class DynamicDistributedLogConfiguration extends ConcurrentBaseConfiguration {

    private final ConcurrentBaseConfiguration defaultConfig;

    public DynamicDistributedLogConfiguration(ConcurrentBaseConfiguration defaultConfig) {
        this.defaultConfig = defaultConfig;
    }

    private static int getInt(ConcurrentBaseConfiguration configuration,
                              String newKey,
                              String oldKey,
                              int defaultValue) {
        return configuration.getInt(newKey, configuration.getInt(oldKey, defaultValue));
    }

    /**
     * Get retention period in hours
     *
     * @return retention period in hours
     */
    public int getRetentionPeriodHours() {
        return getInt(
                this,
                BKDL_RETENTION_PERIOD_IN_HOURS,
                BKDL_RETENTION_PERIOD_IN_HOURS_OLD,
                getInt(defaultConfig,
                        BKDL_RETENTION_PERIOD_IN_HOURS,
                        BKDL_RETENTION_PERIOD_IN_HOURS_OLD,
                        BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT)
        );
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
     * A lower threshold bytes per second limit on writes to the distributedlog proxy globally.
     *
     * @return Bytes per second write limit
     */
    public int getBpsSoftServiceLimit() {
        return getInt(DistributedLogConfiguration.BKDL_BPS_SOFT_SERVICE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BPS_SOFT_SERVICE_LIMIT,
                DistributedLogConfiguration.BKDL_BPS_SOFT_SERVICE_LIMIT_DEFAULT));
    }

    /**
     * An upper threshold bytes per second limit on writes to the distributedlog proxy globally.
     *
     * @return Bytes per second write limit
     */
    public int getBpsHardServiceLimit() {
        return getInt(DistributedLogConfiguration.BKDL_BPS_HARD_SERVICE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BPS_HARD_SERVICE_LIMIT,
                DistributedLogConfiguration.BKDL_BPS_HARD_SERVICE_LIMIT_DEFAULT));
    }

    /**
     * When 60min average bps for the entire service instance hits this value, new streams will be
     * rejected.
     *
     * @return Bytes per second limit
     */
    public int getBpsStreamAcquireServiceLimit() {
        return getInt(DistributedLogConfiguration.BKDL_BPS_STREAM_ACQUIRE_SERVICE_LIMIT,
            defaultConfig.getInt(DistributedLogConfiguration.BKDL_BPS_STREAM_ACQUIRE_SERVICE_LIMIT,
                DistributedLogConfiguration.BKDL_BPS_STREAM_ACQUIRE_SERVICE_LIMIT_DEFAULT));
    }

    /**
     * Get percent of write bytes which should be delayed by BKDL_EI_INJECTED_WRITE_DELAY_MS.
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
        return getInt(
                this,
                BKDL_OUTPUT_BUFFER_SIZE,
                BKDL_OUTPUT_BUFFER_SIZE_OLD,
                getInt(defaultConfig,
                        BKDL_OUTPUT_BUFFER_SIZE,
                        BKDL_OUTPUT_BUFFER_SIZE_OLD,
                        BKDL_OUTPUT_BUFFER_SIZE_DEFAULT)
        );
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

    /**
     * Get the number of entries that readahead worker reads as a batch from bookkeeper
     *
     * @return the batch size
     */
    public int getReadAheadBatchSize() {
        return getInt(
                this,
                BKDL_READAHEAD_BATCHSIZE,
                BKDL_READAHEAD_BATCHSIZE_OLD,
                getInt(defaultConfig,
                        BKDL_READAHEAD_BATCHSIZE,
                        BKDL_READAHEAD_BATCHSIZE_OLD,
                        BKDL_READAHEAD_BATCHSIZE_DEFAULT)
        );
    }

    /**
     * Get the maximum number of {@link com.twitter.distributedlog.LogRecord } that readahead worker will cache.
     *
     * @return the maximum number
     */
    public int getReadAheadMaxRecords() {
        return getInt(
                this,
                BKDL_READAHEAD_MAX_RECORDS,
                BKDL_READAHEAD_MAX_RECORDS_OLD,
                getInt(defaultConfig,
                        BKDL_READAHEAD_MAX_RECORDS,
                        BKDL_READAHEAD_MAX_RECORDS_OLD,
                        BKDL_READAHEAD_MAX_RECORDS_DEFAULT)
        );
    }
}
