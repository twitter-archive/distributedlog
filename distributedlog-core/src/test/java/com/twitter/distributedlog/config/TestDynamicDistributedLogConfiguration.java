package com.twitter.distributedlog.config;

import com.twitter.distributedlog.DistributedLogConfiguration;

import com.twitter.distributedlog.bk.QuorumConfig;
import org.junit.Test;

import static com.twitter.distributedlog.DistributedLogConfiguration.*;
import static org.junit.Assert.*;

public class TestDynamicDistributedLogConfiguration {

    @Test(timeout = 20000)
    public void testDefaults() throws Exception {
        // Default config defines retention period plus two other params, but eaves ack quorum unspecified
        DistributedLogConfiguration underlyingConfig = new DistributedLogConfiguration();
        underlyingConfig.setRetentionPeriodHours(99);
        underlyingConfig.setProperty("rpsHardWriteLimit", 99);

        ConcurrentConstConfiguration defaultConfig = new ConcurrentConstConfiguration(underlyingConfig);
        DynamicDistributedLogConfiguration config = new DynamicDistributedLogConfiguration(defaultConfig);
        assertEquals(99, config.getRetentionPeriodHours());
        assertEquals(99, config.getRpsHardWriteLimit());
        config.setProperty(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS, 5);

        // Config checks primary then secondary then const defaults
        assertEquals(5, config.getRetentionPeriodHours());
        assertEquals(99, config.getRpsHardWriteLimit());
    }

    @Test(timeout = 20000)
    public void testGetRetentionPeriodHours() {
        ConcurrentBaseConfiguration defaultConfig = new ConcurrentBaseConfiguration();
        DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(defaultConfig);
        // get default value
        assertEquals(BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT, dynConf.getRetentionPeriodHours());
        // get value from old key of default config
        defaultConfig.setProperty(BKDL_RETENTION_PERIOD_IN_HOURS_OLD, BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT  + 1);
        assertEquals(BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT  + 1, dynConf.getRetentionPeriodHours());
        // get value from new key of default config
        defaultConfig.setProperty(BKDL_RETENTION_PERIOD_IN_HOURS, BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT + 2);
        assertEquals(BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT  + 2, dynConf.getRetentionPeriodHours());
        // get value from old key of dynamic config
        dynConf.setProperty(BKDL_RETENTION_PERIOD_IN_HOURS_OLD, BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT  + 3);
        assertEquals(BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT  + 3, dynConf.getRetentionPeriodHours());
        // get value from new key of default config
        dynConf.setProperty(BKDL_RETENTION_PERIOD_IN_HOURS, BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT  + 4);
        assertEquals(BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT  + 4, dynConf.getRetentionPeriodHours());
    }

    @Test(timeout = 20000)
    public void testGetOutputBufferSize() {
        ConcurrentBaseConfiguration defaultConfig = new ConcurrentBaseConfiguration();
        DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(defaultConfig);
        // get default value
        assertEquals(BKDL_OUTPUT_BUFFER_SIZE_DEFAULT, dynConf.getOutputBufferSize());
        // get value from old key of default config
        defaultConfig.setProperty(BKDL_OUTPUT_BUFFER_SIZE_OLD, BKDL_OUTPUT_BUFFER_SIZE_DEFAULT + 1);
        assertEquals(BKDL_OUTPUT_BUFFER_SIZE_DEFAULT  + 1, dynConf.getOutputBufferSize());
        // get value from new key of default config
        defaultConfig.setProperty(BKDL_OUTPUT_BUFFER_SIZE, BKDL_OUTPUT_BUFFER_SIZE_DEFAULT + 2);
        assertEquals(BKDL_OUTPUT_BUFFER_SIZE_DEFAULT  + 2, dynConf.getOutputBufferSize());
        // get value from old key of dynamic config
        dynConf.setProperty(BKDL_OUTPUT_BUFFER_SIZE_OLD, BKDL_OUTPUT_BUFFER_SIZE_DEFAULT  + 3);
        assertEquals(BKDL_OUTPUT_BUFFER_SIZE_DEFAULT  + 3, dynConf.getOutputBufferSize());
        // get value from new key of default config
        dynConf.setProperty(BKDL_OUTPUT_BUFFER_SIZE, BKDL_OUTPUT_BUFFER_SIZE_DEFAULT  + 4);
        assertEquals(BKDL_OUTPUT_BUFFER_SIZE_DEFAULT  + 4, dynConf.getOutputBufferSize());
    }

    @Test(timeout = 20000)
    public void testGetReadAheadBatchSize() {
        ConcurrentBaseConfiguration defaultConfig = new ConcurrentBaseConfiguration();
        DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(defaultConfig);
        // get default value
        assertEquals(BKDL_READAHEAD_BATCHSIZE_DEFAULT, dynConf.getReadAheadBatchSize());
        // get value from old key of default config
        defaultConfig.setProperty(BKDL_READAHEAD_BATCHSIZE_OLD, BKDL_READAHEAD_BATCHSIZE_DEFAULT + 1);
        assertEquals(BKDL_READAHEAD_BATCHSIZE_DEFAULT  + 1, dynConf.getReadAheadBatchSize());
        // get value from new key of default config
        defaultConfig.setProperty(BKDL_READAHEAD_BATCHSIZE, BKDL_READAHEAD_BATCHSIZE_DEFAULT + 2);
        assertEquals(BKDL_READAHEAD_BATCHSIZE_DEFAULT  + 2, dynConf.getReadAheadBatchSize());
        // get value from old key of dynamic config
        dynConf.setProperty(BKDL_READAHEAD_BATCHSIZE_OLD, BKDL_READAHEAD_BATCHSIZE_DEFAULT  + 3);
        assertEquals(BKDL_READAHEAD_BATCHSIZE_DEFAULT  + 3, dynConf.getReadAheadBatchSize());
        // get value from new key of default config
        dynConf.setProperty(BKDL_READAHEAD_BATCHSIZE, BKDL_READAHEAD_BATCHSIZE_DEFAULT  + 4);
        assertEquals(BKDL_READAHEAD_BATCHSIZE_DEFAULT  + 4, dynConf.getReadAheadBatchSize());
    }

    @Test(timeout = 20000)
    public void testGetReadAheadMaxRecords() {
        ConcurrentBaseConfiguration defaultConfig = new ConcurrentBaseConfiguration();
        DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(defaultConfig);
        // get default value
        assertEquals(BKDL_READAHEAD_MAX_RECORDS_DEFAULT, dynConf.getReadAheadMaxRecords());
        // get value from old key of default config
        defaultConfig.setProperty(BKDL_READAHEAD_MAX_RECORDS_OLD, BKDL_READAHEAD_MAX_RECORDS_DEFAULT + 1);
        assertEquals(BKDL_READAHEAD_MAX_RECORDS_DEFAULT  + 1, dynConf.getReadAheadMaxRecords());
        // get value from new key of default config
        defaultConfig.setProperty(BKDL_READAHEAD_MAX_RECORDS, BKDL_READAHEAD_MAX_RECORDS_DEFAULT + 2);
        assertEquals(BKDL_READAHEAD_MAX_RECORDS_DEFAULT  + 2, dynConf.getReadAheadMaxRecords());
        // get value from old key of dynamic config
        dynConf.setProperty(BKDL_READAHEAD_MAX_RECORDS_OLD, BKDL_READAHEAD_MAX_RECORDS_DEFAULT  + 3);
        assertEquals(BKDL_READAHEAD_MAX_RECORDS_DEFAULT  + 3, dynConf.getReadAheadMaxRecords());
        // get value from new key of default config
        dynConf.setProperty(BKDL_READAHEAD_MAX_RECORDS, BKDL_READAHEAD_MAX_RECORDS_DEFAULT  + 4);
        assertEquals(BKDL_READAHEAD_MAX_RECORDS_DEFAULT  + 4, dynConf.getReadAheadMaxRecords());
    }

    void assertQuorumConfig(QuorumConfig config,
                            int expectedEnsembleSize,
                            int expectedWriteQuorumSize,
                            int expectedAckQuorumSize) {
        assertEquals(expectedEnsembleSize, config.getEnsembleSize());
        assertEquals(expectedWriteQuorumSize, config.getWriteQuorumSize());
        assertEquals(expectedAckQuorumSize, config.getAckQuorumSize());
    }

    @Test(timeout = 20000)
    public void testGetQuorumConfig() {
        ConcurrentBaseConfiguration defaultConfig = new ConcurrentBaseConfiguration();
        DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(defaultConfig);
        // get default value
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);

        // Test Ensemble Size

        // get value from old key of default config
        defaultConfig.setProperty(BKDL_BOOKKEEPER_ENSEMBLE_SIZE_OLD, BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 1);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 1,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);
        // get value from new key of default config
        defaultConfig.setProperty(BKDL_BOOKKEEPER_ENSEMBLE_SIZE, BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 2);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 2,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);
        // get value from old key of dynamic config
        dynConf.setProperty(BKDL_BOOKKEEPER_ENSEMBLE_SIZE_OLD, BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 3);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 3,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);
        // get value from new key of dynamic config
        dynConf.setProperty(BKDL_BOOKKEEPER_ENSEMBLE_SIZE, BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);

        // Test Write Quorum Size

        // get value from old key of default config
        defaultConfig.setProperty(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_OLD,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 1);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 1,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);
        // get value from new key of default config
        defaultConfig.setProperty(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 2);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 2,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);
        // get value from old key of dynamic config
        dynConf.setProperty(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_OLD,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 3);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 3,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);
        // get value from new key of dynamic config
        dynConf.setProperty(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 4);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);

        // Test Ack Quorum Size

        // get value from old key of default config
        defaultConfig.setProperty(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_OLD,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 1);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 1);
        // get value from new key of default config
        defaultConfig.setProperty(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 2);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 2);
        // get value from old key of dynamic config
        dynConf.setProperty(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_OLD,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 3);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 3);
        // get value from new key of dynamic config
        dynConf.setProperty(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 4);
        assertQuorumConfig(
                dynConf.getQuorumConfig(),
                BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT + 4,
                BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT + 4);
    }


    @Test(timeout = 20000)
    public void testIsDurableWriteEnabled() {
        ConcurrentBaseConfiguration defaultConfig = new ConcurrentBaseConfiguration();
        DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(defaultConfig);

        assertTrue(dynConf.isDurableWriteEnabled());
        defaultConfig.setProperty(BKDL_IS_DURABLE_WRITE_ENABLED, false);
        assertFalse(dynConf.isDurableWriteEnabled());
        dynConf.setProperty(BKDL_IS_DURABLE_WRITE_ENABLED, true);
        assertTrue(dynConf.isDurableWriteEnabled());
    }
}
