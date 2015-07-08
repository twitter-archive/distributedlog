package com.twitter.distributedlog.config;

import com.twitter.distributedlog.DistributedLogConfiguration;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestDynamicDistributedLogConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(TestDynamicDistributedLogConfiguration.class);

    @Test
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
}
