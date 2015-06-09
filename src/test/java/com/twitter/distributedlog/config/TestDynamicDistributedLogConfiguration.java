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
        // Only retention period is set in dyncfg
        DistributedLogConfiguration pri = new DistributedLogConfiguration();
        pri.setRetentionPeriodHours(5);

        // Default config defines retention period plus two other params, but eaves ack quorum unspecified
        DistributedLogConfiguration sec = new DistributedLogConfiguration();
        sec.setRetentionPeriodHours(99);
        sec.setEnsembleSize(99);
        sec.setWriteQuorumSize(99);

        ConcurrentConstConfiguration dynPri = new ConcurrentConstConfiguration(pri);
        ConcurrentConstConfiguration dynSec = new ConcurrentConstConfiguration(sec);
        DynamicDistributedLogConfiguration config = new DynamicDistributedLogConfiguration(dynPri, dynSec);

        // Config checks primary then secondary then const defaults
        assertEquals(5, config.getRetentionPeriodHours());
        assertEquals(99, config.getEnsembleSize());
        assertEquals(99, config.getWriteQuorumSize());
        assertEquals(DistributedLogConfiguration.BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT,
                     config.getAckQuorumSize());
    }
}
