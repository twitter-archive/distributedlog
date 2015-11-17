package com.twitter.distributedlog.v2;

import com.google.common.base.Optional;

import org.apache.commons.configuration.StrictConfigurationComparator;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDistributedLogConfiguration {

    @Test
    public void loadStreamConfGoodOverrideAccepted() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        assertEquals(conf.getPeriodicFlushFrequencyMilliSeconds(),
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT);
        assertEquals(conf.getReaderIdleErrorThresholdMillis(),
            DistributedLogConfiguration.BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT);
        DistributedLogConfiguration override = new DistributedLogConfiguration();
        override.setPeriodicFlushFrequencyMilliSeconds(
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT+1);
        override.setReaderIdleErrorThresholdMillis(
            DistributedLogConfiguration.BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT - 1);
        conf.loadStreamConf(Optional.of(override));
        assertEquals(conf.getPeriodicFlushFrequencyMilliSeconds(),
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT+1);
        assertEquals(conf.getReaderIdleErrorThresholdMillis(),
            DistributedLogConfiguration.BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT - 1);
    }

    @Test
    public void loadStreamConfBadOverrideIgnored() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        assertEquals(conf.getBKClientWriteTimeout(),
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);
        DistributedLogConfiguration override = new DistributedLogConfiguration();
        override.setBKClientWriteTimeout(
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT+1);
        conf.loadStreamConf(Optional.of(override));
        assertEquals(conf.getBKClientWriteTimeout(),
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);
    }

    @Test
    public void loadStreamConfNullOverrides() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogConfiguration confClone = (DistributedLogConfiguration)conf.clone();
        Optional<DistributedLogConfiguration> streamConfiguration = Optional.absent();
        conf.loadStreamConf(streamConfiguration);

        StrictConfigurationComparator comp = new StrictConfigurationComparator();
        assertTrue(comp.compare(conf, confClone));
    }
}

