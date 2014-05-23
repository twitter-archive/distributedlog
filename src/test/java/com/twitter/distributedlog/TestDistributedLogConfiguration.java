package com.twitter.distributedlog;

import org.apache.commons.configuration.StrictConfigurationComparator;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDistributedLogConfiguration {

    @Test
    public void loadStreamConfGoodOverrideAccepted() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        assertEquals(conf.getPeriodicFlushFrequencyMilliSeconds(), 
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT);
        DistributedLogConfiguration override = new DistributedLogConfiguration();
        override.setPeriodicFlushFrequencyMilliSeconds(
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT+1);
        conf.loadStreamConf(override);
        assertEquals(conf.getPeriodicFlushFrequencyMilliSeconds(), 
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT+1);
    }

    @Test
    public void loadStreamConfBadOverrideIgnored() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        assertEquals(conf.getBKClientWriteTimeout(), 
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);
        DistributedLogConfiguration override = new DistributedLogConfiguration();
        override.setBKClientWriteTimeout(
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT+1);
        conf.loadStreamConf(override);
        assertEquals(conf.getBKClientWriteTimeout(), 
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);   
    }

    @Test
    public void loadStreamConfNullOverrides() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogConfiguration confClone = (DistributedLogConfiguration)conf.clone();
        conf.loadStreamConf(null);

        StrictConfigurationComparator comp = new StrictConfigurationComparator();
        assertTrue(comp.compare(conf, confClone));
    }
}
