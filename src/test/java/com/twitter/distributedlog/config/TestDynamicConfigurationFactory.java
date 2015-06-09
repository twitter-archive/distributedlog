package com.twitter.distributedlog.config;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import com.twitter.distributedlog.DistributedLogConfiguration;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestDynamicConfigurationFactory {
    static final Logger LOG = LoggerFactory.getLogger(TestDynamicConfigurationFactory.class);

    private void waitForConfig(DynamicDistributedLogConfiguration conf, int value) throws Exception {
        while (!Objects.equal(conf.getRetentionPeriodHours(), value)) {
            Thread.sleep(100);
        }
    }

    private DynamicConfigurationFactory getConfigFactory(File configFile) {
        String streamConfigPath = configFile.getParent();
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        return new DynamicConfigurationFactory(executorService, 100, TimeUnit.MILLISECONDS);
    }

    private String getNamePart(File configFile) {
        String propsFilename = configFile.getName();
        return propsFilename.substring(0, propsFilename.indexOf(".conf"));
    }

    @Test(timeout = 60000)
    public void testGetDynamicConfigBasics() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        DynamicConfigurationFactory factory = getConfigFactory(writer.getFile());
        Optional<DynamicDistributedLogConfiguration> conf = factory.getDynamicConfiguration(writer.getFile().getPath());
        assertEquals(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT, conf.get().getRetentionPeriodHours());
        writer.setProperty(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS, "1");
        writer.save();
        waitForConfig(conf.get(), 1);
        assertEquals(1, conf.get().getRetentionPeriodHours());
    }

    @Test(timeout = 60000)
    public void testGetDynamicConfigIsSingleton() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        DynamicConfigurationFactory factory = getConfigFactory(writer.getFile());
        String configPath = writer.getFile().getPath();
        Optional<DynamicDistributedLogConfiguration> conf1 = factory.getDynamicConfiguration(configPath);
        Optional<DynamicDistributedLogConfiguration> conf2 = factory.getDynamicConfiguration(configPath);
        assertEquals(conf1, conf2);
    }

    @Test(timeout = 60000)
    public void testMissingConfig() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        DynamicConfigurationFactory factory = getConfigFactory(writer.getFile());
        Optional<DynamicDistributedLogConfiguration> conf = factory.getDynamicConfiguration("missing_config.blah");
        assertFalse(conf.isPresent());
    }
}
