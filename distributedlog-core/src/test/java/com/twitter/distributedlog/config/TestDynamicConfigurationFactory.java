package com.twitter.distributedlog.config;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import com.twitter.distributedlog.DistributedLogConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
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
        ConcurrentBaseConfiguration defaultConf = new ConcurrentConstConfiguration(new DistributedLogConfiguration());
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

    /**
     * If the file is missing, get-config should not fail, and the file should be picked up if its added.
     * If the file is removed externally same should apply.
     */
    @Test(timeout = 60000)
    public void testMissingConfig() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        DynamicConfigurationFactory factory = getConfigFactory(writer.getFile());
        Optional<DynamicDistributedLogConfiguration> conf = factory.getDynamicConfiguration(writer.getFile().getPath());
        writer.setProperty(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS, "1");
        writer.save();
        waitForConfig(conf.get(), 1);
        File configFile = writer.getFile();
        configFile.delete();
        Thread.sleep(1000);
        PropertiesWriter writer2 = new PropertiesWriter(writer.getFile());
        writer2.setProperty(DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS, "2");
        writer2.save();
        waitForConfig(conf.get(), 2);
    }
}
