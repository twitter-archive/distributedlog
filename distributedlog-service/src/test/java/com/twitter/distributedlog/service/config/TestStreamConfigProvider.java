package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.config.PropertiesWriter;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import java.io.File;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestStreamConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TestStreamConfigProvider.class);
    private static final String DEFAULT_CONFIG_PATH = "conf";
    private static final DistributedLogConfiguration DEFAULT_CONFIG = new DistributedLogConfiguration();
    private final ScheduledExecutorService configExecutorService;

    public TestStreamConfigProvider() {
        this.configExecutorService = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Dyncfg-%d").build());
    }

    StreamConfigProvider getServiceProvider(String routerName) throws Exception {
        return getServiceProvider(routerName, DEFAULT_CONFIG_PATH);
    }

    StreamConfigProvider getServiceProvider(String routerName, String configPath) throws Exception {
        return new ServiceStreamConfigProvider(configPath, routerName, DEFAULT_CONFIG,
                                               configExecutorService, 1, TimeUnit.SECONDS);
    }

    StreamConfigProvider getDefaultProvider(String configFile) throws Exception {
        return new DefaultStreamConfigProvider(configFile, configExecutorService, 1, TimeUnit.SECONDS);
    }

    StreamConfigProvider getNullProvider() throws Exception {
        return new NullStreamConfigProvider();
    }

    @Test
    public void testServiceProviderWithConfigRouters() throws Exception {
        getServiceProvider(DistributedLogConfiguration.BKDL_STREAM_CONFIG_ROUTER_CLASS_DEFAULT);
        getServiceProvider("com.twitter.distributedlog.service.config.IdentityConfigRouter");
        getServiceProvider("com.twitter.distributedlog.service.config.EventbusPartitionConfigRouter");
    }

    @Test
    public void testServiceProviderWithMissingConfig() throws Exception {
        StreamConfigProvider provider = getServiceProvider(IdentityConfigRouter.class.getName());
        Optional<DynamicDistributedLogConfiguration> config = provider.getDynamicStreamConfig("stream1");
        assertFalse(config.isPresent());
    }

    @Test
    public void testServiceProviderWithBadConfigRouter() throws Exception {
        try {
            getServiceProvider("badclassname");
            fail("Should have thrown for bad class");
        } catch (RuntimeException ex) {
        }
    }

    @Test
    public void testDefaultProvider() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        writer.setProperty("retention-size", "99");
        writer.save();
        StreamConfigProvider provider = getDefaultProvider(writer.getFile().getPath());
        Optional<DynamicDistributedLogConfiguration> config1 = provider.getDynamicStreamConfig("stream1");
        Optional<DynamicDistributedLogConfiguration> config2 = provider.getDynamicStreamConfig("stream2");
        assertTrue(config1.isPresent());
        assertTrue(config1.get() == config2.get());
        assertEquals(99, config1.get().getRetentionPeriodHours());
    }

    @Test
    public void testNullProvider() throws Exception {
        StreamConfigProvider provider = getNullProvider();
        Optional<DynamicDistributedLogConfiguration> config1 = provider.getDynamicStreamConfig("stream1");
        Optional<DynamicDistributedLogConfiguration> config2 = provider.getDynamicStreamConfig("stream2");
        assertFalse(config1.isPresent());
        assertTrue(config1 == config2);
    }
}
