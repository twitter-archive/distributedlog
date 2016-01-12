package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.config.PropertiesWriter;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.twitter.distributedlog.DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS;
import static org.junit.Assert.*;

public class TestStreamConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TestStreamConfigProvider.class);
    private static final String DEFAULT_CONFIG_DIR = "conf";
    private final String defaultConfigPath;
    private final ScheduledExecutorService configExecutorService;

    public TestStreamConfigProvider() throws Exception {
        this.configExecutorService = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Dyncfg-%d").build());
        PropertiesWriter writer = new PropertiesWriter();
        writer.save();
        this.defaultConfigPath = writer.getFile().getPath();
    }

    StreamConfigProvider getServiceProvider(String routerName) throws Exception {
        return getServiceProvider(routerName, DEFAULT_CONFIG_DIR);
    }

    StreamConfigProvider getServiceProvider(String routerName, String configPath, String defaultPath) throws Exception {
        return new ServiceStreamConfigProvider(configPath, defaultPath, routerName,
                                               configExecutorService, 1, TimeUnit.SECONDS);
    }

    StreamConfigProvider getServiceProvider(String routerName, String configPath) throws Exception {
        return getServiceProvider(routerName, configPath, defaultConfigPath);
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
    }

    @Test
    public void testServiceProviderWithMissingConfig() throws Exception {
        StreamConfigProvider provider = getServiceProvider(IdentityConfigRouter.class.getName());
        Optional<DynamicDistributedLogConfiguration> config = provider.getDynamicStreamConfig("stream1");
        assertTrue(config.isPresent());
    }

    @Test
    public void testServiceProviderWithDefaultConfigPath() throws Exception {
        // Default config with property set.
        PropertiesWriter writer1 = new PropertiesWriter();
        writer1.setProperty("rpsStreamAcquireServiceLimit", "191919");
        writer1.save();
        String fallbackConfPath1 = writer1.getFile().getPath();
        StreamConfigProvider provider1 = getServiceProvider(IdentityConfigRouter.class.getName(), DEFAULT_CONFIG_DIR, fallbackConfPath1);
        Optional<DynamicDistributedLogConfiguration> config1 = provider1.getDynamicStreamConfig("stream1");

        // Empty default config.
        PropertiesWriter writer2 = new PropertiesWriter();
        writer2.save();
        String fallbackConfPath2 = writer2.getFile().getPath();
        StreamConfigProvider provider2 = getServiceProvider(IdentityConfigRouter.class.getName(), DEFAULT_CONFIG_DIR, fallbackConfPath2);
        Optional<DynamicDistributedLogConfiguration> config2 = provider2.getDynamicStreamConfig("stream1");

        assertEquals(191919, config1.get().getRpsStreamAcquireServiceLimit());
        assertEquals(-1, config2.get().getRpsStreamAcquireServiceLimit());
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
        writer.setProperty(BKDL_RETENTION_PERIOD_IN_HOURS, "99");
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
