package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.config.PropertiesWriter;
import com.twitter.distributedlog.service.streamset.IdentityStreamPartitionConverter;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.twitter.distributedlog.DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS;
import static org.junit.Assert.*;

public class TestStreamConfigProvider {
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

    StreamConfigProvider getServiceProvider(StreamPartitionConverter converter)
            throws Exception {
        return getServiceProvider(converter, DEFAULT_CONFIG_DIR);
    }

    StreamConfigProvider getServiceProvider(
            StreamPartitionConverter converter,
            String configPath,
            String defaultPath) throws Exception {
        return new ServiceStreamConfigProvider(
                configPath,
                defaultPath,
                converter,
                configExecutorService,
                1,
                TimeUnit.SECONDS);
    }

    StreamConfigProvider getServiceProvider(
            StreamPartitionConverter converter,
            String configPath) throws Exception {
        return getServiceProvider(converter, configPath, defaultConfigPath);
    }

    StreamConfigProvider getDefaultProvider(String configFile) throws Exception {
        return new DefaultStreamConfigProvider(configFile, configExecutorService, 1, TimeUnit.SECONDS);
    }

    StreamConfigProvider getNullProvider() throws Exception {
        return new NullStreamConfigProvider();
    }

    @Test(timeout = 60000)
    public void testServiceProviderWithConfigRouters() throws Exception {
        getServiceProvider(new IdentityStreamPartitionConverter());
    }

    @Test(timeout = 60000)
    public void testServiceProviderWithMissingConfig() throws Exception {
        StreamConfigProvider provider = getServiceProvider(new IdentityStreamPartitionConverter());
        Optional<DynamicDistributedLogConfiguration> config = provider.getDynamicStreamConfig("stream1");
        assertTrue(config.isPresent());
    }

    @Test(timeout = 60000)
    public void testServiceProviderWithDefaultConfigPath() throws Exception {
        // Default config with property set.
        PropertiesWriter writer1 = new PropertiesWriter();
        writer1.setProperty("rpsStreamAcquireServiceLimit", "191919");
        writer1.save();
        String fallbackConfPath1 = writer1.getFile().getPath();
        StreamConfigProvider provider1 = getServiceProvider(new IdentityStreamPartitionConverter(),
                DEFAULT_CONFIG_DIR, fallbackConfPath1);
        Optional<DynamicDistributedLogConfiguration> config1 = provider1.getDynamicStreamConfig("stream1");

        // Empty default config.
        PropertiesWriter writer2 = new PropertiesWriter();
        writer2.save();
        String fallbackConfPath2 = writer2.getFile().getPath();
        StreamConfigProvider provider2 = getServiceProvider(new IdentityStreamPartitionConverter(),
                DEFAULT_CONFIG_DIR, fallbackConfPath2);
        Optional<DynamicDistributedLogConfiguration> config2 = provider2.getDynamicStreamConfig("stream1");

        assertEquals(191919, config1.get().getRpsStreamAcquireServiceLimit());
        assertEquals(-1, config2.get().getRpsStreamAcquireServiceLimit());
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testNullProvider() throws Exception {
        StreamConfigProvider provider = getNullProvider();
        Optional<DynamicDistributedLogConfiguration> config1 = provider.getDynamicStreamConfig("stream1");
        Optional<DynamicDistributedLogConfiguration> config2 = provider.getDynamicStreamConfig("stream2");
        assertFalse(config1.isPresent());
        assertTrue(config1 == config2);
    }
}
