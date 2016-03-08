package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.config.PropertiesWriter;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.twitter.distributedlog.service.streamset.EventBusStreamPartitionConverter;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.twitter.distributedlog.DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS;
import static com.twitter.distributedlog.DistributedLogConfiguration.BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT;
import static org.junit.Assert.*;

public class TestStreamConfigProvider {
    private static final String DEFAULT_CONFIG_PATH = "conf";
    private final String defaultConfigFile;
    private final ScheduledExecutorService configExecutorService;

    public TestStreamConfigProvider() throws Exception {
        this.configExecutorService = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Dyncfg-%d").build());
        PropertiesWriter writer = new PropertiesWriter();
        writer.save();
        this.defaultConfigFile = writer.getFile().getPath();
    }

    StreamConfigProvider getServiceProvider(StreamPartitionConverter converter)
            throws Exception {
        return getServiceProvider(converter, DEFAULT_CONFIG_PATH);
    }

    StreamConfigProvider getServiceProvider(StreamPartitionConverter converter, String configPath)
            throws Exception {
        return new ServiceStreamConfigProvider(configPath, defaultConfigFile, converter,
                                               configExecutorService, 1, TimeUnit.SECONDS);
    }

    /**
     * Create a few stream config files under a temp directory, and confirm that stream override takes
     * effect when we try to retrieve each of the files.
     */
    @Test
    public void testServiceProviderWithConfigLayout() throws Exception {
        File tempDir = File.createTempFile("test", "dir");
        tempDir.delete();
        tempDir.mkdir();
        PropertiesWriter writer = null;
        writer = new PropertiesWriter(new File(tempDir, "stream1.conf"));
        writer.setProperty(BKDL_RETENTION_PERIOD_IN_HOURS, "66");
        writer.save();
        writer = new PropertiesWriter(new File(tempDir, "stream2.conf"));
        writer.setProperty(BKDL_RETENTION_PERIOD_IN_HOURS, "88");
        writer.save();
        StreamConfigProvider provider = getServiceProvider(new EventBusStreamPartitionConverter(), tempDir.getPath());
        Optional<DynamicDistributedLogConfiguration> config1 = provider.getDynamicStreamConfig("stream1");
        Optional<DynamicDistributedLogConfiguration> config2 = provider.getDynamicStreamConfig("stream2");
        Optional<DynamicDistributedLogConfiguration> config3 = provider.getDynamicStreamConfig("stream3");
        assertTrue(config1.isPresent());
        assertTrue(config2.isPresent());
        assertTrue(config3.isPresent());
        assertEquals(66, config1.get().getRetentionPeriodHours());
        assertEquals(88, config2.get().getRetentionPeriodHours());
        assertEquals(BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT,
                config3.get().getRetentionPeriodHours());
    }
}
