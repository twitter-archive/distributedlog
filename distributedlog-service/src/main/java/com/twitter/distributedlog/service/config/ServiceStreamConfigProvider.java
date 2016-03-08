package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.twitter.distributedlog.config.DynamicConfigurationFactory;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Provide per stream configuration to DistributedLog service layer.
 */
public class ServiceStreamConfigProvider implements StreamConfigProvider {
    static final Logger LOG = LoggerFactory.getLogger(ServiceStreamConfigProvider.class);

    private final File configBaseDir;
    private final File defaultConfigFile;
    private final StreamPartitionConverter partitionConverter;
    private final DynamicConfigurationFactory configFactory;
    private final DynamicDistributedLogConfiguration defaultDynConf;
    private final static String CONFIG_EXTENSION = "conf";

    public ServiceStreamConfigProvider(String configPath,
                                       String defaultConfigPath,
                                       StreamPartitionConverter partitionConverter,
                                       ScheduledExecutorService executorService,
                                       int reloadPeriod,
                                       TimeUnit reloadUnit)
                                       throws ConfigurationException {
        this.configBaseDir = new File(configPath);
        if (!configBaseDir.exists()) {
            throw new ConfigurationException("Stream configuration base directory " + configPath + " does not exist");
        }
        this.defaultConfigFile = new File(configPath);
        if (!defaultConfigFile.exists()) {
            throw new ConfigurationException("Stream configuration default config " + defaultConfigPath + " does not exist");
        }

        // Construct reloading default configuration
        this.partitionConverter = partitionConverter;
        this.configFactory = new DynamicConfigurationFactory(executorService, reloadPeriod, reloadUnit);
        // We know it exists from the check above.
        this.defaultDynConf = configFactory.getDynamicConfiguration(defaultConfigPath).get();
    }

    @Override
    public Optional<DynamicDistributedLogConfiguration> getDynamicStreamConfig(String streamName) {
        String configName = partitionConverter.convert(streamName).getStream();
        String configPath = getConfigPath(configName);
        Optional<DynamicDistributedLogConfiguration> dynConf = Optional.<DynamicDistributedLogConfiguration>absent();
        try {
            dynConf = configFactory.getDynamicConfiguration(configPath, defaultDynConf);
        } catch (ConfigurationException ex) {
            LOG.warn("Configuration exception for stream {} ({}) at {}",
                    new Object[] {streamName, configName, configPath, ex});
        }
        return dynConf;
    }

    private String getConfigPath(String configName) {
        return new File(configBaseDir, String.format("%s.%s", configName, CONFIG_EXTENSION)).getPath();
    }
}
