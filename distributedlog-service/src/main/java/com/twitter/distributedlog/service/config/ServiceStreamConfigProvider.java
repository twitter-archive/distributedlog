package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.ConfigurationSubscription;
import com.twitter.distributedlog.config.DynamicConfigurationFactory;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.config.PropertiesConfigurationBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide per stream configuration to DistributedLog service layer.
 */
public class ServiceStreamConfigProvider implements StreamConfigProvider {
    static final Logger LOG = LoggerFactory.getLogger(ServiceStreamConfigProvider.class);

    private final File configBaseDir;
    private final File defaultConfigFile;
    private final StreamConfigRouter configRouter;
    private final DynamicConfigurationFactory configFactory;
    private final DynamicDistributedLogConfiguration defaultDynConf;
    private final static String CONFIG_EXTENSION = "conf";

    public ServiceStreamConfigProvider(String configPath, String defaultConfigPath, String routerClass,
                                       ScheduledExecutorService executorService, int reloadPeriod, TimeUnit reloadUnit)
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
        this.configRouter = ReflectionUtils.newInstance(routerClass, StreamConfigRouter.class);
        this.configFactory = new DynamicConfigurationFactory(executorService, reloadPeriod, reloadUnit);
        // We know it exists from the check above.
        this.defaultDynConf = configFactory.getDynamicConfiguration(defaultConfigPath).get();
    }

    @Override
    public Optional<DynamicDistributedLogConfiguration> getDynamicStreamConfig(String streamName) {
        String configName = configRouter.getConfig(streamName);
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
