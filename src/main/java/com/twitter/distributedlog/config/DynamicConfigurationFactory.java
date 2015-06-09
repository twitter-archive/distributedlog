package com.twitter.distributedlog.config;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.twitter.distributedlog.DistributedLogConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates creation of DynamicDistributedLogConfiguration instances. Ensures one instance per
 * factory.
 * Notes:
 * Once loaded, stays loaded until shutdown. Caller ensures small finite number of configs are created.
 */
public class DynamicConfigurationFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigurationFactory.class);

    private final Map<String, DynamicDistributedLogConfiguration> dynamicConfigs;
    private final ConcurrentBaseConfiguration defaultConf;
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;

    public DynamicConfigurationFactory(ScheduledExecutorService executorService, int reloadPeriod, TimeUnit reloadUnit,
                                       ConcurrentBaseConfiguration defaultConf) {
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        this.dynamicConfigs = new HashMap<String, DynamicDistributedLogConfiguration>();
        this.defaultConf = defaultConf;
    }

    public synchronized Optional<DynamicDistributedLogConfiguration> getDynamicConfiguration(String configPath)
            throws ConfigurationException {
        Preconditions.checkNotNull(configPath);
        try {
            if (!dynamicConfigs.containsKey(configPath)) {
                File configFile = new File(configPath);
                PropertiesConfigurationBuilder properties = new PropertiesConfigurationBuilder(configFile.toURI().toURL());
                DynamicFileConfiguration concurrentConf =
                        new DynamicFileConfiguration(properties, executorService, reloadPeriod, reloadUnit);
                DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(concurrentConf, defaultConf);
                dynamicConfigs.put(configPath, dynConf);
                LOG.info("Loaded dynamic configuration at {}", configPath);
            }
            return Optional.of(dynamicConfigs.get(configPath));
        } catch (MalformedURLException ex) {
            throw new ConfigurationException(ex);
        }
    }
}
