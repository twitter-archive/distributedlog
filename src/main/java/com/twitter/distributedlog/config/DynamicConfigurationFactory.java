package com.twitter.distributedlog.config;

import com.google.common.base.Optional;

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
 * Encapsulates creation of DynamicDistributedLogConfiguration instances. Ensure one instance per
 * factory.
 * Notes:
 * Once loaded, stays loaded until shutdown. Caller ensures small finite number of configs are created.
 */
public class DynamicConfigurationFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigurationFactory.class);

    private final Map<String, DynamicDistributedLogConfiguration> dynamicConfigs;
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;

    public DynamicConfigurationFactory(ScheduledExecutorService executorService, int reloadPeriod, TimeUnit reloadUnit) {
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        this.dynamicConfigs = new HashMap<String, DynamicDistributedLogConfiguration>();
    }

    public synchronized Optional<DynamicDistributedLogConfiguration> getDynamicConfiguration(String configPath)
            throws ConfigurationException {
        try {
            if (!dynamicConfigs.containsKey(configPath)) {
                File configFile = new File(configPath);
                PropertiesConfigurationBuilder properties = new PropertiesConfigurationBuilder(configFile.toURI().toURL());
                DynamicFileConfiguration concurrentConf =
                        new DynamicFileConfiguration(properties, executorService, reloadPeriod, reloadUnit);
                DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(concurrentConf);
                dynamicConfigs.put(configPath, dynConf);
            }
            return Optional.of(dynamicConfigs.get(configPath));
        } catch (ConfigurationException ex) {
            if (ex.getCause() instanceof FileNotFoundException) {
                return Optional.<DynamicDistributedLogConfiguration>absent();
            } else {
                throw ex;
            }
        } catch (MalformedURLException ex) {
            throw new ConfigurationException(ex);
        }
    }
}