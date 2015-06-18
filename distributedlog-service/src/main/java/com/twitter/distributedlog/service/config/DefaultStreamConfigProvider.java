package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicFileConfiguration;
import com.twitter.distributedlog.config.PropertiesConfigurationBuilder;

import java.io.File;
import java.net.MalformedURLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For all streams return the same dynamic config based on configFile.
 */
public class DefaultStreamConfigProvider implements StreamConfigProvider {
    static final Logger LOG = LoggerFactory.getLogger(DefaultStreamConfigProvider.class);

    private final Optional<DynamicDistributedLogConfiguration> dynConf;

    public DefaultStreamConfigProvider(String configFilePath, ScheduledExecutorService executorService, int reloadPeriod,
                                       TimeUnit reloadUnit) throws ConfigurationException {
        try {
            File configFile = new File(configFilePath);
            PropertiesConfigurationBuilder properties = new PropertiesConfigurationBuilder(configFile.toURI().toURL());
            DynamicFileConfiguration concurrentConf =
                    new DynamicFileConfiguration(properties, executorService, reloadPeriod, reloadUnit);
            ConcurrentConstConfiguration defaultConf = new ConcurrentConstConfiguration(new DistributedLogConfiguration());
            this.dynConf = Optional.of(new DynamicDistributedLogConfiguration(concurrentConf, defaultConf));
        } catch (MalformedURLException ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Optional<DynamicDistributedLogConfiguration> getDynamicStreamConfig(String streamName) {
        return dynConf;
    }
}
