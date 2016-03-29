package com.twitter.distributedlog.service.config;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.ConfigurationSubscription;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.config.FileConfigurationBuilder;
import com.twitter.distributedlog.config.PropertiesConfigurationBuilder;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * For all streams return the same dynamic config based on configFile.
 */
public class DefaultStreamConfigProvider implements StreamConfigProvider {
    static final Logger LOG = LoggerFactory.getLogger(DefaultStreamConfigProvider.class);

    private final Optional<DynamicDistributedLogConfiguration> dynConf;
    private final ConfigurationSubscription confSub;

    public DefaultStreamConfigProvider(String configFilePath, ScheduledExecutorService executorService, int reloadPeriod,
                                       TimeUnit reloadUnit) throws ConfigurationException {
        try {
            File configFile = new File(configFilePath);
            FileConfigurationBuilder properties = new PropertiesConfigurationBuilder(configFile.toURI().toURL());
            ConcurrentConstConfiguration defaultConf = new ConcurrentConstConfiguration(new DistributedLogConfiguration());
            DynamicDistributedLogConfiguration conf = new DynamicDistributedLogConfiguration(defaultConf);
            List<FileConfigurationBuilder> fileConfigBuilders = Lists.newArrayList(properties);
            confSub = new ConfigurationSubscription(conf, fileConfigBuilders, executorService, reloadPeriod, reloadUnit);
            this.dynConf = Optional.of(conf);
        } catch (MalformedURLException ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Optional<DynamicDistributedLogConfiguration> getDynamicStreamConfig(String streamName) {
        return dynConf;
    }
}
