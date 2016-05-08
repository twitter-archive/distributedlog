package com.twitter.distributedlog.feature;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.ConcurrentBaseConfiguration;
import com.twitter.distributedlog.config.ConfigurationListener;
import com.twitter.distributedlog.config.ConfigurationSubscription;
import com.twitter.distributedlog.config.FileConfigurationBuilder;
import com.twitter.distributedlog.config.PropertiesConfigurationBuilder;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Feature Provider based dynamic configuration.
 */
public class DynamicConfigurationFeatureProvider extends AbstractFeatureProvider
        implements ConfigurationListener {

    private final ConcurrentBaseConfiguration featuresConf;
    private ConfigurationSubscription featuresConfSubscription;
    private final ConcurrentMap<String, SettableFeature> features;
    private final ScheduledExecutorService executorService;

    public DynamicConfigurationFeatureProvider(String rootScope,
                                               DistributedLogConfiguration conf,
                                               StatsLogger statsLogger) {
        super(rootScope, conf, statsLogger);
        this.features = new ConcurrentHashMap<String, SettableFeature>();
        this.featuresConf = new ConcurrentBaseConfiguration();
        this.executorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("DynamicConfigurationFeatureProvider-%d").build());
    }

    ConcurrentBaseConfiguration getFeatureConf() {
        return featuresConf;
    }

    ConfigurationSubscription getFeatureConfSubscription() {
        return featuresConfSubscription;
    }

    @Override
    public void start() throws IOException {
        List<FileConfigurationBuilder> fileConfigBuilders =
                Lists.newArrayListWithExpectedSize(2);
        String baseConfigPath = conf.getFileFeatureProviderBaseConfigPath();
        Preconditions.checkNotNull(baseConfigPath);
        File baseConfigFile = new File(baseConfigPath);
        FileConfigurationBuilder baseProperties =
                new PropertiesConfigurationBuilder(baseConfigFile.toURI().toURL());
        fileConfigBuilders.add(baseProperties);
        String overlayConfigPath = conf.getFileFeatureProviderOverlayConfigPath();
        if (null != overlayConfigPath) {
            File overlayConfigFile = new File(overlayConfigPath);
            FileConfigurationBuilder overlayProperties =
                    new PropertiesConfigurationBuilder(overlayConfigFile.toURI().toURL());
            fileConfigBuilders.add(overlayProperties);
        }
        try {
            this.featuresConfSubscription = new ConfigurationSubscription(
                    this.featuresConf,
                    fileConfigBuilders,
                    executorService,
                    conf.getDynamicConfigReloadIntervalSec(),
                    TimeUnit.SECONDS);
        } catch (ConfigurationException e) {
            throw new IOException("Failed to register subscription on features configuration");
        }
        this.featuresConfSubscription.registerListener(this);
    }

    @Override
    public void stop() {
        this.executorService.shutdown();
    }

    @Override
    public void onReload(ConcurrentBaseConfiguration conf) {
        for (Map.Entry<String, SettableFeature> feature : features.entrySet()) {
            String featureName = feature.getKey();
            int availability = conf.getInt(featureName, 0);
            if (availability != feature.getValue().availability()) {
                feature.getValue().set(availability);
                logger.info("Reload feature {}={}", featureName, availability);
            }
        }
    }

    @Override
    protected Feature makeFeature(String featureName) {
        return ConfigurationFeatureProvider.makeFeature(
                featuresConf, features, featureName);
    }

    @Override
    protected FeatureProvider makeProvider(String fullScopeName) {
        return new ConfigurationFeatureProvider(
                fullScopeName, featuresConf, features);
    }
}
