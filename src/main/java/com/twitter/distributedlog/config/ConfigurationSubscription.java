package com.twitter.distributedlog.config;

import com.google.common.base.Preconditions;

import java.lang.UnsupportedOperationException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigurationSubscription publishes a reloading, thread-safe view of file configuration. The class
 * periodically calls FileConfiguration.reload on the underlying conf, and propagates changes to the
 * concurrent config. The configured FileChangedReloadingStrategy ensures that file config will only
 * be reloaded if something changed.
 * Notes:
 * 1. Reload schedule is never terminated. The assumption is a finite number of these are started
 * at the calling layer, and terminated only once the executor service is shut down.
 * 2. The underlying FileConfiguration is not at all thread-safe, so its important to ensure access
 * to this object is always single threaded.
 */
public class ConfigurationSubscription {
    static final Logger LOG = LoggerFactory.getLogger(ConfigurationSubscription.class);

    private final ConcurrentBaseConfiguration viewConfig;
    private final FileConfiguration fileConfig;
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;

    public ConfigurationSubscription(ConcurrentBaseConfiguration viewConfig, FileConfigurationBuilder builder,
                                     ScheduledExecutorService executorService, int reloadPeriod, TimeUnit reloadUnit)
                                     throws ConfigurationException {
        Preconditions.checkNotNull(builder);
        Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(viewConfig);
        this.viewConfig = viewConfig;
        this.fileConfig = builder.getConfiguration();
        FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
        reloadingStrategy.setRefreshDelay(0);
        fileConfig.setReloadingStrategy(reloadingStrategy);
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        reload();
        scheduleReload();
    }

    private void scheduleReload() {
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reload();
            }
        }, 0, reloadPeriod, reloadUnit);
    }

    private void reload() {
        try {
            // Note: This reloads the config view every time which will cause some
            // contention. Could be optimized using config events.
            LOG.debug("Check and reload config, file={}, lastModified={}", fileConfig.getFile(),
                    fileConfig.getFile().lastModified());
            fileConfig.reload();
            loadView();
        } catch (Exception ex) {
            LOG.error("Config reload failed for file {}", fileConfig.getFile(), ex);
        }
    }

    private void loadView() {
        Iterator viewIter = viewConfig.getKeys();
        while (viewIter.hasNext()) {
            String key = (String) viewIter.next();
            if (!fileConfig.containsKey(key)) {
                clearViewProperty(key);
            }
        }
        Iterator fileIter = fileConfig.getKeys();
        while (fileIter.hasNext()) {
            String key = (String) fileIter.next();
            setViewProperty(key, fileConfig.getProperty(key));
        }
    }

    private void clearViewProperty(String key) {
        LOG.debug("Removing property, key={}", key);
        viewConfig.clearProperty(key);
    }

    private void setViewProperty(String key, Object value) {
        if (!viewConfig.containsKey(key) || !viewConfig.getProperty(key).equals(value)) {
            LOG.debug("Setting property, key={} value={}", key, fileConfig.getProperty(key));
            viewConfig.setProperty(key, fileConfig.getProperty(key));
        }
    }
}
