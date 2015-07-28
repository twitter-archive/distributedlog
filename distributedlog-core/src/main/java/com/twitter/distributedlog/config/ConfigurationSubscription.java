package com.twitter.distributedlog.config;

import com.google.common.base.Preconditions;

import java.io.FileNotFoundException;
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
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;
    private final FileConfigurationBuilder fileConfigBuilder;

    private FileConfiguration fileConfig;

    public ConfigurationSubscription(ConcurrentBaseConfiguration viewConfig, FileConfigurationBuilder fileConfigBuilder,
                                     ScheduledExecutorService executorService, int reloadPeriod, TimeUnit reloadUnit)
                                     throws ConfigurationException {
        Preconditions.checkNotNull(fileConfigBuilder);
        Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(viewConfig);
        this.viewConfig = viewConfig;
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        this.fileConfigBuilder = fileConfigBuilder;
        reload();
        scheduleReload();
    }

    private boolean initConfig() {
        try {
            if (null == fileConfig) {
                fileConfig = fileConfigBuilder.getConfiguration();
                FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
                reloadingStrategy.setRefreshDelay(0);
                fileConfig.setReloadingStrategy(reloadingStrategy);
            }
        } catch (ConfigurationException ex) {
            if (!fileNotFound(ex)) {
                LOG.error("Config init failed {}", ex);
            }
        }
        return null != fileConfig;
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
        // No-op if already loaded.
        if (!initConfig()) {
            return;
        }
        // Reload if config exists.
        try {
            LOG.debug("Check and reload config, file={}, lastModified={}", fileConfig.getFile(),
                    fileConfig.getFile().lastModified());
            fileConfig.reload();
            loadView(fileConfig);
        } catch (Exception ex) {
            if (!fileNotFound(ex)) {
                LOG.error("Config reload failed for file {}", fileConfig.getFileName(), ex);
            }
        }
    }

    private boolean fileNotFound(Exception ex) {
        return ex instanceof FileNotFoundException ||
                ex.getCause() != null && ex.getCause() instanceof FileNotFoundException;
    }

    private void loadView(FileConfiguration fileConfig) {
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
