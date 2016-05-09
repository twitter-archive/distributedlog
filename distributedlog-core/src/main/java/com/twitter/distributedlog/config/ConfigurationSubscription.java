/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.config;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
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
    private final List<FileConfigurationBuilder> fileConfigBuilders;
    private final List<FileConfiguration> fileConfigs;
    private final CopyOnWriteArraySet<ConfigurationListener> confListeners;

    public ConfigurationSubscription(ConcurrentBaseConfiguration viewConfig,
                                     List<FileConfigurationBuilder> fileConfigBuilders,
                                     ScheduledExecutorService executorService,
                                     int reloadPeriod,
                                     TimeUnit reloadUnit)
            throws ConfigurationException {
        Preconditions.checkNotNull(fileConfigBuilders);
        Preconditions.checkArgument(!fileConfigBuilders.isEmpty());
        Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(viewConfig);
        this.viewConfig = viewConfig;
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        this.fileConfigBuilders = fileConfigBuilders;
        this.fileConfigs = Lists.newArrayListWithExpectedSize(this.fileConfigBuilders.size());
        this.confListeners = new CopyOnWriteArraySet<ConfigurationListener>();
        reload();
        scheduleReload();
    }

    public void registerListener(ConfigurationListener listener) {
        this.confListeners.add(listener);
    }

    public void unregisterListener(ConfigurationListener listener) {
        this.confListeners.remove(listener);
    }

    private boolean initConfig() {
        if (fileConfigs.isEmpty()) {
            try {
                for (FileConfigurationBuilder fileConfigBuilder : fileConfigBuilders) {
                    FileConfiguration fileConfig = fileConfigBuilder.getConfiguration();
                    FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
                    reloadingStrategy.setRefreshDelay(0);
                    fileConfig.setReloadingStrategy(reloadingStrategy);
                    fileConfigs.add(fileConfig);
                }
            } catch (ConfigurationException ex) {
                if (!fileNotFound(ex)) {
                    LOG.error("Config init failed {}", ex);
                }
            }
        }
        return !fileConfigs.isEmpty();
    }

    private void scheduleReload() {
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reload();
            }
        }, 0, reloadPeriod, reloadUnit);
    }

    @VisibleForTesting
    void reload() {
        // No-op if already loaded.
        if (!initConfig()) {
            return;
        }
        // Reload if config exists.
        Set<String> confKeys = Sets.newHashSet();
        for (FileConfiguration fileConfig : fileConfigs) {
            LOG.debug("Check and reload config, file={}, lastModified={}", fileConfig.getFile(),
                    fileConfig.getFile().lastModified());
            fileConfig.reload();
            // load keys
            Iterator keyIter = fileConfig.getKeys();
            while (keyIter.hasNext()) {
                String key = (String) keyIter.next();
                confKeys.add(key);
            }
        }
        // clear unexisted keys
        Iterator viewIter = viewConfig.getKeys();
        while (viewIter.hasNext()) {
            String key = (String) viewIter.next();
            if (!confKeys.contains(key)) {
                clearViewProperty(key);
            }
        }
        LOG.info("Reload features : {}", confKeys);
        // load keys from files
        for (FileConfiguration fileConfig : fileConfigs) {
            try {
                loadView(fileConfig);
            } catch (Exception ex) {
                if (!fileNotFound(ex)) {
                    LOG.error("Config reload failed for file {}", fileConfig.getFileName(), ex);
                }
            }
        }
        for (ConfigurationListener listener : confListeners) {
            listener.onReload(viewConfig);
        }
    }

    private boolean fileNotFound(Exception ex) {
        return ex instanceof FileNotFoundException ||
                ex.getCause() != null && ex.getCause() instanceof FileNotFoundException;
    }

    private void loadView(FileConfiguration fileConfig) {
        Iterator fileIter = fileConfig.getKeys();
        while (fileIter.hasNext()) {
            String key = (String) fileIter.next();
            setViewProperty(fileConfig, key, fileConfig.getProperty(key));
        }
    }

    private void clearViewProperty(String key) {
        LOG.debug("Removing property, key={}", key);
        viewConfig.clearProperty(key);
    }

    private void setViewProperty(FileConfiguration fileConfig,
                                 String key,
                                 Object value) {
        if (!viewConfig.containsKey(key) || !viewConfig.getProperty(key).equals(value)) {
            LOG.debug("Setting property, key={} value={}", key, fileConfig.getProperty(key));
            viewConfig.setProperty(key, fileConfig.getProperty(key));
        }
    }
}
