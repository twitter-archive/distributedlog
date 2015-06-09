package com.twitter.distributedlog.config;

import com.google.common.base.Preconditions;

import java.lang.UnsupportedOperationException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DynamicFileConfiguration publishes a reloading, thread-safe view of file configuration. The class
 * periodically calls FileConfiguration.reload on the underlying conf, and propagates changes to the
 * concurrent config. The configured FileChangedReloadingStrategy ensures that file config will only
 * be reloaded if something changed.
 * Notes:
 * 1. Reload schedule is never terminated. The assumption is a finite number of these are started
 * at the calling layer, and terminated only once the executor service is shut down.
 * 2. The underlying FileConfiguration is not at all thread-safe, so its important to ensure access
 * to this object is always single threaded.
 */
public class DynamicFileConfiguration extends ConcurrentBaseConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(DynamicFileConfiguration.class);

    private final FileConfiguration fileConfig;
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;

    public DynamicFileConfiguration(FileConfigurationBuilder builder, ScheduledExecutorService executorService,
                                    int reloadPeriod, TimeUnit reloadUnit) throws ConfigurationException {
        Preconditions.checkNotNull(builder);
        Preconditions.checkNotNull(executorService);
        this.fileConfig = builder.getConfiguration();
        FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
        reloadingStrategy.setRefreshDelay(0);
        fileConfig.setReloadingStrategy(reloadingStrategy);
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        setThrowExceptionOnMissing(false);
        loadView();
        scheduleReload();
    }

    private void scheduleReload() {
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // Note: This reloads the config view every time which will cause some
                // contention. Could be optimized using config events.
                LOG.debug("Check and reload config, file={}, lastModified={}", fileConfig.getFile(), fileConfig.getFile().lastModified());
                fileConfig.reload();
                loadView();
            }
        }, 0, reloadPeriod, reloadUnit);
    }

    private void loadView() {
        Iterator iter = getKeys();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            if (!fileConfig.containsKey(key)) {
                LOG.debug("Removing property, key={}", key);
                clearProperty(key);
            }
        }
        copy(fileConfig);
    }
}
