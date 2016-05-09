package com.twitter.distributedlog.config;

/**
 * Configuration listener triggered when reloading configuration settings.
 */
public interface ConfigurationListener {

    /**
     * Reload the configuration.
     *
     * @param conf configuration to reload
     */
    void onReload(ConcurrentBaseConfiguration conf);

}
