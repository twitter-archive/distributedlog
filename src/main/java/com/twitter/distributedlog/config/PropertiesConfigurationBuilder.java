package com.twitter.distributedlog.config;

import java.net.URL;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Hide PropertiesConfiguration dependency.
 */
public class PropertiesConfigurationBuilder implements FileConfigurationBuilder {
    private URL url;

    public PropertiesConfigurationBuilder(URL url) {
        this.url = url;
    }

    @Override
    public FileConfiguration getConfiguration() throws ConfigurationException {
        return new PropertiesConfiguration(url);
    }
}
