package com.twitter.distributedlog.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.FileConfiguration;

/**
 * Abstract out FileConfiguration subclass construction.
 */
public interface FileConfigurationBuilder {
    FileConfiguration getConfiguration() throws ConfigurationException;
}
