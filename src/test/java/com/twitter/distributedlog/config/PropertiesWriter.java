package com.twitter.distributedlog.config;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesWriter {
    static final Logger LOG = LoggerFactory.getLogger(PropertiesWriter.class);

    final FileOutputStream outputStream;
    final File configFile;
    final Properties properties;

    public PropertiesWriter() throws Exception {
        this(null);
    }

    public PropertiesWriter(File configFile) throws Exception {
        if (null == configFile) {
            this.configFile = File.createTempFile("temp", ".conf");
        } else {
            this.configFile = configFile;
        }
        this.configFile.deleteOnExit();
        this.properties = new Properties();
        this.outputStream = new FileOutputStream(this.configFile);
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public void save() throws Exception {
        FileOutputStream outputStream = new FileOutputStream(configFile);
        properties.store(outputStream, null);
        configFile.setLastModified(configFile.lastModified()+1000);
        LOG.debug("save modified={}", configFile.lastModified());
    }

    public File getFile() {
        return configFile;
    }
}