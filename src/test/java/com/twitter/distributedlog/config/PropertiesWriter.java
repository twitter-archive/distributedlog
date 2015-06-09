package com.twitter.distributedlog.config;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PropertiesWriter {
    static final Logger LOG = LoggerFactory.getLogger(PropertiesWriter.class);

    final FileOutputStream outputStream;
    final File tempFile;
    final Properties properties;

    PropertiesWriter() throws Exception {
        this.tempFile = File.createTempFile("temp", ".conf");
        tempFile.deleteOnExit();
        this.properties = new Properties();
        this.outputStream = new FileOutputStream(tempFile);
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public void save() throws Exception {
        FileOutputStream outputStream = new FileOutputStream(tempFile);
        properties.store(outputStream, null);
        tempFile.setLastModified(tempFile.lastModified()+1000);
        LOG.debug("save modified={}", tempFile.lastModified());
    }

    public File getFile() {
        return tempFile;
    }
}