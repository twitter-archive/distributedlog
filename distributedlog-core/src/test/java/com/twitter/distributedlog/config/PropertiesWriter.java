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