package com.twitter.distributedlog.config;

import com.google.common.base.Objects;

import org.apache.commons.configuration.Configuration;

class ConfigTestUtil {
    static void waitForConfig(Configuration conf, String name, String value) throws Exception {
        while (!Objects.equal(conf.getProperty(name), value)) {
            Thread.sleep(100);
        }
    }
}
