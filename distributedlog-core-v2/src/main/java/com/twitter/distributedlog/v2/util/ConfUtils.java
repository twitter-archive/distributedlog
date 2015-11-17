package com.twitter.distributedlog.v2.util;

import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

public class ConfUtils {

    /**
     * Load configurations with prefixed <i>section</i> from source configuration <i>srcConf</i> into
     * target configuration <i>targetConf</i>.
     *
     * @param targetConf
     *          Target Configuration
     * @param srcConf
     *          Source Configuration
     * @param section
     *          Section Key
     */
    public static void loadConfiguration(Configuration targetConf, Configuration srcConf, String section) {
        Iterator<String> confKeys = srcConf.getKeys();
        while (confKeys.hasNext()) {
            String key = confKeys.next();
            if (key.startsWith(section)) {
                targetConf.setProperty(key.substring(section.length()), srcConf.getProperty(key));
            }
        }
    }
}
