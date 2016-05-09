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
package com.twitter.distributedlog.util;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
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
        Iterator confKeys = srcConf.getKeys();
        while (confKeys.hasNext()) {
            Object keyObject = confKeys.next();
            if (!(keyObject instanceof String)) {
                continue;
            }
            String key = (String) keyObject;
            if (key.startsWith(section)) {
                targetConf.setProperty(key.substring(section.length()), srcConf.getProperty(key));
            }
        }
    }

    /**
     * Create const dynamic configuration based on distributedlog configuration.
     *
     * @param conf
     *          static distributedlog configuration.
     * @return dynamic configuration
     */
    public static DynamicDistributedLogConfiguration getConstDynConf(DistributedLogConfiguration conf) {
        ConcurrentConstConfiguration constConf = new ConcurrentConstConfiguration(conf);
        return new DynamicDistributedLogConfiguration(constConf);
    }
}
