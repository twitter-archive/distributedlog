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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates creation of DynamicDistributedLogConfiguration instances. Ensures one instance per
 * factory.
 * Notes:
 * Once loaded, stays loaded until shutdown. Caller ensures small finite number of configs are created.
 */
public class DynamicConfigurationFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigurationFactory.class);

    private final Map<String, DynamicDistributedLogConfiguration> dynamicConfigs;
    private final List<ConfigurationSubscription> subscriptions;
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;

    public DynamicConfigurationFactory(ScheduledExecutorService executorService, int reloadPeriod, TimeUnit reloadUnit) {
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        this.dynamicConfigs = new HashMap<String, DynamicDistributedLogConfiguration>();
        this.subscriptions = new LinkedList<ConfigurationSubscription>();
    }

    public synchronized Optional<DynamicDistributedLogConfiguration> getDynamicConfiguration(
            String configPath,
            ConcurrentBaseConfiguration defaultConf) throws ConfigurationException {
        Preconditions.checkNotNull(configPath);
        try {
            if (!dynamicConfigs.containsKey(configPath)) {
                File configFile = new File(configPath);
                FileConfigurationBuilder properties =
                        new PropertiesConfigurationBuilder(configFile.toURI().toURL());
                DynamicDistributedLogConfiguration dynConf =
                        new DynamicDistributedLogConfiguration(defaultConf);
                List<FileConfigurationBuilder> fileConfigBuilders = Lists.newArrayList(properties);
                ConfigurationSubscription subscription = new ConfigurationSubscription(
                        dynConf, fileConfigBuilders, executorService, reloadPeriod, reloadUnit);
                subscriptions.add(subscription);
                dynamicConfigs.put(configPath, dynConf);
                LOG.info("Loaded dynamic configuration at {}", configPath);
            }
            return Optional.of(dynamicConfigs.get(configPath));
        } catch (MalformedURLException ex) {
            throw new ConfigurationException(ex);
        }
    }

    public synchronized Optional<DynamicDistributedLogConfiguration> getDynamicConfiguration(String configPath) throws ConfigurationException {
        return getDynamicConfiguration(configPath, new ConcurrentConstConfiguration(new DistributedLogConfiguration()));
    }
}
