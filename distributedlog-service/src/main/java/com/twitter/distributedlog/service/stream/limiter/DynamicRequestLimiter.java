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
package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.RequestLimiter;

import java.io.Closeable;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamically rebuild a rate limiter when the supplied dynamic config changes. Subclasses
 * implement build() to build the limiter. DynamicRequestLimiter must be closed to deregister
 * the config listener.
 */
public abstract class DynamicRequestLimiter<Request> implements RequestLimiter<Request>, Closeable {
    static final Logger LOG = LoggerFactory.getLogger(DynamicRequestLimiter.class);

    private final ConfigurationListener listener;
    private final Feature rateLimitDisabledFeature;
    volatile RequestLimiter<Request> limiter;
    final DynamicDistributedLogConfiguration dynConf;

    public DynamicRequestLimiter(DynamicDistributedLogConfiguration dynConf,
                                 StatsLogger statsLogger, Feature rateLimitDisabledFeature) {
        final StatsLogger limiterStatsLogger = statsLogger.scope("dynamic");
        this.dynConf = dynConf;
        this.rateLimitDisabledFeature = rateLimitDisabledFeature;
        this.listener = new ConfigurationListener() {
            @Override
            public void configurationChanged(ConfigurationEvent event) {
                // Note that this method may be called several times if several config options
                // are changed. The effect is harmless except that we create and discard more
                // objects than we need to.
                LOG.debug("Config changed callback invoked with event {} {} {} {}", new Object[] {
                        event.getPropertyName(), event.getPropertyValue(), event.getType(),
                        event.isBeforeUpdate()});
                if (!event.isBeforeUpdate()) {
                    limiterStatsLogger.getCounter("config_changed").inc();
                    LOG.debug("Rebuilding limiter");
                    limiter = build();
                }
            }
        };
        LOG.debug("Registering config changed callback");
        dynConf.addConfigurationListener(listener);
    }

    public void initialize() {
        this.limiter = build();
    }

    @Override
    public void apply(Request request) throws OverCapacityException {
        if (rateLimitDisabledFeature.isAvailable()) {
            return;
        }
        limiter.apply(request);
    }

    @Override
    public void close() {
        boolean success = dynConf.removeConfigurationListener(listener);
        LOG.debug("Deregistering config changed callback success={}", success);
    }

   /**
    * Build the underlying limiter. Called when DynamicRequestLimiter detects config has changed.
    * This may be called multiple times so the method should be cheap.
    */
    protected abstract RequestLimiter<Request> build();
}
