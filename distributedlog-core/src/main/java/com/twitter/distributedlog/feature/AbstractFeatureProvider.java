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
package com.twitter.distributedlog.feature;

import com.twitter.distributedlog.DistributedLogConfiguration;
import org.apache.bookkeeper.feature.CacheableFeatureProvider;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Decider based feature provider
 */
public abstract class AbstractFeatureProvider<T extends Feature> extends CacheableFeatureProvider<T> {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractFeatureProvider.class);

    public static FeatureProvider getFeatureProvider(String rootScope,
                                                     DistributedLogConfiguration conf,
                                                     StatsLogger statsLogger)
            throws IOException {
        Class<? extends FeatureProvider> featureProviderClass;
        try {
            featureProviderClass = conf.getFeatureProviderClass();
        } catch (ConfigurationException e) {
            throw new IOException("Can't initialize the feature provider : ", e);
        }
        // create feature provider
        Constructor<? extends FeatureProvider> constructor;
        try {
            constructor = featureProviderClass.getDeclaredConstructor(
                    String.class,
                    DistributedLogConfiguration.class,
                    StatsLogger.class);
        } catch (NoSuchMethodException e) {
            throw new IOException("No constructor found for feature provider class " + featureProviderClass + " : ", e);
        }
        try {
            return constructor.newInstance(rootScope, conf, statsLogger);
        } catch (InstantiationException e) {
            throw new IOException("Failed to instantiate feature provider : ", e);
        } catch (IllegalAccessException e) {
            throw new IOException("Encountered illegal access when instantiating feature provider : ", e);
        } catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            if (targetException instanceof IOException) {
                throw (IOException) targetException;
            } else {
                throw new IOException("Encountered invocation target exception while instantiating feature provider : ", e);
            }
        }
    }

    protected final DistributedLogConfiguration conf;
    protected final StatsLogger statsLogger;

    protected AbstractFeatureProvider(String rootScope,
                                      DistributedLogConfiguration conf,
                                      StatsLogger statsLogger) {
        super(rootScope);
        this.conf = conf;
        this.statsLogger = statsLogger;
    }

    /**
     * Start the feature provider.
     *
     * @throws IOException when failed to start the feature provider.
     */
    public void start() throws IOException {
        // no-op
    }

    /**
     * Stop the feature provider.
     */
    public void stop() {
        // no-op
    }

}
