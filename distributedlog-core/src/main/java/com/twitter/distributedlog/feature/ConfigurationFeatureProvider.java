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

import com.twitter.distributedlog.config.ConcurrentBaseConfiguration;
import org.apache.bookkeeper.feature.CacheableFeatureProvider;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * Feature Provider that load features from configuration
 */
class ConfigurationFeatureProvider extends CacheableFeatureProvider {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationFeatureProvider.class);

    static SettableFeature makeFeature(ConcurrentBaseConfiguration featuresConf,
                                       ConcurrentMap<String, SettableFeature> features,
                                       String featureName) {
        SettableFeature feature = features.get(featureName);
        if (null == feature) {
            int availability = featuresConf.getInt(featureName, 0);
            feature = new SettableFeature(featureName, availability);
            SettableFeature oldFeature =
                    features.putIfAbsent(featureName, feature);
            if (null != oldFeature) {
                feature = oldFeature;
            } else {
                logger.info("Load feature {}={}", featureName, availability);
            }
        }
        return feature;
    }

    private final ConcurrentBaseConfiguration featuresConf;
    private final ConcurrentMap<String, SettableFeature> features;

    ConfigurationFeatureProvider(String rootScope,
                                 ConcurrentBaseConfiguration featuresConf,
                                 ConcurrentMap<String, SettableFeature> features) {
        super(rootScope);
        this.featuresConf = featuresConf;
        this.features = features;
    }

    @Override
    protected Feature makeFeature(String featureName) {
        return makeFeature(featuresConf, features, featureName);
    }

    @Override
    protected FeatureProvider makeProvider(String fullScopeName) {
        return new ConfigurationFeatureProvider(
                fullScopeName, featuresConf, features);
    }
}
