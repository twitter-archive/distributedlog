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
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Default feature provider which disable all features by default.
 */
public class DefaultFeatureProvider extends AbstractFeatureProvider {

    public DefaultFeatureProvider(String rootScope,
                                  DistributedLogConfiguration conf,
                                  StatsLogger statsLogger) {
        super(rootScope, conf, statsLogger);
    }

    @Override
    protected Feature makeFeature(String featureName) {
        return new SettableFeature(featureName, 0);
    }

    @Override
    protected FeatureProvider makeProvider(String fullScopeName) {
        return new SettableFeatureProvider(fullScopeName, 0);
    }
}
