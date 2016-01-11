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
