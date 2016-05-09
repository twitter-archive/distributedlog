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
