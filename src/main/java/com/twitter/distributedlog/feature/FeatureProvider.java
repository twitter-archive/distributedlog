package com.twitter.distributedlog.feature;

/**
 * Provider to provide features.
 */
public interface FeatureProvider {
    /**
     * Return the feature with given name.
     *
     * @param name feature name
     * @return feature instance
     */
    Feature getFeature(String name);
}
