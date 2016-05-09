package com.twitter.distributedlog.feature;

import com.twitter.distributedlog.config.ConcurrentBaseConfiguration;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.SettableFeature;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;

/**
 * Test case for configuration based feature provider
 */
public class TestConfigurationFeatureProvider {

    @Test(timeout = 60000)
    public void testConfigurationFeatureProvider() throws Exception {
        String rootScope = "dl";
        ConcurrentBaseConfiguration featureConf =
                new ConcurrentBaseConfiguration();
        ConcurrentMap<String, SettableFeature> features =
                new ConcurrentHashMap<String, SettableFeature>();
        ConfigurationFeatureProvider featureProvider =
                new ConfigurationFeatureProvider(rootScope, featureConf, features);

        String featureName1 = "feature1";
        String fullFeatureName1 = rootScope + "." + featureName1;
        int availability1 = 1234;
        featureConf.setProperty(fullFeatureName1, availability1);
        Feature feature1 = featureProvider.getFeature(featureName1);
        assertEquals(availability1, feature1.availability());
        assertTrue(features.containsKey(fullFeatureName1));
        assertTrue(feature1 == features.get(fullFeatureName1));

        String subScope = "subscope";
        String featureName2 = "feature2";
        String fullFeatureName2 = rootScope + "." + subScope + "." + featureName2;
        int availability2 = 4321;
        featureConf.setProperty(fullFeatureName2, availability2);
        Feature feature2 = featureProvider.scope(subScope).getFeature(featureName2);
        assertEquals(availability2, feature2.availability());
        assertTrue(features.containsKey(fullFeatureName2));
        assertTrue(feature2 == features.get(fullFeatureName2));
    }

}
