package com.twitter.distributedlog.feature;

import com.twitter.decider.Decider;
import com.twitter.decider.MockDecider;
import com.twitter.decider.NullDecider;
import com.twitter.distributedlog.DistributedLogConfiguration;
import org.junit.Test;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import scala.collection.Set$;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test Case for {@link com.twitter.distributedlog.feature.DeciderFeatureProvider}
 */
public class TestDeciderFeatureProvider {

    private final DistributedLogConfiguration conf = new DistributedLogConfiguration();

    @Test(timeout = 60000)
    public void testNullDecider() {
        Decider decider = new NullDecider(false);
        DeciderFeatureProvider provider = new DeciderFeatureProvider(conf, decider);
        Feature feature = provider.getFeature("null-feature");
        assertFalse(feature.isAvailable());
        assertEquals(0, feature.availability());
    }

    @Test(timeout = 60000)
    public void testAvailabilities() {
        Set<String> jFeatures = new HashSet<String>();
        jFeatures.add("feature-1");
        scala.collection.Set<String> features =
                scala.collection.JavaConversions.asScalaSet(jFeatures);
        scala.collection.Set<String> clientFeatures = Set$.MODULE$.empty();
        Decider decider = new MockDecider(features, clientFeatures);
        DeciderFeatureProvider provider = new DeciderFeatureProvider(conf, decider);
        Feature feature = provider.getFeature("feature-1");
        assertTrue(feature.isAvailable());
        assertTrue(feature.availability() > 0);
        Feature nullFeature = provider.getFeature("null-feature");
        assertFalse(nullFeature.isAvailable());
        assertEquals(0, nullFeature.availability());
    }

    @Test(timeout = 60000)
    public void testLoadDeciderFromBase() {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.setDeciderBaseConfigPath("test_decider.yml");
        DeciderFeatureProvider provider = new DeciderFeatureProvider(confLocal);
        Feature feature1 = provider.getFeature("feature_1");
        assertTrue(feature1.isAvailable());
        assertEquals(10000, feature1.availability());
        Feature feature2 = provider.getFeature("feature_2");
        assertTrue(feature2.isAvailable());
        assertEquals(5000, feature2.availability());
        Feature feature3 = provider.getFeature("feature_3");
        assertFalse(feature3.isAvailable());
        assertEquals(0, feature3.availability());
        Feature feature4 = provider.getFeature("unknown_feature");
        assertFalse(feature4.isAvailable());
        assertEquals(0, feature4.availability());
    }

    @Test(timeout = 60000)
    public void testLoadDeciderFromOverlay() {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.setDeciderBaseConfigPath("test_decider.yml");
        confLocal.setDeciderOverlayConfigPath("test_overlay.yml");
        DeciderFeatureProvider provider = new DeciderFeatureProvider(confLocal);
        Feature feature1 = provider.getFeature("feature_1");
        assertTrue(feature1.isAvailable());
        assertEquals(10000, feature1.availability());
        Feature feature2 = provider.getFeature("feature_2");
        assertTrue(feature2.isAvailable());
        assertEquals(6000, feature2.availability());
        Feature feature3 = provider.getFeature("feature_3");
        assertFalse(feature3.isAvailable());
        assertEquals(0, feature3.availability());
        Feature feature4 = provider.getFeature("feature_4");
        assertFalse(feature4.isAvailable());
        assertEquals(0, feature4.availability());
        Feature feature5 = provider.getFeature("unknown_feature");
        assertFalse(feature5.isAvailable());
        assertEquals(0, feature5.availability());
    }
}
