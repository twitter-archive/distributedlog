package com.twitter.distributedlog.feature;

import com.twitter.distributedlog.DistributedLogConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Decider based feature provider
 *
 * TODO: make it loaded by reflection?
 */
public abstract class AbstractFeatureProvider implements FeatureProvider {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractFeatureProvider.class);

    public static FeatureProvider getFeatureProvider(DistributedLogConfiguration conf) throws IOException {
        Class<? extends FeatureProvider> featureProviderClass;
        try {
            featureProviderClass = conf.getFeatureProviderClass();
        } catch (ConfigurationException e) {
            throw new IOException("Can't initialize the feature provider : ", e);
        }
        // create feature provider
        Constructor<? extends FeatureProvider> constructor;
        try {
            constructor = featureProviderClass.getDeclaredConstructor(DistributedLogConfiguration.class);
        } catch (NoSuchMethodException e) {
            throw new IOException("No constructor found for feature provider class " + featureProviderClass + " : ", e);
        }
        try {
            return constructor.newInstance(conf);
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
    protected final ConcurrentMap<String, Feature> features =
            new ConcurrentHashMap<String, Feature>();

    protected AbstractFeatureProvider(DistributedLogConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public Feature getFeature(String name) {
        Feature feature = features.get(name);
        if (feature == null) {
            Feature newFeature = makeFeature(name);
            Feature oldFeature = features.putIfAbsent(name, newFeature);
            if (oldFeature != null) {
                feature = oldFeature;
            } else {
                feature = newFeature;
            }
        }
        return feature;
    }

    protected abstract Feature makeFeature(String name);
}
