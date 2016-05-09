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
