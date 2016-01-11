package com.twitter.distributedlog.feature;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.decider.Decider;
import com.twitter.decider.DeciderFactory;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.util.Duration;

import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.concurrent.TimeUnit;

/**
 * Decider based feature provider
 */
public class DeciderFeatureProvider extends AbstractFeatureProvider<DeciderFeature> {

    private static final Logger logger = LoggerFactory.getLogger(DeciderFeatureProvider.class);

    private final Decider decider;

    private static Decider initializeDecider(DistributedLogConfiguration conf) {
        String baseConfigPath = DeciderUtils.getDeciderBaseConfigPath(conf);
        String overlayConfigPath = DeciderUtils.getDeciderOverlayConfigPath(conf);

        logger.info("Initializing decider : base_config_path = {}, overlay_config_path = {}",
                baseConfigPath, overlayConfigPath);

        return new DeciderFactory(
                Option.apply(baseConfigPath),
                Option.apply(overlayConfigPath),
                Option.apply((String) null),
                Duration.apply(1, TimeUnit.MINUTES),
                DeciderFactory.DefaultDecisionMakers(),
                false).apply();
    }

    @VisibleForTesting
    DeciderFeatureProvider(String rootScope,
                           DistributedLogConfiguration conf) {
        this(rootScope, conf, initializeDecider(conf), NullStatsLogger.INSTANCE);
    }

    public DeciderFeatureProvider(String rootScope,
                                  DistributedLogConfiguration conf,
                                  StatsLogger statsLogger) {
        this(rootScope, conf, initializeDecider(conf), statsLogger);
    }

    @VisibleForTesting
    DeciderFeatureProvider(String rootScope,
                           DistributedLogConfiguration conf,
                           Decider decider) {
        this(rootScope, conf, decider, NullStatsLogger.INSTANCE);
    }

    DeciderFeatureProvider(String rootScope,
                           DistributedLogConfiguration conf,
                           Decider decider,
                           StatsLogger statsLogger) {
        super(rootScope, conf, statsLogger);
        this.decider = decider;
        logger.info("Loaded features : {}", decider.features());
    }

    @Override
    public DeciderFeature getFeature(String name) {
        DeciderFeature feature = this.features.get(name);
        if(null == feature) {
            DeciderFeature newFeature = this.makeFeature(this.makeName(name));
            DeciderFeature oldFeature = this.features.putIfAbsent(name, newFeature);
            if(null == oldFeature) {
                newFeature.init();
                feature = newFeature;
            } else {
                feature = oldFeature;
            }
        }

        return feature;
    }

    @Override
    protected DeciderFeature makeFeature(String featureName) {
        return new DeciderFeature(featureName, decider, statsLogger);
    }

    @Override
    protected FeatureProvider makeProvider(String fullScopeName) {
        return new DeciderFeatureProvider(fullScopeName, conf, decider, statsLogger);
    }
}
