package com.twitter.distributedlog.feature;

import com.twitter.decider.Decider;
import com.twitter.decider.DeciderFactory;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.util.Duration;

import org.apache.bookkeeper.feature.FeatureProvider;
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
        String baseConfigPath = conf.getDeciderBaseConfigPath();
        String overlayConfigPath = conf.getDeciderOverlayConfigPath();

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

    public DeciderFeatureProvider(String rootScope,
                                  DistributedLogConfiguration conf) {
        this(rootScope, conf, initializeDecider(conf));
    }

    DeciderFeatureProvider(String rootScope,
                           DistributedLogConfiguration conf,
                           Decider decider) {
        super(rootScope, conf);
        this.decider = decider;
        logger.info("Loaded features : {}", decider.features());
    }

    @Override
    protected DeciderFeature makeFeature(String name) {
        return new DeciderFeature(makeName(name), decider);
    }

    @Override
    protected FeatureProvider makeProvider(String scope) {
        return new DeciderFeatureProvider(makeName(scope), conf, decider);
    }
}
