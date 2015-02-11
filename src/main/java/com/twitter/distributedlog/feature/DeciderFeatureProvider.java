package com.twitter.distributedlog.feature;

import com.twitter.decider.Decider;
import com.twitter.decider.DeciderFactory;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import scala.Option;

import java.util.concurrent.TimeUnit;

/**
 * Decider based feature provider
 */
public class DeciderFeatureProvider extends AbstractFeatureProvider {

    private static final Logger logger = LoggerFactory.getLogger(DeciderFeatureProvider.class);

    private static class DeciderFeature implements Feature {

        private final String name;
        private final Decider decider;

        DeciderFeature(String name, Decider decider) {
            this.name = name;
            this.decider = decider;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public int availability() {
            Option<Object> availability = decider.availability(name);
            if (availability.isDefined()) {
                return Int.unbox(availability.get());
            } else {
                return 0;
            }
        }

        @Override
        public boolean isAvailable() {
            return availability() > 0;
        }
    }

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

    public DeciderFeatureProvider(DistributedLogConfiguration conf) {
        this(conf, initializeDecider(conf));
    }

    DeciderFeatureProvider(DistributedLogConfiguration conf, Decider decider) {
        super(conf);
        this.decider = decider;
        logger.info("Loaded features : {}", decider.features());
    }

    @Override
    protected Feature makeFeature(String name) {
        return new DeciderFeature(name, decider);
    }
}
