package com.twitter.distributedlog.feature;

import com.twitter.decider.Decider;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.Int;
import scala.Option;

/**
 * Decider based feature implementation.
 */
class DeciderFeature implements Feature, Gauge<Number> {
    private final String name;
    private final Decider decider;
    private final StatsLogger statsLogger;

    DeciderFeature(String name,
                   Decider decider,
                   StatsLogger statsLogger) {
        this.name = name;
        this.decider = decider;
        this.statsLogger = statsLogger;
    }

    DeciderFeature init() {
        this.statsLogger.registerGauge(name, this);
        return this;
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

    @Override
    public Number getDefaultValue() {
        return 0;
    }

    @Override
    public Number getSample() {
        return availability();
    }
}
