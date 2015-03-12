package com.twitter.distributedlog.feature;

import com.twitter.decider.Decider;
import org.apache.bookkeeper.feature.Feature;
import scala.Int;
import scala.Option;

/**
 * Decider based feature implementation.
 */
class DeciderFeature implements Feature {
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
