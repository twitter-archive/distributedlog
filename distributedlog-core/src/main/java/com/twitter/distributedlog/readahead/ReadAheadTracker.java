package com.twitter.distributedlog.readahead;

import com.twitter.distributedlog.ReadAheadCache;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * ReadAheadTracker is tracking the progress of readahead worker. so we could use it to investigate where
 * the readahead worker is.
 */
public class ReadAheadTracker {
    // ticks is used to differentiate that the worker enter same phase in different time.
    final AtomicLong ticks = new AtomicLong(0);
    // which phase that the worker is in.
    ReadAheadPhase phase;

    ReadAheadTracker(String streamName,
                     final ReadAheadCache cache,
                     ReadAheadPhase initialPhase,
                     StatsLogger statsLogger) {
        this.phase = initialPhase;
        statsLogger.registerGauge("phase", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return ReadAheadPhase.SCHEDULE_READAHEAD.getCode();
            }

            @Override
            public Number getSample() {
                return phase.getCode();
            }
        });
        statsLogger.registerGauge("ticks", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return ticks.get();
            }
        });
        statsLogger.registerGauge("cache_entries", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return cache.getNumCachedRecords();
            }
        });
    }

    ReadAheadPhase getPhase() {
        return this.phase;
    }

    public void enterPhase(ReadAheadPhase readAheadPhase) {
        this.ticks.incrementAndGet();
        this.phase = readAheadPhase;
    }
}
