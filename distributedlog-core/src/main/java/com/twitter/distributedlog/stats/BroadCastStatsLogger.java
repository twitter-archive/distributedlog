package com.twitter.distributedlog.stats;

import com.google.common.base.Preconditions;

import org.apache.bookkeeper.stats.CachingStatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

public class BroadCastStatsLogger {
    public static class Two implements StatsLogger {
        private final StatsLogger first;
        private final StatsLogger second;

        public Two(StatsLogger first, StatsLogger second) {
            super();
            Preconditions.checkNotNull(first);
            Preconditions.checkNotNull(second);
            this.first = first;
            this.second = second;
        }

        @Override
        public OpStatsLogger getOpStatsLogger(final String statName) {
            final OpStatsLogger firstLogger = first.getOpStatsLogger(statName);
            final OpStatsLogger secondLogger = second.getOpStatsLogger(statName);
            return new OpStatsLogger() {
                @Override
                public void registerFailedEvent(long l) {
                    firstLogger.registerFailedEvent(l);
                    secondLogger.registerFailedEvent(l);
                }

                @Override
                public void registerSuccessfulEvent(long l) {
                    firstLogger.registerSuccessfulEvent(l);
                    secondLogger.registerSuccessfulEvent(l);
                }

                @Override
                public OpStatsData toOpStatsData() {
                    // Eventually consistent.
                    return firstLogger.toOpStatsData();
                }

                @Override
                public void clear() {
                    firstLogger.clear();
                    secondLogger.clear();
                }
            };
        }

        @Override
        public Counter getCounter(final String statName) {
            final Counter firstCounter = first.getCounter(statName);
            final Counter secondCounter = second.getCounter(statName);
            return new Counter() {
                @Override
                public void clear() {
                    firstCounter.clear();
                    secondCounter.clear();
                }

                @Override
                public void inc() {
                    firstCounter.inc();
                    secondCounter.inc();
                }

                @Override
                public void dec() {
                    firstCounter.dec();
                    secondCounter.dec();
                }

                @Override
                public void add(long l) {
                    firstCounter.add(l);
                    secondCounter.add(l);
                }

                @Override
                public Long get() {
                    // Eventually consistent.
                    return firstCounter.get();
                }
            };
        }

        @Override
        public <T extends Number> void registerGauge(String statName, Gauge<T> gauge) {
            // Different underlying stats loggers have different semantics wrt. gauge registration.
            throw new RuntimeException("Cannot register a gauge on BroadCastStatsLogger.Two");
        }

        @Override
        public StatsLogger scope(final String statName) {
            return new CachingStatsLogger(new Two(first.scope(statName), second.scope(statName)));
        }
    }
}
