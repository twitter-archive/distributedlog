/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.stats;

import com.google.common.base.Preconditions;

import org.apache.bookkeeper.stats.CachingStatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Stats Loggers that broadcast stats to multiple {@link StatsLogger}.
 */
public class BroadCastStatsLogger {

    /**
     * Create a broadcast stats logger of two stats loggers `<code>first</code>` and
     * `<code>second</code>`. The returned stats logger doesn't allow registering any
     * {@link Gauge}.
     *
     * @param first
     *          first stats logger
     * @param second
     *          second stats logger
     * @return broadcast stats logger
     */
    public static StatsLogger two(StatsLogger first, StatsLogger second) {
        return new CachingStatsLogger(new Two(first, second));
    }

    static class Two implements StatsLogger {
        protected final StatsLogger first;
        protected final StatsLogger second;

        private Two(StatsLogger first, StatsLogger second) {
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
        public StatsLogger scope(final String scope) {
            return new Two(first.scope(scope), second.scope(scope));
        }
    }

    /**
     * Create a broadcast stats logger of two stats loggers <code>master</code> and <code>slave</code>.
     * It is similar as {@link #two(StatsLogger, StatsLogger)}, but it allows registering {@link Gauge}s.
     * The {@link Gauge} will be registered under master.
     *
     * @param master
     *          master stats logger to receive {@link Counter}, {@link OpStatsLogger} and {@link Gauge}.
     * @param slave
     *          slave stats logger to receive only {@link Counter} and {@link OpStatsLogger}.
     * @return broadcast stats logger
     */
    public static StatsLogger masterslave(StatsLogger master, StatsLogger slave) {
        return new CachingStatsLogger(new MasterSlave(master, slave));
    }

    static class MasterSlave extends Two {

        private MasterSlave(StatsLogger master, StatsLogger slave) {
            super(master, slave);
        }

        @Override
        public <T extends Number> void registerGauge(String statName, Gauge<T> gauge) {
            first.registerGauge(statName, gauge);
        }

        @Override
        public StatsLogger scope(String scope) {
            return new MasterSlave(first.scope(scope), second.scope(scope));
        }
    }
}
