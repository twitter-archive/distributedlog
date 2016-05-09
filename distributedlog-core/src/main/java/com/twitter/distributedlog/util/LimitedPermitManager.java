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
package com.twitter.distributedlog.util;

import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manager to control all the log segments rolling.
 */
public class LimitedPermitManager implements PermitManager, Runnable, Watcher {

    static final Logger LOG = LoggerFactory.getLogger(LimitedPermitManager.class);

    static enum PermitState {
        ALLOWED, DISALLOWED, DISABLED
    }

    class EpochPermit implements Permit {

        final PermitState state;
        final int epoch;

        EpochPermit(PermitState state) {
            this.state = state;
            this.epoch = LimitedPermitManager.this.epoch.get();
        }

        int getEpoch() {
            return epoch;
        }

        @Override
        public boolean isAllowed() {
            return PermitState.ALLOWED == state;
        }
    }

    boolean enablePermits = true;
    final Semaphore semaphore;
    final int period;
    final TimeUnit timeUnit;
    final ScheduledExecutorService executorService;
    final AtomicInteger epoch = new AtomicInteger(0);

    public LimitedPermitManager(int concurrency, int period, TimeUnit timeUnit,
                                ScheduledExecutorService executorService) {
        this(concurrency, period, timeUnit, executorService, NullStatsLogger.INSTANCE);
    }

    public LimitedPermitManager(final int concurrency, int period, TimeUnit timeUnit,
            ScheduledExecutorService executorService, StatsLogger statsLogger) {
        if (concurrency > 0) {
            this.semaphore = new Semaphore(concurrency);
        } else {
            this.semaphore = null;
        }
        this.period = period;
        this.timeUnit = timeUnit;
        this.executorService = executorService;
        statsLogger.scope("permits").registerGauge("outstanding", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return null == semaphore ? 0 : concurrency - semaphore.availablePermits();
            }
        });
    }

    @Override
    synchronized public Permit acquirePermit() {
        if (!enablePermits) {
            return new EpochPermit(PermitState.DISABLED);
        }
        if (null != semaphore) {
            return semaphore.tryAcquire() ? new EpochPermit(PermitState.ALLOWED) :
                    new EpochPermit(PermitState.DISALLOWED);
        } else {
            return new EpochPermit(PermitState.ALLOWED);
        }
    }

    @Override
    synchronized public void releasePermit(Permit permit) {
        if (null != semaphore && permit.isAllowed()) {
            if (period <= 0) {
                semaphore.release();
            } else {
                try {
                    executorService.schedule(this, period, timeUnit);
                } catch (RejectedExecutionException ree) {
                    LOG.warn("Failed on scheduling releasing permit in given period ({}ms)." +
                            " Release it immediately : ", timeUnit.toMillis(period), ree);
                    semaphore.release();
                }
            }
        }
    }

    @Override
    synchronized public boolean disallowObtainPermits(Permit permit) {
        if (!(permit instanceof EpochPermit)) {
            return false;
        }
        if (epoch.getAndIncrement() == ((EpochPermit)permit).getEpoch()) {
            this.enablePermits = false;
            LOG.info("EnablePermits = {}, Epoch = {}.", this.enablePermits, epoch.get());
            return true;
        } else {
            return false;
        }
    }

    @Override
    synchronized public boolean allowObtainPermits() {
        forceSetAllowPermits(true);
        return true;
    }

    synchronized void forceSetAllowPermits(boolean allowPermits) {
        epoch.getAndIncrement();
        this.enablePermits = allowPermits;
        LOG.info("EnablePermits = {}, Epoch = {}.", this.enablePermits, epoch.get());
    }

    @Override
    public void run() {
        semaphore.release();
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(Event.EventType.None)) {
            switch (event.getState()) {
            case SyncConnected:
                forceSetAllowPermits(true);
                break;
            case Disconnected:
                forceSetAllowPermits(false);
                break;
            case Expired:
                forceSetAllowPermits(false);
                break;
            default:
                break;
            }
        }
    }
}
