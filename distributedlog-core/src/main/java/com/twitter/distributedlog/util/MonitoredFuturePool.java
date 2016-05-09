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

import com.google.common.base.Stopwatch;

import com.twitter.util.FuturePool;
import com.twitter.util.FuturePool$;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import scala.runtime.BoxedUnit;
import scala.Function0;

/**
 * {@link FuturePool} with exposed stats. This class is exposing following stats for helping understanding
 * the healthy of this thread pool executor.
 * <h3>Metrics</h3>
 * Stats are only exposed when <code>traceTaskExecution</code> is true.
 * <ul>
 * <li>task_pending_time: opstats. measuring the characteristics about the time that tasks spent on waiting
 * being executed.
 * <li>task_execution_time: opstats. measuring the characteristics about the time that tasks spent on executing.
 * <li>task_enqueue_time: opstats. measuring the characteristics about the time that tasks spent on submitting.
 * <li>tasks_pending: gauge. how many tasks are pending in this future pool.
 * </ul>
 */
public class MonitoredFuturePool implements FuturePool {
    static final Logger LOG = LoggerFactory.getLogger(MonitoredFuturePool.class);

    private final FuturePool futurePool;

    private final StatsLogger statsLogger;
    private final OpStatsLogger taskPendingTime;
    private final OpStatsLogger taskExecutionTime;
    private final OpStatsLogger taskEnqueueTime;
    private final Counter taskPendingCounter;

    private final boolean traceTaskExecution;
    private final long traceTaskExecutionWarnTimeUs;

    class TimedFunction0<T> extends com.twitter.util.Function0<T> {
        private final Function0<T> function0;
        private Stopwatch pendingStopwatch = Stopwatch.createStarted();

        TimedFunction0(Function0<T> function0) {
            this.function0 = function0;
            this.pendingStopwatch = Stopwatch.createStarted();
        }

        @Override
        public T apply() {
            taskPendingTime.registerSuccessfulEvent(pendingStopwatch.elapsed(TimeUnit.MICROSECONDS));
            Stopwatch executionStopwatch = Stopwatch.createStarted();
            T result = function0.apply();
            taskExecutionTime.registerSuccessfulEvent(executionStopwatch.elapsed(TimeUnit.MICROSECONDS));
            long elapsed = executionStopwatch.elapsed(TimeUnit.MICROSECONDS);
            if (elapsed > traceTaskExecutionWarnTimeUs) {
                LOG.info("{} took too long {} microseconds", function0.toString(), elapsed);
            }
            return result;
        }
    }

    /**
     * Create a future pool with stats exposed.
     *
     * @param futurePool underlying future pool to execute futures
     * @param statsLogger stats logger to receive exposed stats
     * @param traceTaskExecution flag to enable/disable exposing stats about task execution
     * @param traceTaskExecutionWarnTimeUs flag to enable/disable logging slow tasks
     *                                     whose execution time is above this value
     */
    public MonitoredFuturePool(FuturePool futurePool,
                               StatsLogger statsLogger,
                               boolean traceTaskExecution,
                               long traceTaskExecutionWarnTimeUs) {
        this.futurePool = futurePool;
        this.traceTaskExecution = traceTaskExecution;
        this.traceTaskExecutionWarnTimeUs = traceTaskExecutionWarnTimeUs;
        this.statsLogger = statsLogger;
        this.taskPendingTime = statsLogger.getOpStatsLogger("task_pending_time");
        this.taskExecutionTime = statsLogger.getOpStatsLogger("task_execution_time");
        this.taskEnqueueTime = statsLogger.getOpStatsLogger("task_enqueue_time");
        this.taskPendingCounter = statsLogger.getCounter("tasks_pending");
    }

    @Override
    public <T> Future<T> apply(Function0<T> function0) {
        if (traceTaskExecution) {
            taskPendingCounter.inc();
            Stopwatch taskEnqueueStopwatch = Stopwatch.createStarted();
            Future<T> futureResult = futurePool.apply(new TimedFunction0<T>(function0));
            taskEnqueueTime.registerSuccessfulEvent(taskEnqueueStopwatch.elapsed(TimeUnit.MICROSECONDS));
            futureResult.ensure(new com.twitter.util.Function0<BoxedUnit>() {
                @Override
                public BoxedUnit apply() {
                    taskPendingCounter.dec();
                    return null;
                }
            });
            return futureResult;
        } else {
            return futurePool.apply(function0);
        }
    }
}
