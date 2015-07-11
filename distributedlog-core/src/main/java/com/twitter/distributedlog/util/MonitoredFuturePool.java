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
            Future<T> futureResult = futurePool.apply(new TimedFunction0(function0));
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
