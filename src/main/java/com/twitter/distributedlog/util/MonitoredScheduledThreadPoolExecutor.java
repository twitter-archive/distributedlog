package com.twitter.distributedlog.util;

import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class MonitoredScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
    static final Logger LOG = LoggerFactory.getLogger(MonitoredScheduledThreadPoolExecutor.class);

    private class TimedRunnable implements Runnable {

        final Runnable runnable;
        final long enqueueNanos;

        TimedRunnable(Runnable runnable) {
            this.runnable = runnable;
            this.enqueueNanos = MathUtils.nowInNano();
        }

        @Override
        public void run() {
            long startNanos = MathUtils.nowInNano();
            long pendingMicros = TimeUnit.NANOSECONDS.toMicros(startNanos - enqueueNanos);
            taskPendingStats.registerSuccessfulEvent(pendingMicros);
            try {
                runnable.run();
            } finally {
                long executionMicros = TimeUnit.NANOSECONDS.toMicros(MathUtils.nowInNano() - startNanos);
                taskExecutionStats.registerSuccessfulEvent(executionMicros);
            }
        }
    }

    private class TimedCallable<T> implements Callable<T> {

        final Callable<T> task;
        final long enqueueNanos;

        TimedCallable(Callable<T> task) {
            this.task = task;
            this.enqueueNanos = MathUtils.nowInNano();
        }

        @Override
        public T call() throws Exception {
            long startNanos = MathUtils.nowInNano();
            long pendingMicros = TimeUnit.NANOSECONDS.toMicros(startNanos - enqueueNanos);
            taskPendingStats.registerSuccessfulEvent(pendingMicros);
            try {
                return task.call();
            } finally {
                long executionMicros = TimeUnit.NANOSECONDS.toMicros(MathUtils.nowInNano() - startNanos);
                taskExecutionStats.registerSuccessfulEvent(executionMicros);
            }
        }
    }

    protected final StatsLogger statsLogger;
    protected final boolean traceTaskExecution;
    protected final OpStatsLogger taskExecutionStats;
    protected final OpStatsLogger taskPendingStats;

    public MonitoredScheduledThreadPoolExecutor(int corePoolSize,
                                                ThreadFactory threadFactory,
                                                StatsLogger statsLogger,
                                                boolean traceTaskExecution) {
        super(corePoolSize, threadFactory);
        this.statsLogger = statsLogger;
        this.traceTaskExecution = traceTaskExecution;

        this.taskPendingStats = statsLogger.getOpStatsLogger("task_pending_time");
        this.taskExecutionStats = statsLogger.getOpStatsLogger("task_execution_time");
        // outstanding tasks
        statsLogger.registerGauge("pending_tasks", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return getQueue().size();
            }
        });
        // completed tasks
        statsLogger.registerGauge("completed_tasks", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return getCompletedTaskCount();
            }
        });
        // total tasks
        statsLogger.registerGauge("total_tasks", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return getTaskCount();
            }
        });
    }

    private Runnable timedRunnable(Runnable r) {
        return traceTaskExecution ? new TimedRunnable(r) : r;
    }

    private <T> Callable<T> timedCallable(Callable<T> task) {
        return traceTaskExecution ? new TimedCallable<T>(task) : task;
    }

    @Override
    public Future<?> submit(Runnable task) {
        return super.submit(timedRunnable(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return super.submit(timedRunnable(task), result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return super.submit(timedCallable(task));
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        Throwable hiddenThrowable = extractThrowable(r);
        if (hiddenThrowable != null)
            logAndHandle(hiddenThrowable, true);

        // The executor re-throws exceptions thrown by the task to the uncaught exception handler
        // so we don't need to pass the exception to the handler explicitly
        if (null != t) {
            logAndHandle(t, false);
        }
    }

    /**
     * The executor re-throws exceptions thrown by the task to the uncaught exception handler
     * so we only need to do anything if uncaught exception handler has not been se
     */
    private void logAndHandle(Throwable t, boolean passToHandler) {
        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            LOG.error("Unhandled exception on thread {}", Thread.currentThread().getName(), t);
        }
        else {
            LOG.info("Unhandled exception on thread {}", Thread.currentThread().getName(), t);
            if (passToHandler) {
                Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
            }
        }
    }


    /**
     * Extract the exception (throwable) inside the ScheduledFutureTask
     * @param runnable - The runable that was executed
     * @return exception enclosed in the Runnable if any; null otherwise
     */
    private Throwable extractThrowable(Runnable runnable) {
        // Check for exceptions wrapped by FutureTask.
        // We do this by calling get(), which will cause it to throw any saved exception.
        // Check for isDone to prevent blocking
        if ((runnable instanceof Future<?>) && ((Future<?>) runnable).isDone()) {
            try {
                ((Future<?>) runnable).get();
            } catch (CancellationException e) {
                LOG.info("Task cancelled", e);
            } catch (InterruptedException e) {
                LOG.info("Task was interrupted", e);
            } catch (ExecutionException e) {
                return e.getCause();
            }
        }

        return null;
    }



}
