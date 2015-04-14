package com.twitter.distributedlog.util;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.FuturePool;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import scala.Function0;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Ordered Scheduler
 */
public class OrderedScheduler implements ScheduledExecutorService {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String name = "OrderedScheduler";
        private int corePoolSize = -1;
        private ThreadFactory threadFactory = null;
        private boolean traceTaskExecution = false;
        private long traceTaskExecutionWarnTimeUs = Long.MAX_VALUE;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder corePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
            return this;
        }

        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public Builder traceTaskExecution(boolean trace) {
            this.traceTaskExecution = trace;
            return this;
        }

        public Builder traceTaskExecutionWarnTimeUs(long timeUs) {
            this.traceTaskExecutionWarnTimeUs = timeUs;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public OrderedScheduler build() {
            if (corePoolSize <= 0) {
                corePoolSize = Runtime.getRuntime().availableProcessors();
            }
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }

            return new OrderedScheduler(name, corePoolSize, threadFactory,
                    traceTaskExecution, traceTaskExecutionWarnTimeUs, statsLogger);
        }

    }

    protected final String name;
    protected final int corePoolSize;
    protected final MonitoredScheduledThreadPoolExecutor[] executors;
    protected final MonitoredFuturePool[] futurePools;
    protected final Random random;

    private OrderedScheduler(String name,
                             int corePoolSize,
                             ThreadFactory threadFactory,
                             boolean traceTaskExecution,
                             long traceTaskExecutionWarnTimeUs,
                             StatsLogger statsLogger) {
        this.name = name;
        this.corePoolSize = corePoolSize;
        this.executors = new MonitoredScheduledThreadPoolExecutor[corePoolSize];
        this.futurePools = new MonitoredFuturePool[corePoolSize];
        for (int i = 0; i < corePoolSize; i++) {
            ThreadFactory tf = new ThreadFactoryBuilder()
                    .setNameFormat(name + "-executor-" + i + "-%d")
                    .setThreadFactory(threadFactory)
                    .build();
            executors[i] = new MonitoredScheduledThreadPoolExecutor(
                    1, tf, statsLogger.scope("executor-" + i), traceTaskExecution);
            futurePools[i] = new MonitoredFuturePool(
                    new ExecutorServiceFuturePool(executors[i]), statsLogger,
                    traceTaskExecution,
                    traceTaskExecutionWarnTimeUs);
        }
        this.random = new Random(System.currentTimeMillis());
    }

    protected MonitoredScheduledThreadPoolExecutor chooseExecutor() {
        return corePoolSize == 1 ? executors[0] : executors[random.nextInt(corePoolSize)];
    }

    protected MonitoredScheduledThreadPoolExecutor chooseExecutor(Object key) {
        return corePoolSize == 1 ? executors[0] :
                executors[MathUtils.signSafeMod(Objects.hashCode(key), corePoolSize)];
    }

    protected FuturePool chooseFuturePool(Object key) {
        return corePoolSize == 1 ? futurePools[0] :
                futurePools[MathUtils.signSafeMod(Objects.hashCode(key), corePoolSize)];
    }

    protected FuturePool chooseFuturePool() {
        return corePoolSize == 1 ? futurePools[0] : futurePools[random.nextInt(corePoolSize)];
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return chooseExecutor().schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return chooseExecutor().schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay, long period, TimeUnit unit) {
        return chooseExecutor().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                     long initialDelay, long delay, TimeUnit unit) {
        return chooseExecutor().scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            executor.shutdown();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            runnables.addAll(executor.shutdownNow());
        }
        return runnables;
    }

    @Override
    public boolean isShutdown() {
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            if (!executor.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTerminated() {
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            if (!executor.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            if (!executor.awaitTermination(timeout, unit)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return chooseExecutor().submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return chooseExecutor().submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return chooseExecutor().submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return chooseExecutor().invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return chooseExecutor().invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        return chooseExecutor().invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return chooseExecutor().invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        chooseExecutor().execute(command);
    }

    // Ordered Functions

    public <T> com.twitter.util.Future<T> apply(Object key, Function0<T> function) {
        return chooseFuturePool(key).apply(function);
    }

    public <T> com.twitter.util.Future<T> apply(Function0<T> function) {
        return chooseFuturePool().apply(function);
    }
}
