package com.twitter.distributedlog.util;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
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
 * Ordered Scheduler. It is thread pool based {@link ScheduledExecutorService}, additionally providing
 * the ability to execute/schedule tasks by <code>key</code>. Hence the tasks submitted by same <i>key</i>
 * will be executed in order.
 * <p>
 * The scheduler is comprised of multiple {@link MonitoredScheduledThreadPoolExecutor}s. Each
 * {@link MonitoredScheduledThreadPoolExecutor} is a single thread executor. Normal task submissions will
 * be submitted to executors in a random manner to guarantee load balancing. Keyed task submissions (e.g
 * {@link OrderedScheduler#apply(Object, Function0)} will be submitted to a dedicated executor based on
 * the hash value of submit <i>key</i>.
 *
 * <h3>Metrics</h3>
 *
 * <h4>Per Executor Metrics</h4>
 *
 * Metrics about individual executors are exposed via {@link Builder#perExecutorStatsLogger}
 * under <i>`scope`/`name`-executor-`id`-0</i>. `name` is the scheduler name provided by {@link Builder#name}
 * while `id` is the index of this executor in the pool. And corresponding stats of future pool of
 * that executor are exposed under <i>`scope`/`name`-executor-`id`-0/futurepool</i>.
 * <p>
 * See {@link MonitoredScheduledThreadPoolExecutor} and {@link MonitoredFuturePool} for per executor metrics
 * exposed.
 *
 * <h4>Aggregated Metrics</h4>
 * <ul>
 * <li>task_pending_time: opstats. measuring the characteristics about the time that tasks spent on
 * waiting being executed.
 * <li>task_execution_time: opstats. measuring the characteristics about the time that tasks spent on
 * executing.
 * <li>futurepool/task_pending_time: opstats. measuring the characteristics about the time that tasks spent
 * on waiting in future pool being executed.
 * <li>futurepool/task_execution_time: opstats. measuring the characteristics about the time that tasks spent
 * on executing.
 * <li>futurepool/task_enqueue_time: opstats. measuring the characteristics about the time that tasks spent on
 * submitting to future pool.
 * <li>futurepool/tasks_pending: gauge. how many tasks are pending in this future pool.
 * </ul>
 */
public class OrderedScheduler implements ScheduledExecutorService {

    /**
     * Create a builder to build scheduler.
     *
     * @return scheduler builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link OrderedScheduler}.
     */
    public static class Builder {

        private String name = "OrderedScheduler";
        private int corePoolSize = -1;
        private ThreadFactory threadFactory = null;
        private boolean traceTaskExecution = false;
        private long traceTaskExecutionWarnTimeUs = Long.MAX_VALUE;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        private StatsLogger perExecutorStatsLogger = NullStatsLogger.INSTANCE;

        /**
         * Set the name of this scheduler. It would be used as part of stats scope and thread name.
         *
         * @param name
         *          name of the scheduler.
         * @return scheduler builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the number of threads to be used in this scheduler.
         *
         * @param corePoolSize the number of threads to keep in the pool, even
         *        if they are idle
         * @return scheduler builder
         */
        public Builder corePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
            return this;
        }

        /**
         * Set the thread factory that the scheduler uses to create a new thread.
         *
         * @param threadFactory the factory to use when the executor
         *        creates a new thread
         * @return scheduler builder
         */
        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Enable/Disable exposing task execution stats.
         *
         * @param trace
         *          flag to enable/disable exposing task execution stats.
         * @return scheduler builder
         */
        public Builder traceTaskExecution(boolean trace) {
            this.traceTaskExecution = trace;
            return this;
        }

        /**
         * Enable/Disable logging slow tasks whose execution time is above <code>timeUs</code>.
         *
         * @param timeUs
         *          slow task execution time threshold in us.
         * @return scheduler builder.
         */
        public Builder traceTaskExecutionWarnTimeUs(long timeUs) {
            this.traceTaskExecutionWarnTimeUs = timeUs;
            return this;
        }

        /**
         * Expose the aggregated stats over <code>statsLogger</code>.
         *
         * @param statsLogger
         *          stats logger to receive aggregated stats.
         * @return scheduler builder
         */
        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        /**
         * Expose stats of individual executors over <code>perExecutorStatsLogger</code>.
         * Each executor's stats will be exposed under a sub-scope `name`-executor-`id`-0.
         * `name` is the scheduler name, while `id` is the index of the scheduler in the pool.
         *
         * @param perExecutorStatsLogger
         *          stats logger to receive per executor stats.
         * @return scheduler builder
         */
        public Builder perExecutorStatsLogger(StatsLogger perExecutorStatsLogger) {
            this.perExecutorStatsLogger = perExecutorStatsLogger;
            return this;
        }

        /**
         * Build the ordered scheduler.
         *
         * @return ordered scheduler
         */
        public OrderedScheduler build() {
            if (corePoolSize <= 0) {
                corePoolSize = Runtime.getRuntime().availableProcessors();
            }
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }

            return new OrderedScheduler(
                    name,
                    corePoolSize,
                    threadFactory,
                    traceTaskExecution,
                    traceTaskExecutionWarnTimeUs,
                    statsLogger,
                    perExecutorStatsLogger);
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
                             StatsLogger statsLogger,
                             StatsLogger perExecutorStatsLogger) {
        this.name = name;
        this.corePoolSize = corePoolSize;
        this.executors = new MonitoredScheduledThreadPoolExecutor[corePoolSize];
        this.futurePools = new MonitoredFuturePool[corePoolSize];
        for (int i = 0; i < corePoolSize; i++) {
            ThreadFactory tf = new ThreadFactoryBuilder()
                    .setNameFormat(name + "-executor-" + i + "-%d")
                    .setThreadFactory(threadFactory)
                    .build();
            StatsLogger broadcastStatsLogger =
                    BroadCastStatsLogger.masterslave(perExecutorStatsLogger.scope("executor-" + i), statsLogger);
            executors[i] = new MonitoredScheduledThreadPoolExecutor(
                    1, tf, broadcastStatsLogger, traceTaskExecution);
            futurePools[i] = new MonitoredFuturePool(
                    new ExecutorServiceFuturePool(executors[i]),
                    broadcastStatsLogger.scope("futurepool"),
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

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return chooseExecutor().schedule(command, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return chooseExecutor().schedule(callable, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay, long period, TimeUnit unit) {
        return chooseExecutor().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                     long initialDelay, long delay, TimeUnit unit) {
        return chooseExecutor().scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            executor.shutdown();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            runnables.addAll(executor.shutdownNow());
        }
        return runnables;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            if (!executor.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTerminated() {
        for (MonitoredScheduledThreadPoolExecutor executor : executors) {
            if (!executor.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return chooseExecutor().submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return chooseExecutor().submit(task, result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<?> submit(Runnable task) {
        return chooseExecutor().submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return chooseExecutor().invokeAll(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return chooseExecutor().invokeAll(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        return chooseExecutor().invokeAny(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return chooseExecutor().invokeAny(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable command) {
        chooseExecutor().execute(command);
    }

    // Ordered Functions

    /**
     * Execute the <code>function</code> in the executor that assigned by <code>key</code>.
     *
     * @see com.twitter.util.Future
     * @param key key of the <i>function</i> to run
     * @param function function to run
     * @return future representing the result of the <i>function</i>
     */
    public <T> com.twitter.util.Future<T> apply(Object key, Function0<T> function) {
        return chooseFuturePool(key).apply(function);
    }

    /**
     * Execute the <code>function</code> by the scheduler. It would be submitted to any executor randomly.
     *
     * @param function function to run
     * @return future representing the result of the <i>function</i>
     */
    public <T> com.twitter.util.Future<T> apply(Function0<T> function) {
        return chooseFuturePool().apply(function);
    }

    public ScheduledFuture<?> schedule(Object key, Runnable command, long delay, TimeUnit unit) {
        return chooseExecutor(key).schedule(command, delay, unit);
    }

    public Future<?> submit(Object key, Runnable command) {
        return chooseExecutor(key).submit(command);
    }
}
