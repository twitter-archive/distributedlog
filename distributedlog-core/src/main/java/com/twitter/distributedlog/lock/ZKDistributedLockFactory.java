package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory to create zookeeper based locks.
 */
public class ZKDistributedLockFactory implements DistributedLockFactory {

    private final ZooKeeperClient zkc;
    private final String clientId;
    private final OrderedScheduler lockStateExecutor;
    private final long lockOpTimeout;
    private final int lockCreationRetries;
    private final long zkRetryBackoffMs;

    // Stats
    private final StatsLogger lockStatsLogger;

    public ZKDistributedLockFactory(ZooKeeperClient zkc,
                                    String clientId,
                                    OrderedScheduler lockStateExecutor,
                                    int lockCreationRetries,
                                    long lockOpTimeout,
                                    long zkRetryBackoffMs,
                                    StatsLogger statsLogger) {
        this.zkc = zkc;
        this.clientId = clientId;
        this.lockStateExecutor = lockStateExecutor;
        this.lockCreationRetries = lockCreationRetries;
        this.lockOpTimeout = lockOpTimeout;
        this.zkRetryBackoffMs = zkRetryBackoffMs;

        this.lockStatsLogger = statsLogger.scope("lock");
    }

    @Override
    public Future<DistributedLock> createLock(String lockPath,
                                              DistributedLockContext context) {
        AtomicInteger numRetries = new AtomicInteger(lockCreationRetries);
        final AtomicReference<Throwable> interruptedException = new AtomicReference<Throwable>(null);
        Promise<DistributedLock> createPromise =
                new Promise<DistributedLock>(new com.twitter.util.Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable t) {
                interruptedException.set(t);
                return BoxedUnit.UNIT;
            }
        });
        createLock(
                lockPath,
                context,
                interruptedException,
                numRetries,
                createPromise,
                0L);
        return createPromise;
    }

    void createLock(final String lockPath,
                    final DistributedLockContext context,
                    final AtomicReference<Throwable> interruptedException,
                    final AtomicInteger numRetries,
                    final Promise<DistributedLock> createPromise,
                    final long delayMs) {
        lockStateExecutor.schedule(lockPath, new Runnable() {
            @Override
            public void run() {
                if (null != interruptedException.get()) {
                    createPromise.updateIfEmpty(new Throw<DistributedLock>(interruptedException.get()));
                    return;
                }
                try {
                    DistributedLock lock = new ZKDistributedLock(
                            zkc,
                            lockPath,
                            clientId,
                            lockStateExecutor,
                            lockOpTimeout,
                            lockStatsLogger,
                            context);
                    createPromise.updateIfEmpty(new Return<DistributedLock>(lock));
                } catch (DLInterruptedException dlie) {
                    // if the creation is interrupted, throw the exception without retrie.
                    createPromise.updateIfEmpty(new Throw<DistributedLock>(dlie));
                    return;
                } catch (IOException e) {
                    if (numRetries.getAndDecrement() < 0) {
                        createPromise.updateIfEmpty(new Throw<DistributedLock>(e));
                        return;
                    }
                    createLock(
                            lockPath,
                            context,
                            interruptedException,
                            numRetries,
                            createPromise,
                            zkRetryBackoffMs);
                }
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }
}
