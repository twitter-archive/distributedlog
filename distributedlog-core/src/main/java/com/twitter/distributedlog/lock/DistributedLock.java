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
package com.twitter.distributedlog.lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.FutureUtils.OrderedFutureEventListener;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed lock, using ZooKeeper.
 * <p/>
 * The lock is vulnerable to timing issues. For example, the process could
 * encounter a really long GC cycle between acquiring the lock, and writing to
 * a ledger. This could have timed out the lock, and another process could have
 * acquired the lock and started writing to bookkeeper. Therefore other
 * mechanisms are required to ensure correctness (i.e. Fencing).
 * <p/>
 * The lock is only allowed to acquire once. If the lock is acquired successfully,
 * the caller holds the ownership until it loses the ownership either because of
 * others already acquired the lock when session expired or explicitly close it.
 * <p>
 * The caller could use {@link #checkOwnership()} or {@link #checkOwnershipAndReacquire()}
 * to check if it still holds the lock. If it doesn't hold the lock, the caller should
 * give up the ownership and close the lock.
 * <h3>Metrics</h3>
 * All the lock related stats are exposed under `lock`.
 * <ul>
 * <li>lock/acquire: opstats. latency spent on acquiring a lock.
 * <li>lock/reacquire: opstats. latency spent on re-acquiring a lock.
 * <li>lock/internalTryRetries: counter. the number of retries on re-creating internal locks.
 * </ul>
 * Other internal lock related stats are also exposed under `lock`. See {@link SessionLock}
 * for details.
 */
public class DistributedLock implements LockListener, AsyncCloseable {

    static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);

    private final SessionLockFactory lockFactory;
    private final OrderedScheduler lockStateExecutor;
    private final String lockPath;
    private final long lockTimeout;
    private final DistributedLockContext lockContext = new DistributedLockContext();

    // We have two lock acquire futures:
    // 1. lock acquire future: for the initial acquire op
    // 2. lock reacquire future: for reacquire necessary when session expires, lock is closed
    private Future<DistributedLock> lockAcquireFuture = null;
    private Future<DistributedLock> lockReacquireFuture = null;
    // following variable tracking the status of acquire process
    //   => create (internalLock) => tryLock (tryLockFuture) => waitForAcquire (lockWaiter)
    private SessionLock internalLock = null;
    private Future<LockWaiter> tryLockFuture = null;
    private LockWaiter lockWaiter = null;
    // exception indicating if the reacquire failed
    private LockingException lockReacquireException = null;
    // closeFuture
    private volatile boolean closed = false;
    private Future<Void> closeFuture = null;

    // A counter to track how many re-acquires happened during a lock's life cycle.
    private final AtomicInteger reacquireCount = new AtomicInteger(0);
    private final StatsLogger lockStatsLogger;
    private final OpStatsLogger acquireStats;
    private final OpStatsLogger reacquireStats;
    private final Counter internalTryRetries;

    public DistributedLock(
            OrderedScheduler lockStateExecutor,
            SessionLockFactory lockFactory,
            String lockPath,
            long lockTimeout,
            StatsLogger statsLogger) {
        this.lockStateExecutor = lockStateExecutor;
        this.lockPath = lockPath;
        this.lockTimeout = lockTimeout;
        this.lockFactory = lockFactory;

        lockStatsLogger = statsLogger.scope("lock");
        acquireStats = lockStatsLogger.getOpStatsLogger("acquire");
        reacquireStats = lockStatsLogger.getOpStatsLogger("reacquire");
        internalTryRetries = lockStatsLogger.getCounter("internalTryRetries");
    }

    private LockClosedException newLockClosedException() {
        return new LockClosedException(lockPath, "Lock is already closed");
    }

    private synchronized void checkLockState() throws LockingException {
        if (closed) {
            throw newLockClosedException();
        }
        if (null != lockReacquireException) {
            throw lockReacquireException;
        }
    }

    /**
     * Asynchronously acquire the lock. Technically the try phase of this operation--which adds us to the waiter
     * list--is executed synchronously, but the lock wait itself doesn't block.
     */
    public synchronized Future<DistributedLock> asyncAcquire() {
        if (null != lockAcquireFuture) {
            return Future.exception(new UnexpectedException("Someone is already acquiring/acquired lock " + lockPath));
        }
        final Promise<DistributedLock> promise =
                new Promise<DistributedLock>(new Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable cause) {
                lockStateExecutor.submit(lockPath, new Runnable() {
                    @Override
                    public void run() {
                        asyncClose();
                    }
                });
                return BoxedUnit.UNIT;
            }
        });
        final Stopwatch stopwatch = Stopwatch.createStarted();
        promise.addEventListener(new FutureEventListener<DistributedLock>() {
            @Override
            public void onSuccess(DistributedLock lock) {
                acquireStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
            @Override
            public void onFailure(Throwable cause) {
                acquireStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                // release the lock if fail to acquire
                asyncClose();
            }
        });
        this.lockAcquireFuture = promise;
        lockStateExecutor.submit(lockPath, new Runnable() {
            @Override
            public void run() {
                doAsyncAcquire(promise, lockTimeout);
            }
        });
        return promise;
    }

    void doAsyncAcquire(final Promise<DistributedLock> acquirePromise,
                        final long lockTimeout) {
        LOG.trace("Async Lock Acquire {}", lockPath);
        try {
            checkLockState();
        } catch (IOException ioe) {
            FutureUtils.setException(acquirePromise, ioe);
            return;
        }

        lockFactory.createLock(lockPath, lockContext).addEventListener(OrderedFutureEventListener.of(
                new FutureEventListener<SessionLock>() {
            @Override
            public void onSuccess(SessionLock lock) {
                synchronized (DistributedLock.this) {
                    if (closed) {
                        LOG.info("Skipping tryLocking lock {} since it is already closed", lockPath);
                        FutureUtils.setException(acquirePromise, newLockClosedException());
                        return;
                    }
                }
                synchronized (DistributedLock.this) {
                    internalLock = lock;
                    internalLock.setLockListener(DistributedLock.this);
                }
                asyncTryLock(lock, acquirePromise, lockTimeout);
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.setException(acquirePromise, cause);
            }
        }, lockStateExecutor, lockPath));
    }

    void asyncTryLock(SessionLock lock,
                      final Promise<DistributedLock> acquirePromise,
                      final long lockTimeout) {
        if (null != tryLockFuture) {
            tryLockFuture.cancel();
        }
        tryLockFuture = lock.asyncTryLock(lockTimeout, TimeUnit.MILLISECONDS);
        tryLockFuture.addEventListener(OrderedFutureEventListener.of(
                new FutureEventListener<LockWaiter>() {
                    @Override
                    public void onSuccess(LockWaiter waiter) {
                        synchronized (DistributedLock.this) {
                            if (closed) {
                                LOG.info("Skipping acquiring lock {} since it is already closed", lockPath);
                                waiter.getAcquireFuture().raise(new LockingException(lockPath, "lock is already closed."));
                                FutureUtils.setException(acquirePromise, newLockClosedException());
                                return;
                            }
                        }
                        tryLockFuture = null;
                        lockWaiter = waiter;
                        waitForAcquire(waiter, acquirePromise);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        FutureUtils.setException(acquirePromise, cause);
                    }
                }, lockStateExecutor, lockPath));
    }

    void waitForAcquire(final LockWaiter waiter,
                        final Promise<DistributedLock> acquirePromise) {
        waiter.getAcquireFuture().addEventListener(OrderedFutureEventListener.of(
                new FutureEventListener<Boolean>() {
                    @Override
                    public void onSuccess(Boolean acquired) {
                        LOG.info("{} acquired lock {}", waiter, lockPath);
                        if (acquired) {
                            FutureUtils.setValue(acquirePromise, DistributedLock.this);
                        } else {
                            FutureUtils.setException(acquirePromise,
                                    new OwnershipAcquireFailedException(lockPath, waiter.getCurrentOwner()));
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        FutureUtils.setException(acquirePromise, cause);
                    }
                }, lockStateExecutor, lockPath));
    }

    /**
     * NOTE: The {@link LockListener#onExpired()} is already executed in lock executor.
     */
    @Override
    public void onExpired() {
        try {
            reacquireLock(false);
        } catch (LockingException le) {
            // should not happen
            LOG.error("Locking exception on re-acquiring lock {} : ", lockPath, le);
        }
    }

    /**
     * Check if hold lock, if it doesn't, then re-acquire the lock.
     *
     * @throws LockingException     if the lock attempt fails
     */
    public synchronized void checkOwnershipAndReacquire() throws LockingException {
        if (null == lockAcquireFuture || !lockAcquireFuture.isDefined()) {
            throw new LockingException(lockPath, "check ownership before acquiring");
        }

        if (haveLock()) {
            return;
        }

        // We may have just lost the lock because of a ZK session timeout
        // not necessarily because someone else acquired the lock.
        // In such cases just try to reacquire. If that fails, it will throw
        reacquireLock(true);
    }

    /**
     * Check if lock is held.
     * If not, error out and do not reacquire. Use this in cases where there are many waiters by default
     * and reacquire is unlikley to succeed.
     *
     * @throws LockingException     if the lock attempt fails
     */
    public synchronized void checkOwnership() throws LockingException {
        if (null == lockAcquireFuture || !lockAcquireFuture.isDefined()) {
            throw new LockingException(lockPath, "check ownership before acquiring");
        }
        if (!haveLock()) {
            throw new LockingException(lockPath, "Lost lock ownership");
        }
    }

    @VisibleForTesting
    int getReacquireCount() {
        return reacquireCount.get();
    }

    @VisibleForTesting
    Future<DistributedLock> getLockReacquireFuture() {
        return lockReacquireFuture;
    }

    @VisibleForTesting
    Future<DistributedLock> getLockAcquireFuture() {
        return lockAcquireFuture;
    }

    @VisibleForTesting
    synchronized SessionLock getInternalLock() {
        return internalLock;
    }

    @VisibleForTesting
    LockWaiter getLockWaiter() {
        return lockWaiter;
    }

    synchronized boolean haveLock() {
        return !closed && internalLock != null && internalLock.isLockHeld();
    }

    void closeWaiter(final LockWaiter waiter,
                     final Promise<Void> closePromise) {
        if (null == waiter) {
            interruptTryLock(tryLockFuture, closePromise);
        } else {
            waiter.getAcquireFuture().addEventListener(OrderedFutureEventListener.of(
                    new FutureEventListener<Boolean>() {
                        @Override
                        public void onSuccess(Boolean value) {
                            unlockInternalLock(closePromise);
                        }
                        @Override
                        public void onFailure(Throwable cause) {
                            unlockInternalLock(closePromise);
                        }
                    }, lockStateExecutor, lockPath));
            FutureUtils.cancel(waiter.getAcquireFuture());
        }
    }

    void interruptTryLock(final Future<LockWaiter> tryLockFuture,
                          final Promise<Void> closePromise) {
        if (null == tryLockFuture) {
            unlockInternalLock(closePromise);
        } else {
            tryLockFuture.addEventListener(OrderedFutureEventListener.of(
                    new FutureEventListener<LockWaiter>() {
                        @Override
                        public void onSuccess(LockWaiter waiter) {
                            closeWaiter(waiter, closePromise);
                        }
                        @Override
                        public void onFailure(Throwable cause) {
                            unlockInternalLock(closePromise);
                        }
                    }, lockStateExecutor, lockPath));
            FutureUtils.cancel(tryLockFuture);
        }
    }

    synchronized void unlockInternalLock(final Promise<Void> closePromise) {
        if (internalLock == null) {
            FutureUtils.setValue(closePromise, null);
        } else {
            internalLock.asyncUnlock().ensure(new AbstractFunction0<BoxedUnit>() {
                @Override
                public BoxedUnit apply() {
                    FutureUtils.setValue(closePromise, null);
                    return BoxedUnit.UNIT;
                }
            });
        }
    }

    @Override
    public Future<Void> asyncClose() {
        final Promise<Void> closePromise;
        synchronized (this) {
            if (closed) {
                return closeFuture;
            }
            closed = true;
            closeFuture = closePromise = new Promise<Void>();
        }
        final Promise<Void> closeWaiterFuture = new Promise<Void>();
        closeWaiterFuture.addEventListener(OrderedFutureEventListener.of(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                complete();
            }
            @Override
            public void onFailure(Throwable cause) {
                complete();
            }

            private void complete() {
                FutureUtils.setValue(closePromise, null);
            }
        }, lockStateExecutor, lockPath));
        lockStateExecutor.submit(lockPath, new Runnable() {
            @Override
            public void run() {
                closeWaiter(lockWaiter, closeWaiterFuture);
            }
        });
        return closePromise;
    }

    void internalReacquireLock(final AtomicInteger numRetries,
                               final long lockTimeout,
                               final Promise<DistributedLock> reacquirePromise) {
        lockStateExecutor.submit(lockPath, new Runnable() {
            @Override
            public void run() {
                doInternalReacquireLock(numRetries, lockTimeout, reacquirePromise);
            }
        });
    }

    void doInternalReacquireLock(final AtomicInteger numRetries,
                                 final long lockTimeout,
                                 final Promise<DistributedLock> reacquirePromise) {
        internalTryRetries.inc();
        Promise<DistributedLock> tryPromise = new Promise<DistributedLock>();
        tryPromise.addEventListener(new FutureEventListener<DistributedLock>() {
            @Override
            public void onSuccess(DistributedLock lock) {
                FutureUtils.setValue(reacquirePromise, lock);
            }

            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof OwnershipAcquireFailedException) {
                    // the lock has been acquired by others
                    FutureUtils.setException(reacquirePromise, cause);
                } else {
                    if (numRetries.getAndDecrement() > 0 && !closed) {
                        internalReacquireLock(numRetries, lockTimeout, reacquirePromise);
                    } else {
                        FutureUtils.setException(reacquirePromise, cause);
                    }
                }
            }
        });
        doAsyncAcquire(tryPromise, 0);
    }

    private Future<DistributedLock> reacquireLock(boolean throwLockAcquireException) throws LockingException {
        LOG.info("reacquiring lock at {}", lockPath);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        Promise<DistributedLock> lockPromise;
        synchronized (this) {
            if (closed) {
                throw newLockClosedException();
            }
            if (null != lockReacquireException) {
                if (throwLockAcquireException) {
                    throw lockReacquireException;
                } else {
                    return null;
                }
            }
            if (null != lockReacquireFuture) {
                return lockReacquireFuture;
            }
            lockReacquireFuture = lockPromise = new Promise<DistributedLock>();
            lockReacquireFuture.addEventListener(new FutureEventListener<DistributedLock>() {
                @Override
                public void onSuccess(DistributedLock lock) {
                    // if re-acquire successfully, clear the state.
                    synchronized (DistributedLock.this) {
                        lockReacquireFuture = null;
                    }
                    reacquireStats.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                }

                @Override
                public void onFailure(Throwable cause) {
                    synchronized (DistributedLock.this) {
                        if (cause instanceof LockingException) {
                            lockReacquireException = (LockingException) cause;
                        } else {
                            lockReacquireException = new LockingException(lockPath,
                                    "Exception on re-acquiring lock", cause);
                        }
                    }
                    reacquireStats.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                }
            });
        }
        reacquireCount.incrementAndGet();
        internalReacquireLock(new AtomicInteger(Integer.MAX_VALUE), 0, lockPromise);
        return lockPromise;
    }

}
