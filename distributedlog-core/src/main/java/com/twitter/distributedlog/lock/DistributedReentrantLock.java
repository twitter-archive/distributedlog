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
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.TimeoutException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Distributed lock, using ZooKeeper.
 * <p/>
 * The lock is vulnerable to timing issues. For example, the process could
 * encounter a really long GC cycle between acquiring the lock, and writing to
 * a ledger. This could have timed out the lock, and another process could have
 * acquired the lock and started writing to bookkeeper. Therefore other
 * mechanisms are required to ensure correctness (i.e. Fencing).
 * <p/>
 * There are sync (acquire) and async (asyncAcquire) apis available in the
 * lock interface. It is inadvisable to use them together. Doing so will
 * essentialy render timeouts meaningless because the async interface doesn't
 * support timeouts. I.e. if you asyncAcquire and then sync acquire with a
 * timeout, the sync acquire timeout will cancel the lock, and cause the
 * asyncAcquire to abort. That said, using both together is considered safe
 * and is tested.
 * <h3>Metrics</h3>
 * All the lock related stats are exposed under `lock`.
 * <ul>
 * <li>lock/acquire: opstats. latency spent on acquiring a lock.
 * <li>lock/release: opstats. latency spent on releasing a lock.
 * <li>lock/reacquire: opstats. latency spent on re-acquiring a lock.
 * <li>lock/internalTryRetries: counter. the number of retries on re-creating internal locks.
 * <li>lock/acquireTimeouts: counter. the number of timeouts on acquiring locks
 * </ul>
 * Other internal lock related stats are also exposed under `lock`. See {@link DistributedLock}
 * for details.
 */
public class DistributedReentrantLock {

    static final Logger LOG = LoggerFactory.getLogger(DistributedReentrantLock.class);

    private final OrderedSafeExecutor lockStateExecutor;
    private final String lockPath;

    // We have two lock acquire futures:
    // 1. lock acquire future (in lockAcquireInfo): for the initial acquire op, unset only on full lock release
    // 2. lock reacquire future: for reacquire necessary when session expires, lock is closed
    // Future [2] is set and unset every time expire/close occurs, behind the scenes, whereas
    // future [1] is set and completed for the lifetime of one underlying acquire.
    private LockAcquireInfo lockAcquireInfo = null;
    private Future<String> lockReacquireFuture = null;
    private LockingException lockReacquireException = null;

    private final AtomicInteger lockCount = new AtomicInteger(0);
    private final AtomicIntegerArray lockAcqTracker;
    private ZKDistributedLock internalLock;
    private final ZooKeeperClient zooKeeperClient;
    private final String clientId;
    private final long lockTimeout;
    private final long lockReacquireTimeout;
    private final long lockOpTimeout;
    private final int lockCreationRetries;
    private boolean logUnbalancedWarning = true;
    private volatile boolean closed = false;
    private final DistributedLockContext lockContext = new DistributedLockContext();

    // A counter to track how many re-acquires happened during a lock's life cycle.
    private final AtomicInteger reacquireCount = new AtomicInteger(0);

    public enum LockReason {
        WRITEHANDLER (0), PERSTREAMWRITER(1), RECOVER(2), COMPLETEANDCLOSE(3), DELETELOG(4),
        READHANDLER(5), MAXREASON(6);
        private final int value;

        private LockReason(int value) {
            this.value = value;
        }
    }

    private final StatsLogger lockStatsLogger;
    private final OpStatsLogger acquireStats;
    private final OpStatsLogger releaseStats;
    private final OpStatsLogger reacquireStats;
    private final Counter internalTryRetries;
    private final Counter checkAndReacquireTimeouts;

    private final FutureEventListener<Void> RESET_ACQUIRE_STATE_ON_FAILURE = new FutureEventListener<Void>() {
        @Override
        public void onSuccess(Void complete) {
        }
        @Override
        public void onFailure(Throwable cause) {
            synchronized (DistributedReentrantLock.this) {
                lockAcquireInfo = null;
            }
        }
    };

    /**
     * Construct a lock.
     * NOTE: ensure the lockpath is created before constructing the lock.
     *
     * @param zkc
     *          zookeeper client.
     * @param lockPath
     *          lock path.
     * @param lockTimeout
     *          lock timeout.
     * @param clientId
     *          client id.
     * @throws IOException
     */
    DistributedReentrantLock(
        OrderedSafeExecutor lockStateExecutor,
        ZooKeeperClient zkc,
        String lockPath,
        long lockTimeout,
        String clientId,
        StatsLogger statsLogger) throws IOException {
        this(lockStateExecutor, zkc, lockPath, lockTimeout, clientId, statsLogger, 0,
             DistributedLogConstants.LOCK_REACQUIRE_TIMEOUT_DEFAULT * 1000,
             DistributedLogConstants.LOCK_OP_TIMEOUT_DEFAULT * 1000);
    }

    DistributedReentrantLock(
        OrderedSafeExecutor lockStateExecutor,
        ZooKeeperClient zkc,
        String lockPath,
        long lockTimeout,
        String clientId,
        StatsLogger statsLogger,
        int lockCreationRetries) throws IOException {
        this(lockStateExecutor, zkc, lockPath, lockTimeout, clientId, statsLogger, lockCreationRetries,
             DistributedLogConstants.LOCK_REACQUIRE_TIMEOUT_DEFAULT * 1000,
             DistributedLogConstants.LOCK_OP_TIMEOUT_DEFAULT * 1000);
    }

    public DistributedReentrantLock(
            OrderedSafeExecutor lockStateExecutor,
            ZooKeeperClient zkc,
            String lockPath,
            long lockTimeout,
            String clientId,
            StatsLogger statsLogger,
            int lockCreationRetries,
            long lockReacquireTimeout,
            long lockOpTimeout) throws IOException {
        this.lockStateExecutor = lockStateExecutor;
        this.lockPath = lockPath;
        this.lockTimeout = lockTimeout;
        this.zooKeeperClient = zkc;
        this.clientId = clientId;
        this.lockCreationRetries = lockCreationRetries;
        this.lockAcqTracker = new AtomicIntegerArray(LockReason.MAXREASON.value);
        this.lockReacquireTimeout = lockReacquireTimeout;
        this.lockOpTimeout = lockOpTimeout;

        lockStatsLogger = statsLogger.scope("lock");
        acquireStats = lockStatsLogger.getOpStatsLogger("acquire");
        releaseStats = lockStatsLogger.getOpStatsLogger("release");
        reacquireStats = lockStatsLogger.getOpStatsLogger("reacquire");
        internalTryRetries = lockStatsLogger.getCounter("internalTryRetries");
        checkAndReacquireTimeouts = lockStatsLogger.getCounter("acquireTimeouts");

        // Initialize the internal lock
        this.internalLock = createInternalLock(new AtomicInteger(lockCreationRetries));
    }

    private static class LockAcquireInfo {
        private final String previousOwner;
        private final Future<Void> acquireFuture;
        public LockAcquireInfo(String previousOwner, Future<Void> acquireFuture) {
            this.previousOwner = previousOwner;
            this.acquireFuture = acquireFuture;
        }
        public Future<Void> getAcquireFuture() {
            return acquireFuture;
        }
        public String getPreviousOwner() {
            return previousOwner;
        }
    }

    public void acquire(LockReason reason) throws LockingException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            doAcquire(reason);
            success = true;
        } finally {
            if (success) {
                acquireStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                acquireStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    private synchronized void checkLockState() throws LockingException {
        if (closed) {
            throw new LockingException(lockPath, "Lock is closed");
        }
        if (null != lockReacquireException) {
            throw lockReacquireException;
        }
    }

    void doAcquire(LockReason reason) throws LockingException {
        LOG.trace("Lock Acquire {}, {}", lockPath, reason);
        checkLockState();
        while (true) {
            if (lockCount.get() == 0) {
                synchronized (this) {
                    if (lockCount.get() > 0) {
                        lockCount.incrementAndGet();
                        break;
                    }
                    if (internalLock.isExpiredOrClosing()) {
                        try {
                            internalLock = createInternalLock(new AtomicInteger(lockCreationRetries));
                        } catch (IOException e) {
                            throw new LockingException(lockPath, "Failed to create new internal lock", e);
                        }
                    }
                    if (null != lockAcquireInfo) {
                        if (!internalLock.waitForAcquire(lockAcquireInfo.getAcquireFuture(), lockTimeout, TimeUnit.MILLISECONDS)) {
                            throw new OwnershipAcquireFailedException(lockPath, lockAcquireInfo.getPreviousOwner());
                        }
                        lockCount.getAndIncrement();
                    } else {
                        internalLock.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
                        lockAcquireInfo = new LockAcquireInfo(null, internalLock.getAcquireFuture());
                        lockCount.set(1);
                    }
                    break;
                }
            } else {
                int ret = lockCount.getAndIncrement();
                if (ret == 0) {
                    lockCount.decrementAndGet();
                } else {
                    break;
                }
            }
        }
        lockAcqTracker.incrementAndGet(reason.value);
    }

    /**
     * Asynchronously acquire the lock. Technically the try phase of this operation--which adds us to the waiter
     * list--is executed synchronously, but the lock wait itself doesn't block.
     */
    public synchronized Future<Void> asyncAcquire(final LockReason reason) throws LockingException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return doAsyncAcquire(reason).addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void complete) {
                acquireStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
            @Override
            public void onFailure(Throwable cause) {
                acquireStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        });
    }

    synchronized Future<Void> doAsyncAcquire(final LockReason reason) throws LockingException {
        LOG.trace("Async Lock Acquire {}, {}", lockPath, reason);
        checkLockState();

        if (null == lockAcquireInfo) {
            if (internalLock.isExpiredOrClosing()) {
                try {
                    internalLock = createInternalLock(new AtomicInteger(lockCreationRetries));
                } catch (IOException ioe) {
                    LockingException lex = new LockingException(lockPath, "Failed to create new internal lock", ioe);
                    return Future.exception(lex);
                }
            }

            // We've got a valid internal lock, we need to try-acquire (which will begin the lock wait process)
            // synchronously. There's no lock blocking on the try operation, so this shouldn't take long.
            final Stopwatch stopwatch = Stopwatch.createStarted();
            Future<String> result = internalLock.asyncTryLock(DistributedLogConstants.LOCK_TIMEOUT_INFINITE, TimeUnit.MILLISECONDS);
            String previousOwner = internalLock.waitForTry(stopwatch, result);

            // Failure should return us to a valid state.
            Future<Void> acquireFuture = internalLock.getAcquireFuture().addEventListener(RESET_ACQUIRE_STATE_ON_FAILURE);
            lockAcquireInfo = new LockAcquireInfo(previousOwner, acquireFuture);
        }

        // Bump use counts as soon as we acquire the lock.
        return lockAcquireInfo.getAcquireFuture().addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void complete) {
                synchronized (DistributedReentrantLock.this) {
                    lockCount.getAndIncrement();
                    lockAcqTracker.incrementAndGet(reason.value);
                }
            }
            @Override
            public void onFailure(Throwable cause) {
            }
        });
    }

    public void release(LockReason reason) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            doRelease(reason, false);
            success = true;
        } finally {
            if (success) {
                releaseStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                releaseStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    void doRelease(LockReason reason, boolean exceptionOnUnbalancedHandling) {
        LOG.trace("Lock Release {}, {}", lockPath, reason);
        int perReasonLockCount = lockAcqTracker.decrementAndGet(reason.value);
        Future<BoxedUnit> unlockFuture = Future.Done();
        synchronized (this) {
            if ((perReasonLockCount < 0) && logUnbalancedWarning) {
                LOG.warn("Unbalanced lock handling for {} type {}, lockCount is {} ",
                    new Object[]{lockPath, reason, perReasonLockCount});
                if (exceptionOnUnbalancedHandling) {
                    throw new IllegalStateException(
                        String.format("Unbalanced lock handling for %s type %s, lockCount is %s",
                            lockPath, reason, perReasonLockCount));
                }
            }
            if (lockCount.decrementAndGet() <= 0) {
                if (logUnbalancedWarning && (lockCount.get() < 0)) {
                    LOG.warn("Unbalanced lock handling for {}, lockCount is {} ",
                        lockPath, lockCount.get());
                    logUnbalancedWarning = false;
                }
                if (lockCount.get() <= 0) {
                    if (internalLock.isLockHeld()) {
                        LOG.info("Lock Release {}, {}", lockPath, reason);
                        unlockFuture = internalLock.unlockAsync();
                    }
                    lockAcquireInfo = null;
                }
            }
        }
        try {
            Await.result(unlockFuture);
        } catch (Exception e) {
            LOG.warn("{} failed to unlock {} : ", lockPath, e);
        }
    }

    /**
     * Check if hold lock, if it doesn't, then re-acquire the lock.
     *
     * @param sync  should we wait for the reacquire attempt to complete
     * @throws LockingException     if the lock attempt fails
     */
    public void checkOwnershipAndReacquire(boolean sync) throws LockingException {
        Future<String> futureToWaitFor;
        synchronized (this) {
            if (haveLock()) {
                return;
            }

            // We may have just lost the lock because of a ZK session timeout
            // not necessarily because someone else acquired the lock.
            // In such cases just try to reacquire. If that fails, it will throw
            futureToWaitFor = reacquireLock(true);
        }

        if (null != futureToWaitFor && sync) {
            try {
                Await.result(futureToWaitFor, Duration.fromMilliseconds(lockReacquireTimeout));
            } catch (TimeoutException toe) {
                // Should rarely happen--if it does, higher layer expected to cleanup the lock.
                checkAndReacquireTimeouts.inc();
                throw new LockingException(lockPath, "Timeout re-acquiring lock : ", toe);
            } catch (Exception e) {
                if (e instanceof LockingException) {
                    throw (LockingException) e;
                } else {
                    throw new LockingException(lockPath, "Exception on re-acquiring lock : ", e);
                }
            }
        }
    }

    /**
     * Check if lock is held.
     * If not, error out and do not reacquire. Use this in cases where there are many waiters by default
     * and reacquire is unlikley to succeed.
     *
     * @throws LockingException     if the lock attempt fails
     */
    public void checkOwnership() throws LockingException {
        synchronized (this) {
            if (!haveLock()) {
                throw new LockingException(lockPath, "Lost lock ownership");
            }
        }
    }

    @VisibleForTesting
    int getReacquireCount() {
        return reacquireCount.get();
    }

    @VisibleForTesting
    Future<String> getLockReacquireFuture() {
        return lockReacquireFuture;
    }

    @VisibleForTesting
    public int getLockCount() {
        return lockCount.get();
    }

    @VisibleForTesting
    ZKDistributedLock getInternalLock() {
        return internalLock;
    }

    boolean haveLock() {
        return internalLock.isLockHeld();
    }

    public void close() {
        Future<String> lockReacquireFutureSaved = null;
        DistributedLock internalLockSaved;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            lockReacquireFutureSaved = lockReacquireFuture;
            internalLockSaved = internalLock;
        }
        if (null != lockReacquireFutureSaved) {
            try {
                Await.result(lockReacquireFutureSaved, Duration.fromMilliseconds(lockReacquireTimeout));
            } catch (TimeoutException toe) {
                LOG.error("Timeout re-acquiring lock {} : ", lockPath, toe);
            } catch (Exception e) {
                LOG.warn("Exception while waiting to re-acquire lock {} : ", lockPath, e);
            }
        }
        internalLockSaved.unlock();
    }

    void internalTryLock(final AtomicInteger numRetries, final long lockTimeout,
                         final Promise<String> result) {
        internalTryRetries.inc();
        lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
            @Override
            public void safeRun() {
                ZKDistributedLock lock;
                synchronized (DistributedReentrantLock.this) {
                    try {
                        lock = createInternalLock(numRetries);
                    } catch (IOException ioe) {
                        result.setException(ioe);
                        return;
                    }
                    internalLock = lock;
                }
                lock.asyncTryLock(lockTimeout, TimeUnit.MILLISECONDS)
                        .addEventListener(new FutureEventListener<String>() {
                            @Override
                            public void onSuccess(String owner) {
                                // Try lock success means *either* we got the lock or we discovered someone
                                // else owns it. Failure means indicates some internal failure, ex. connection
                                // issue. Handle both here.
                                if (internalLock.isLockHeld()) {
                                    result.setValue(owner);
                                } else {
                                    result.setException(new OwnershipAcquireFailedException(lockPath, owner));
                                }
                            }

                            @Override
                            public void onFailure(Throwable cause) {
                                if (numRetries.getAndDecrement() > 0 && !closed) {
                                    internalTryLock(numRetries, lockTimeout, result);
                                } else {
                                    result.setException(cause);
                                }
                            }
                        });
            }
        });
    }

    ZKDistributedLock createInternalLock(AtomicInteger numRetries) throws IOException {
        ZKDistributedLock lock;
        do {
            if (closed) {
                throw new LockingException(lockPath, "Lock is closed");
            }
            try {
                lock = new ZKDistributedLock(
                        zooKeeperClient,
                        lockPath,
                        clientId,
                        lockStateExecutor,
                        lockOpTimeout,
                        lockStatsLogger,
                        lockContext)
                .setLockListener(new LockListener() {
                    @Override
                    public void onExpired() {
                        try {
                            reacquireLock(false);
                        } catch (LockingException e) {
                            // should not happen
                            LOG.error("Locking exception on re-acquiring lock {} : ", lockPath, e);
                        }
                    }
                });
            } catch (DLInterruptedException dlie) {
                // if the creation is interrupted, throw the exception without retrie.
                throw dlie;
            } catch (IOException ioe) {
                lock = null;
                if (numRetries.getAndDecrement() < 0) {
                    throw ioe;
                }
            }
        } while (null == lock);
        return lock;
    }

    private Future<String> reacquireLock(boolean throwLockAcquireException) throws LockingException {
        LOG.info("reacquiring lock at {}", lockPath);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        Promise<String> lockPromise;
        synchronized (this) {
            if (closed) {
                throw new LockingException(lockPath, "Lock is closed");
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
            lockReacquireFuture = lockPromise = new Promise<String>();
            lockReacquireFuture.addEventListener(new FutureEventListener<String>() {
                @Override
                public void onSuccess(String value) {
                    // if re-acquire successfully, clear the state.
                    synchronized (DistributedReentrantLock.this) {
                        lockReacquireFuture = null;
                    }
                    reacquireStats.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                }

                @Override
                public void onFailure(Throwable cause) {
                    synchronized (DistributedReentrantLock.this) {
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
        internalTryLock(new AtomicInteger(Integer.MAX_VALUE), 0, lockPromise);
        return lockPromise;
    }

}
