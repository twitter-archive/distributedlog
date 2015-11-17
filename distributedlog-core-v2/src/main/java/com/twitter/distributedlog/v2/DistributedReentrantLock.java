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
package com.twitter.distributedlog.v2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.rmi.UnexpectedException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Distributed lock, using ZooKeeper.
 * <p/>
 * The lock is vulnerable to timing issues. For example, the process could
 * encounter a really long GC cycle between acquiring the lock, and writing to
 * a ledger. This could have timed out the lock, and another process could have
 * acquired the lock and started writing to bookkeeper. Therefore other
 * mechanisms are required to ensure correctness (i.e. Fencing).
 */
class DistributedReentrantLock {

    static final Logger LOG = LoggerFactory.getLogger(DistributedReentrantLock.class);

    private final OrderedSafeExecutor lockStateExecutor;
    private final String lockPath;
    private Future<String> asyncLockAcquireFuture = null;
    private LockingException asyncLockAcquireException = null;

    private final AtomicInteger lockCount = new AtomicInteger(0);
    private final AtomicIntegerArray lockAcqTracker;
    private DistributedLock internalLock;
    private final ZooKeeperClient zooKeeperClient;
    private final String clientId;
    private final long lockTimeout;
    private final int lockCreationRetries;
    private final boolean allowSiblingLock;
    private boolean logUnbalancedWarning = true;
    private volatile boolean closed = false;

    public enum LockReason {
        WRITEHANDLER (0), PERSTREAMWRITER(1), RECOVER(2), COMPLETEANDCLOSE(3), DELETELOG(4), MAXREASON(5);
        private final int value;

        private LockReason(int value) {
            this.value = value;
        }
    }

    private static StatsLogger lockStatsLogger = null;
    private static OpStatsLogger acquireStats = null;
    private static OpStatsLogger releaseStats = null;

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
        this(lockStateExecutor, zkc, lockPath, lockTimeout, clientId, statsLogger, 0, true);
    }

    DistributedReentrantLock(
            OrderedSafeExecutor lockStateExecutor,
            ZooKeeperClient zkc,
            String lockPath,
            long lockTimeout,
            String clientId,
            StatsLogger statsLogger,
            int lockCreationRetries,
            boolean allowSiblingLock) throws IOException {
        this.lockStateExecutor = lockStateExecutor;
        this.lockPath = lockPath;
        this.lockTimeout = lockTimeout;
        this.zooKeeperClient = zkc;
        this.clientId = clientId;
        this.lockCreationRetries = lockCreationRetries;
        this.allowSiblingLock = allowSiblingLock;
        this.lockAcqTracker = new AtomicIntegerArray(LockReason.MAXREASON.value);

        try {
            if (zkc.get().exists(lockPath, false) == null) {
                zkc.get().create(lockPath, null, zkc.getDefaultACL(), CreateMode.PERSISTENT);
            }
        } catch (KeeperException ke) {
            throw new IOException("Exception when creating zookeeper lock " + lockPath, ke);
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on creating zookeeper lock " + lockPath, e);
        }
        this.internalLock = createInternalLock(new AtomicInteger(lockCreationRetries));

        if (null == lockStatsLogger) {
            lockStatsLogger = statsLogger.scope("lock");
        }
        if (null == acquireStats) {
            acquireStats = lockStatsLogger.getOpStatsLogger("acquire");
        }
        if (null == releaseStats) {
            releaseStats = lockStatsLogger.getOpStatsLogger("release");
        }
    }

    void acquire(LockReason reason) throws LockingException {
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
        if (null != asyncLockAcquireException) {
            throw asyncLockAcquireException;
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
                    if (internalLock.isExpiredOrClosed()) {
                        try {
                            internalLock = createInternalLock(new AtomicInteger(lockCreationRetries));
                        } catch (IOException e) {
                            throw new LockingException(lockPath, "Failed to create new internal lock", e);
                        }
                    }
                    if (internalLock.tryLock(lockTimeout, TimeUnit.MILLISECONDS)) {
                        lockCount.set(1);
                        break;
                    } else {
                        throw new LockingException(lockPath, "Failed to acquire lock within timeout");
                    }
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

    void release(LockReason reason) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            doRelease(reason);
            success = true;
        } finally {
            if (success) {
                releaseStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                releaseStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    void doRelease(LockReason reason) {
        LOG.trace("Lock Release {}, {}", lockPath, reason);
        int perReasonLockCount = lockAcqTracker.decrementAndGet(reason.value);
        if ((perReasonLockCount < 0) && logUnbalancedWarning) {
            LOG.warn("Unbalanced lock handling for {} type {}, lockCount is {} ",
                new Object[]{lockPath, reason, perReasonLockCount});
        }
        if (lockCount.decrementAndGet() <= 0) {
            if (logUnbalancedWarning && (lockCount.get() < 0)) {
                LOG.warn("Unbalanced lock handling for {}, lockCount is {} ",
                    lockPath, lockCount.get());
                logUnbalancedWarning = false;
            }
            synchronized (this) {
                if (lockCount.get() <= 0) {
                    if (internalLock.isLockHeld()) {
                        LOG.info("Lock Release {}, {}", lockPath, reason);
                        internalLock.unlock();
                    }
                }
            }
        }
    }

    /**
     * Check if hold write lock, if it doesn't, then re-acquire the lock.
     *
     * @param acquiresync
     * @throws LockingException
     */
    public void checkWriteLock(boolean acquiresync) throws LockingException {
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

        if (null != futureToWaitFor && acquiresync) {
            try {
                Await.result(futureToWaitFor);
            } catch (Exception e) {
                if (e instanceof LockingException) {
                    throw (LockingException) e;
                } else {
                    throw new LockingException(lockPath, "Exception on re-acquiring lock : ", e);
                }
            }
        }
    }

    @VisibleForTesting
    Future<String> getAsyncLockAcquireFuture() {
        return asyncLockAcquireFuture;
    }

    @VisibleForTesting
    int getLockCount() {
        return lockCount.get();
    }

    @VisibleForTesting
    DistributedLock getInternalLock() {
        return internalLock;
    }

    boolean haveLock() {
        return internalLock.isLockHeld();
    }

    public void close() {
        Future<String> futureToWait;
        DistributedLock lockToClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            futureToWait = asyncLockAcquireFuture;
            lockToClose = internalLock;
        }
        if (null != futureToWait) {
            try {
                Await.result(futureToWait);
            } catch (Exception e) {
                LOG.warn("Exception on waiting re-acquiring lock {} : ", lockPath, e);
            }
        }
        lockToClose.unlock();
    }

    void internalTryLock(final AtomicInteger numRetries, final long lockTimeout,
                         final Promise<String> result) {
        lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
            @Override
            public void safeRun() {
                DistributedLock lock;
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

    DistributedLock createInternalLock(AtomicInteger numRetries) throws IOException {
        DistributedLock lock;
        do {
            if (closed) {
                throw new LockingException(lockPath, "Lock is closed");
            }
            try {
                lock = new DistributedLock(zooKeeperClient, lockPath, clientId, allowSiblingLock, lockStateExecutor, new LockListener() {
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
        Promise<String> lockPromise;
        synchronized (this) {
            if (closed) {
                throw new LockingException(lockPath, "Lock is closed");
            }
            if (null != asyncLockAcquireException) {
                if (throwLockAcquireException) {
                    throw asyncLockAcquireException;
                } else {
                    return null;
                }
            }
            if (null != asyncLockAcquireFuture) {
                return asyncLockAcquireFuture;
            }
            asyncLockAcquireFuture = lockPromise = new Promise<String>();
            asyncLockAcquireFuture.addEventListener(new FutureEventListener<String>() {
                @Override
                public void onSuccess(String value) {
                    // if success re-acquire
                }

                @Override
                public void onFailure(Throwable cause) {
                    synchronized (DistributedReentrantLock.this) {
                        if (cause instanceof LockingException) {
                            asyncLockAcquireException = (LockingException) cause;
                        } else {
                            asyncLockAcquireException = new LockingException(lockPath,
                                    "Exception on re-acquiring lock", cause);
                        }
                    }
                }
            });
        }
        internalTryLock(new AtomicInteger(Integer.MAX_VALUE), 0, lockPromise);
        return lockPromise;
    }

    public static interface LockAction {
        void execute();
        String getActionName();
    }

    public static interface LockListener {
        /**
         * Triggered when a lock is changed from CLAIMED to EXPIRED.
         */
        void onExpired();
    }

    public static class LockStateChangedException extends LockingException {

        private static final long serialVersionUID = -3770866789942102262L;

        public LockStateChangedException(String lockPath, Pair<String, Long> lockId,
                                         DistributedLock.State expectedState, DistributedLock.State currentState) {
            super(lockPath, "Lock state of " + lockId + " for " + lockPath + " has changed : expected "
                    + expectedState + ", but " + currentState);
        }
    }

    /**
     * Exception indicates that epoch already changed when executing a given
     * {@link com.twitter.distributedlog.v2.DistributedReentrantLock.LockAction}.
     */
    public static class EpochChangedException extends LockingException {

        private static final long serialVersionUID = 8775257025963870331L;

        public EpochChangedException(String lockPath, int expectedEpoch, int currentEpoch) {
            super(lockPath, "lock " + lockPath + " already moved to epoch " + currentEpoch + ", expected " + expectedEpoch);
        }

    }

    /**
     * A lock under a given zookeeper session. This is a one-time lock.
     * It is not reusable: if lock failed, if zookeeper session is expired, if #unlock is called,
     * it would be transitioned to expired or closed state.
     *
     * The Locking Procedure is described as below.
     *
     * <p>
     * 0. if it is an immediate lock, it would get lock waiters first. if the lock is already held
     *    by someone. it would fail immediately with {@link com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException}
     *    with current owner. if there is no lock waiters, it would start locking procedure from 1.
     * 1. prepare: create a sequential znode to identify the lock.
     * 2. check lock waiters: get all lock waiters to check after prepare. if it is the first waiter,
     *    claim the ownership; if it is not the first waiter, but first waiter was itself (same client id and same session id)
     *    claim the ownership too; otherwise, it would set watcher on its sibling and wait it to disappared.
     * </p>
     */
    static class DistributedLock {

        private static final String LOCK_PATH_PREFIX = "/member_";
        private static final String LOCK_PART_SEP = "_";

        public static String getLockPathPrefixV1(String lockPath) {
            // member_
            return lockPath + LOCK_PATH_PREFIX;
        }

        public static String getLockPathPrefixV2(String lockPath, String clientId) throws UnsupportedEncodingException {
            // member_<clientid>_
            return lockPath + LOCK_PATH_PREFIX + URLEncoder.encode(clientId, UTF_8.name()) + LOCK_PART_SEP;
        }

        public static String getLockPathPrefixV3(String lockPath, String clientId, long sessionOwner) throws UnsupportedEncodingException {
            // member_<clientid>_s<owner_session>_
            StringBuilder sb = new StringBuilder();
            sb.append(lockPath).append(LOCK_PATH_PREFIX).append(URLEncoder.encode(clientId, UTF_8.name())).append(LOCK_PART_SEP)
                    .append("s").append(String.format("%10d", sessionOwner)).append(LOCK_PART_SEP);
            return sb.toString();
        }

        public static byte[] serializeClientId(String clientId) {
            return clientId.getBytes(UTF_8);
        }

        public static String deserializeClientId(byte[] data) {
            return new String(data, UTF_8);
        }

        public static String getLockIdFromPath(String path) {
            // We only care about our actual id since we want to compare ourselves to siblings.
            if (path.contains("/")) {
                return path.substring(path.lastIndexOf("/") + 1);
            } else {
                return path;
            }
        }

        static final Comparator<String> MEMBER_COMPARATOR = new Comparator<String>() {
            public int compare(String o1, String o2) {
                int l1 = parseMemberID(o1);
                int l2 = parseMemberID(o2);
                return l1 - l2;
            }
        };

        static enum State {
            INIT,      // initialized state
            PREPARING, // preparing to lock, but no lock node created
            PREPARED,  // lock node created
            CLAIMED,   // claim lock ownership
            WAITING,   // waiting for the ownership
            EXPIRED,   // lock is expired
            CLOSED,    // lock is closed
        }

        private final ZooKeeperClient zkClient;
        private final ZooKeeper zk;
        private final String lockPath;
        // Identify a unique lock
        private final Pair<String, Long> lockId;
        private final boolean allowSiblingLock;
        private volatile State lockState;

        private final CountDownLatch syncPoint;
        private String currentId;
        private String currentNode;
        private String watchedNode;
        private LockWatcher watcher;
        private final AtomicInteger epoch = new AtomicInteger(0);
        private final OrderedSafeExecutor lockStateExecutor;
        private final LockListener lockListener;

        public DistributedLock(ZooKeeperClient zkClient, String lockPath, String clientId,
                               OrderedSafeExecutor lockStateExecutor, LockListener lockListener)
                throws IOException {
            this(zkClient, lockPath, clientId, true, lockStateExecutor, lockListener);
        }

        /**
         * Creates a distributed lock using the given {@code zkClient} to coordinate locking.
         *
         * @param zkClient The ZooKeeper client to use.
         * @param lockPath The path used to manage the lock under.
         * @param clientId client id use for lock.
         * @param allowSiblingLock allow acquiring lock if sibling node is from same zk session id & using same lock id
         * @param lockStateExecutor executor to execute all lock state changes.
         * @param lockListener listener on lock state change.
         */
        public DistributedLock(ZooKeeperClient zkClient, String lockPath, String clientId, boolean allowSiblingLock,
                               OrderedSafeExecutor lockStateExecutor, LockListener lockListener)
                throws IOException {
            this.zkClient = zkClient;
            try {
                this.zk = zkClient.get();
            } catch (ZooKeeperClient.ZooKeeperConnectionException zce) {
                throw new IOException("Failed to get zookeeper client for lock " + lockPath);
            } catch (InterruptedException e) {
                throw new DLInterruptedException("Interrupted on getting zookeeper client for lock " + lockPath, e);
            }
            this.lockPath = lockPath;
            this.lockId = Pair.of(clientId, this.zk.getSessionId());
            this.allowSiblingLock = allowSiblingLock;
            this.lockStateExecutor = lockStateExecutor;
            this.lockListener = lockListener;
            this.syncPoint = new CountDownLatch(1);
            this.lockState = State.INIT;
        }

        @VisibleForTesting
        AtomicInteger getEpoch() {
            return epoch;
        }

        @VisibleForTesting
        State getLockState() {
            return lockState;
        }

        @VisibleForTesting
        Pair<String, Long> getLockId() {
            return lockId;
        }

        boolean isExpiredOrClosed() {
            return State.CLOSED == lockState || State.EXPIRED == lockState;
        }

        /**
         * Execute a lock action of a given <i>lockEpoch</i> in ordered safe way.
         *
         * @param lockEpoch
         *          lock epoch
         * @param func
         *          function to execute a lock action
         */
        protected void executeLockAction(final int lockEpoch, final LockAction func) {
            lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                @Override
                public void safeRun() {
                    if (DistributedLock.this.epoch.get() == lockEpoch) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("{} executing lock action '{}' under epoch {} for lock {}",
                                    new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                        }
                        func.execute();
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("{} executed lock action '{}' under epoch {} for lock {}",
                                    new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                        }
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("{} skipped executing lock action '{}' for lock {}, since epoch is changed from {} to {}.",
                                    new Object[]{lockId, func.getActionName(), lockPath, lockEpoch, DistributedLock.this.epoch.get()});
                        }
                    }
                }
            });
        }

        /**
         * Execute a lock action of a given <i>lockEpoch</i> in ordered safe way. If the lock action couln't be
         * executed due to epoch changed, fail the given <i>promise</i> with
         * {@link com.twitter.distributedlog.v2.DistributedReentrantLock.EpochChangedException}
         *
         * @param lockEpoch
         *          lock epoch
         * @param func
         *          function to execute a lock action
         * @param promise
         *          promise
         */
        protected <T> void executeLockAction(final int lockEpoch, final LockAction func, final Promise<T> promise) {
            lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                @Override
                public void safeRun() {
                    int currentEpoch = DistributedLock.this.epoch.get();
                    if (currentEpoch == lockEpoch) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("{} executed lock action '{}' under epoch {} for lock {}",
                                    new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                        }
                        func.execute();
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("{} executed lock action '{}' under epoch {} for lock {}",
                                    new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                        }
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("{} skipped executing lock action '{}' for lock {}, since epoch is changed from {} to {}.",
                                    new Object[]{lockId, func.getActionName(), lockPath, lockEpoch, currentEpoch});
                        }
                        promise.setException(new EpochChangedException(lockPath, lockEpoch, currentEpoch));
                    }
                }
            });
        }

        /**
         * Parse member id generated by zookeeper from given <i>nodeName</i>
         *
         * @param nodeName
         *          lock node name
         * @return member id generated by zookeeper
         */
        static int parseMemberID(String nodeName) {
            int id = -1;
            String[] parts = nodeName.split("_");
            if (parts.length > 0) {
                try {
                    id = Integer.parseInt(parts[parts.length - 1]);
                } catch (NumberFormatException nfe) {
                    // make it to be MAX_VALUE, so the bad znode will never acquire the lock
                    id = Integer.MAX_VALUE;
                }
            }
            return id;
        }

        /**
         * Get client id and its ephemeral owner.
         *
         * @param zkClient
         *          zookeeper client
         * @param lockPath
         *          lock path
         * @param nodeName
         *          node name
         * @return client id and its ephemeral owner.
         */
        static Future<Pair<String, Long>> asyncParseClientID(ZooKeeper zkClient, String lockPath, String nodeName) {
            String[] parts = nodeName.split("_");
            // member_<clientid>_s<owner_session>_
            if (4 == parts.length && parts[2].startsWith("s")) {
                long sessionOwner = Long.parseLong(parts[2].substring(1));
                String clientId;
                try {
                    clientId = URLDecoder.decode(parts[1], UTF_8.name());
                    return Future.value(Pair.of(clientId, sessionOwner));
                } catch (UnsupportedEncodingException e) {
                    // if failed to parse client id, we have to get client id by zookeeper#getData.
                }
            }
            final Promise<Pair<String, Long>> promise = new Promise<Pair<String, Long>>();
            zkClient.getData(lockPath + "/" + nodeName, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                    } else {
                        promise.setValue(Pair.of(deserializeClientId(data), stat.getEphemeralOwner()));
                    }
                }
            }, null);
            return promise;
        }

        private Future<String> asyncTryLock(long timeout, TimeUnit timeUnit) {
            final Promise<String> result = new Promise<String>();
            final boolean wait = DistributedLogConstants.LOCK_IMMEDIATE != timeout;
            if (wait) {
                asyncTryLock(wait, result);
            } else {
                // try to check locks first
                zk.getChildren(lockPath, null, new AsyncCallback.Children2Callback() {
                    @Override
                    public void processResult(final int rc, String path, Object ctx,
                                              final List<String> children, Stat stat) {
                        lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                            @Override
                            public void safeRun() {
                                if (State.INIT != lockState) {
                                    result.setException(new LockStateChangedException(lockPath, lockId, State.INIT, lockState));
                                    return;
                                }
                                if (KeeperException.Code.OK.intValue() != rc) {
                                    result.setException(KeeperException.create(KeeperException.Code.get(rc)));
                                    return;
                                }
                                Collections.sort(children, MEMBER_COMPARATOR);
                                if (children.size() > 0) {
                                    asyncParseClientID(zk, lockPath, children.get(0)).addEventListener(
                                    new FutureEventListener<Pair<String, Long>>() {
                                        @Override
                                        public void onSuccess(Pair<String, Long> owner) {
                                            checkOrClaimLockOwner(owner, result);
                                        }

                                        @Override
                                        public void onFailure(final Throwable cause) {
                                            lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                                                @Override
                                                public void safeRun() {
                                                    result.setException(cause);
                                                }
                                            });
                                        }
                                    });
                                } else {
                                    asyncTryLock(wait, result);
                                }
                            }
                        });
                    }
                }, null);
            }
            return result;
        }

        private void checkOrClaimLockOwner(final Pair<String, Long> currentOwner,
                                           final Promise<String> result) {
            if (lockId.compareTo(currentOwner) != 0) {
                lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        result.setValue(currentOwner.getLeft());
                    }
                });
                return;
            }
            // current owner is itself
            final int curEpoch = epoch.incrementAndGet();
            executeLockAction(curEpoch, new LockAction() {
                @Override
                public void execute() {
                    if (State.INIT != lockState) {
                        result.setException(new LockStateChangedException(lockPath, lockId, State.INIT, lockState));
                        return;
                    }
                    asyncTryLock(false, result);
                }
                @Override
                public String getActionName() {
                    return "claimOwnership(owner=" + currentOwner + ")";
                }
            }, result);
        }

        /**
         * Try lock. If it failed, it would cleanup its attempt.
         *
         * @param wait
         *          whether to wait for ownership.
         * @param result
         *          promise to satisfy with current lock owner
         */
        private void asyncTryLock(boolean wait, final Promise<String> result) {
            final Promise<String> lockResult = new Promise<String>();
            lockResult.addEventListener(new FutureEventListener<String>() {
                @Override
                public void onSuccess(String currentOwner) {
                    result.setValue(currentOwner);
                }

                @Override
                public void onFailure(final Throwable lockCause) {
                    // if tryLock failed due to state changed, we don't need to cleanup
                    if (lockCause instanceof LockStateChangedException) {
                        result.setException(lockCause);
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} is cleaning up its lock state for {} due to : ",
                                  new Object[] { lockId, lockPath, lockCause });
                    }
                    // if encountered any exception
                    // we should cleanup
                    Promise<BoxedUnit> cleanupResult = new Promise<BoxedUnit>();
                    cleanupResult.addEventListener(new FutureEventListener<BoxedUnit>() {
                        @Override
                        public void onSuccess(BoxedUnit value) {
                            result.setException(lockCause);
                        }
                        @Override
                        public void onFailure(Throwable cause) {
                            result.setException(lockCause);
                        }
                    });
                    cleanup(cleanupResult);
                }
            });
            asyncTryLockWithoutCleanup(wait, lockResult);
        }

        /**
         * Try lock. If wait is true, it would wait and watch sibling to acquire lock when
         * the sibling is dead. <i>syncPoint</i> will be notified either it locked successfully
         * or the lock failed. The promise will only satisfy with current lock owner.
         *
         * NOTE: the <i>promise</i> is only satisfied on <i>lockStateExecutor</i>, so any
         * transformations attached on promise will be executed in order.
         *
         * @param wait
         *          whether to wait for ownership.
         * @param promise
         *          promise to satisfy with current lock owner.
         */
        private void asyncTryLockWithoutCleanup(final boolean wait, final Promise<String> promise) {
            executeLockAction(epoch.get(), new LockAction() {
                @Override
                public void execute() {
                    if (State.INIT != lockState) {
                        promise.setException(new LockStateChangedException(lockPath, lockId, State.INIT, lockState));
                        return;
                    }
                    lockState = State.PREPARING;

                    final int curEpoch = epoch.incrementAndGet();
                    watcher = new LockWatcher(curEpoch);
                    // register watcher for session expires
                    zkClient.register(watcher);
                    // Encode both client id and session in the lock node
                    String myPath;
                    try {
                        // member_<clientid>_s<owner_session>_
                        myPath = getLockPathPrefixV3(lockPath, lockId.getLeft(), lockId.getRight());
                    } catch (UnsupportedEncodingException uee) {
                        myPath = getLockPathPrefixV1(lockPath);
                    }
                    zk.create(myPath, serializeClientId(lockId.getLeft()), zkClient.getDefaultACL(), CreateMode.EPHEMERAL_SEQUENTIAL,
                            new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(final int rc, String path, Object ctx, final String name) {
                            executeLockAction(curEpoch, new LockAction() {
                                @Override
                                public void execute() {
                                    if (KeeperException.Code.OK.intValue() != rc) {
                                        KeeperException ke = KeeperException.create(KeeperException.Code.get(rc));
                                        promise.setException(ke);
                                        return;
                                    }
                                    currentNode = name;
                                    currentId = getLockIdFromPath(currentNode);
                                    LOG.trace("{} received member id for lock {} : ", lockId, currentId);
                                    // Complete preparation
                                    lockState = State.PREPARED;
                                    checkLockOwnerAndWaitIfPossible(watcher, wait, promise);
                                }

                                @Override
                                public String getActionName() {
                                    return "postPrepare(wait=" + wait + ")";
                                }
                            });
                        }
                    }, null);
                }
                @Override
                public String getActionName() {
                    return "prepare(wait=" + wait + ")";
                }
            }, promise);
        }

        public boolean tryLock(long timeout, TimeUnit unit) throws LockingException {
            return waitForLock(asyncTryLock(timeout, unit), timeout, unit);
        }

        public synchronized boolean waitForLock(Future<String> lockFuture, long timeout, TimeUnit unit)
                throws LockingException {
            boolean success = false;
            boolean stateChanged = false;
            try {
                String owner;
                try {
                    owner = Await.result(lockFuture);
                } catch (Exception e) {
                    if (e instanceof LockingException) {
                        stateChanged = e instanceof LockStateChangedException;
                        throw (LockingException) e;
                    } else {
                        throw new LockingException(lockPath, lockId + " failed to lock " + lockPath, e);
                    }
                }
                try {
                    if (DistributedLogConstants.LOCK_IMMEDIATE != timeout) {
                        if (DistributedLogConstants.LOCK_TIMEOUT_INFINITE == timeout) {
                            syncPoint.await();
                        }
                        else {
                            if (!syncPoint.await(timeout, unit)) {
                                LOG.debug("Failed on tryLocking {} in {} ms.", lockPath, unit.toMillis(timeout));
                            }
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                success = isLockHeld();
                if (!success) {
                    throw new OwnershipAcquireFailedException(lockPath, owner);
                }
            } finally {
                if (!success && !stateChanged) {
                    unlock();
                }
            }
            return true;
        }

        public synchronized void unlock() {
            Promise<BoxedUnit> cleanupResult = new Promise<BoxedUnit>();
            cleanup(cleanupResult);
            try {
                Await.result(cleanupResult);
            } catch (Exception e) {
                LOG.warn("{} failed to unlock {} : ", new Object[] { lockId, lockPath, e });
            }
        }

        // Lock State Changes (all state changes should be executed under a LockAction)

        private void claimOwnership(int lockEpoch) {
            lockState = State.CLAIMED;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Notify lock waiters on {} at {} : watcher epoch {}, lock epoch {}",
                          new Object[] { lockPath, System.currentTimeMillis(),
                                  lockEpoch, DistributedLock.this.epoch.get() });
            }
            syncPoint.countDown();
        }

        public boolean isLockHeld() {
            return State.CLAIMED == lockState;
        }

        /**
         * NOTE: cleanup should only after try lock.
         */
        private void cleanup(Promise<BoxedUnit> promise) {
            // already closed or expired, nothing to cleanup
            this.epoch.incrementAndGet();
            if (null != watcher) {
                this.zkClient.unregister(watcher);
            }
            if (State.CLOSED == lockState || State.EXPIRED == lockState) {
                promise.setValue(BoxedUnit.UNIT);
                return;
            }

            LOG.info("Lock {} for {} is closed from state {}.",
                    new Object[] { lockId, lockPath, lockState });
            if (State.INIT == lockState) {
                // nothing to cleanup
                lockState = State.CLOSED;
                promise.setValue(BoxedUnit.UNIT);
                return;
            }

            lockState = State.CLOSED;
            // in any other state, we should clean the member node
            deleteLockNode(promise);
        }

        private void deleteLockNode(final Promise<BoxedUnit> promise) {
            if (null == currentNode) {
                promise.setValue(BoxedUnit.UNIT);
                return;
            }
            zk.delete(currentNode, -1, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(final int rc, final String path, Object ctx) {
                    lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                        @Override
                        public void safeRun() {
                            if (KeeperException.Code.OK.intValue() == rc ||
                                    KeeperException.Code.NONODE.intValue() == rc ||
                                    KeeperException.Code.SESSIONEXPIRED.intValue() == rc) {
                                LOG.info("Deleted lock node {} for {} successfully.", path, lockId);
                            } else {
                                LOG.warn("Failed on deleting lock node {} for {} : {}",
                                        new Object[] { path, lockId, KeeperException.Code.get(rc) });
                            }
                            promise.setValue(BoxedUnit.UNIT);
                        }
                    });
                }
            }, null);
        }

        /**
         * Handle session expired for lock watcher at epoch <i>lockEpoch</i>.
         *
         * @param lockEpoch
         *          lock epoch
         */
        private void handleSessionExpired(final int lockEpoch) {
            executeLockAction(lockEpoch, new LockAction() {
                @Override
                public void execute() {
                    boolean shouldNotifyLockListener = State.CLAIMED == lockState;

                    lockState = State.EXPIRED;
                    // remove the watcher
                    if (null != watcher) {
                        zkClient.unregister(watcher);
                    }
                    // increment epoch to avoid any ongoing locking action
                    DistributedLock.this.epoch.incrementAndGet();
                    // if session expired, just notify the waiter. as the lock acquire doesn't succeed.
                    // we don't even need to clean up the lock as the znode will disappear after session expired
                    syncPoint.countDown();

                    if (shouldNotifyLockListener) {
                        // if session expired after claimed, we need to notify the caller to re-lock
                        if (null != lockListener) {
                            lockListener.onExpired();
                        }
                    }
                }

                @Override
                public String getActionName() {
                    return "handleSessionExpired(epoch=" + lockEpoch + ")";
                }
            });
        }

        private void handleNodeDelete(int lockEpoch, final WatchedEvent event) {
            executeLockAction(lockEpoch, new LockAction() {
                @Override
                public void execute() {
                    // The lock is either expired or closed
                    if (State.WAITING != lockState) {
                        LOG.info("{} ignore watched node {} deleted event, since lock state has moved to {}.",
                                 new Object[] { lockId, event.getPath(), lockState });
                        return;
                    }
                    lockState = State.PREPARED;
                    // we don't need to wait and check the result, since:
                    // 1) if it claimed the ownership, it would notify the waiters when claimed ownerships
                    // 2) if it failed, it would also notify the waiters, the waiters would cleanup the state.
                    checkLockOwnerAndWaitIfPossible(watcher, true);
                }

                @Override
                public String getActionName() {
                    return "handleNodeDelete(path=" + event.getPath() + ")";
                }
            });
        }

        private Future<String> checkLockOwnerAndWaitIfPossible(final LockWatcher lockWatcher,
                                                               final boolean wait) {
            final Promise<String> promise = new Promise<String>();
            checkLockOwnerAndWaitIfPossible(lockWatcher, wait, promise);
            return promise;
        }

        /**
         * Check Lock Owner Phase 1 : Get all lock waiters.
         *
         * @param lockWatcher
         *          lock watcher.
         * @param wait
         *          whether to wait for ownership.
         * @param promise
         *          promise to satisfy with current lock owner
         */
        private void checkLockOwnerAndWaitIfPossible(final LockWatcher lockWatcher,
                                                     final boolean wait,
                                                     final Promise<String> promise) {
            zk.getChildren(lockPath, false, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                    processLockWaiters(lockWatcher, wait, rc, children, promise);
                }
            }, null);
        }

        /**
         * Check Lock Owner Phase 2 : check all lock waiters to get current owner and wait for ownership if necessary.
         *
         * @param lockWatcher
         *          lock watcher.
         * @param wait
         *          whether to wait for ownership.
         * @param getChildrenRc
         *          result of getting all lock waiters
         * @param children
         *          current lock waiters.
         * @param promise
         *          promise to satisfy with current lock owner.
         */
        private void processLockWaiters(final LockWatcher lockWatcher,
                                        final boolean wait,
                                        final int getChildrenRc,
                                        final List<String> children,
                                        final Promise<String> promise) {
            executeLockAction(lockWatcher.epoch, new LockAction() {
                @Override
                public void execute() {
                    if (State.PREPARED != lockState) { // e.g. lock closed or session expired after prepared
                        promise.setException(new LockStateChangedException(lockPath, lockId, State.PREPARED, lockState));
                        return;
                    }

                    if (KeeperException.Code.OK.intValue() != getChildrenRc) {
                        promise.setException(KeeperException.create(KeeperException.Code.get(getChildrenRc)));
                        return;
                    }
                    if (children.isEmpty()) {
                        LOG.error("Error, memeber list is empty for lock {}.", lockPath);
                        promise.setException(new UnexpectedException("Empty member list for lock " + lockPath));
                        return;
                    }

                    // sort the children
                    Collections.sort(children, MEMBER_COMPARATOR);
                    final String cid = currentId;
                    final int memberIndex = children.indexOf(cid);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} is the number {} member in the list.", cid, memberIndex);
                    }
                    // If we hold the lock
                    if (memberIndex == 0) {
                        LOG.info("{} acquired the lock {}.", cid, lockPath);
                        claimOwnership(lockWatcher.epoch);
                        promise.setValue(cid);
                    } else if (memberIndex > 0) { // we are in the member list but we didn't hold the lock
                        // get ownership of current owner
                        asyncParseClientID(zk, lockPath, children.get(0)).addEventListener(new FutureEventListener<Pair<String, Long>>() {
                            @Override
                            public void onSuccess(Pair<String, Long> currentOwner) {
                                watchLockOwner(lockWatcher, wait,
                                        cid, children.get(memberIndex - 1), children.get(0), currentOwner, promise);
                            }
                            @Override
                            public void onFailure(final Throwable cause) {
                                // ensure promise is satisfied in lock thread
                                executeLockAction(lockWatcher.epoch, new LockAction() {
                                    @Override
                                    public void execute() {
                                        promise.setException(cause);
                                    }

                                    @Override
                                    public String getActionName() {
                                        return "handleFailureOnParseClientID(lockPath=" + lockPath + ")";
                                    }
                                }, promise);
                            }
                        });
                    } else {
                        LOG.error("Member {} doesn't exist in the members list {} for lock {}.",
                                new Object[]{ cid, children, lockPath});
                        promise.setException(
                                new UnexpectedException("Member " + cid + " doesn't exist in member list " +
                                        children + " for lock " + lockPath));
                    }
                }

                @Override
                public String getActionName() {
                    return "processLockWaiters(rc=" + getChildrenRc + ", waiters=" + children + ")";
                }
            }, promise);
        }

        /**
         * Check Lock Owner Phase 3: watch sibling node for lock ownership.
         *
         * @param lockWatcher
         *          lock watcher.
         * @param wait
         *          whether to wait for ownership.
         * @param myNode
         *          my lock node.
         * @param siblingNode
         *          my sibling lock node.
         * @param ownerNode
         *          owner lock node.
         * @param currentOwner
         *          current owner info.
         * @param promise
         *          promise to satisfy with current lock owner.
         */
        private void watchLockOwner(final LockWatcher lockWatcher,
                                    final boolean wait,
                                    final String myNode,
                                    final String siblingNode,
                                    final String ownerNode,
                                    final Pair<String, Long> currentOwner,
                                    final Promise<String> promise) {
            executeLockAction(lockWatcher.epoch, new LockAction() {
                @Override
                public void execute() {
                    boolean shouldWatch;
                    if (allowSiblingLock && lockId.compareTo(currentOwner) == 0 && siblingNode.equals(ownerNode)) {
                        // I found that my sibling is the current owner with same lock id (client id & session id)
                        // It must be left by any race condition from same zookeeper client
                        // I would watch owner instead of sibling
                        shouldWatch = true;
                        LOG.info("LockWatcher {} for {} found itself {} already held lock at sibling node {}, watch it to claim ownership.",
                                new Object[]{myNode, lockPath, lockId, siblingNode});
                    } else {
                        shouldWatch = wait;
                        if (wait) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Current LockWatcher for {} with ephemeral node {}, is waiting for {} to release lock at {}.",
                                        new Object[]{lockPath, myNode, siblingNode, System.currentTimeMillis()});
                            }
                        }
                    }

                    // watch sibling for lock ownership
                    if (shouldWatch) {
                        watchedNode = String.format("%s/%s", lockPath, siblingNode);
                        zk.exists(watchedNode, lockWatcher, new AsyncCallback.StatCallback() {
                            @Override
                            public void processResult(final int rc, String path, Object ctx, final Stat stat) {
                                executeLockAction(lockWatcher.epoch, new LockAction() {
                                    @Override
                                    public void execute() {
                                        if (KeeperException.Code.OK.intValue() == rc) {
                                            if (lockId.compareTo(currentOwner) == 0 && siblingNode.equals(ownerNode)) {
                                                // watch owner successfully
                                                claimOwnership(lockWatcher.epoch);
                                                LOG.info("LockWatcher {} claimed ownership for {} after set watcher on {}.",
                                                        new Object[]{ myNode, lockPath, ownerNode });
                                                promise.setValue(currentOwner.getLeft());
                                            } else {
                                                // watch sibling successfully
                                                lockState = State.WAITING;
                                                promise.setValue(currentOwner.getLeft());
                                            }
                                        } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                            // sibling just disappeared, it might be the chance to claim ownership
                                            checkLockOwnerAndWaitIfPossible(lockWatcher, wait, promise);
                                        } else {
                                            promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                                        }
                                    }

                                    @Override
                                    public String getActionName() {
                                        StringBuilder sb = new StringBuilder();
                                        sb.append("postWatchLockOwner(myNode=").append(myNode).append(", siblingNode=")
                                                .append(siblingNode).append(", ownerNode=").append(ownerNode).append(")");
                                        return sb.toString();
                                    }
                                }, promise);
                            }
                        }, null);
                    } else {
                        promise.setValue(currentOwner.getLeft());
                    }
                }

                @Override
                public String getActionName() {
                    StringBuilder sb = new StringBuilder();
                    sb.append("watchLockOwner(myNode=").append(myNode).append(", siblingNode=")
                            .append(siblingNode).append(", ownerNode=").append(ownerNode).append(")");
                    return sb.toString();
                }
            }, promise);
        }

        class LockWatcher implements Watcher {

            // Enforce a epoch number to avoid a race on canceling attempt
            final int epoch;

            LockWatcher(int epoch) {
                this.epoch = epoch;
            }

            @Override
            public void process(WatchedEvent event) {
                LOG.info("Received event {} from lock {} at {} : watcher epoch {}, lock epoch {}.",
                          new Object[] { event, lockPath, System.currentTimeMillis(), epoch, DistributedLock.this.epoch.get() });
                if (event.getType() == Watcher.Event.EventType.None) {
                    switch (event.getState()) {
                        case SyncConnected:
                            break;
                        case Expired:
                            handleSessionExpired(epoch);
                            break;
                        default:
                            break;
                    }
                } else if (event.getType() == Event.EventType.NodeDeleted) {
                    // this handles the case where we have aborted a lock and deleted ourselves but still have a
                    // watch on the nextLowestNode. This is a workaround since ZK doesn't support unsub.
                    if (!event.getPath().equals(watchedNode)) {
                        LOG.warn("{} (watching {}) ignored watched event from {} ",
                                 new Object[] { lockId, watchedNode, event.getPath() });
                        return;
                    }
                    handleNodeDelete(epoch, event);
                } else {
                    LOG.warn("Unexpected ZK event: {}", event.getType().name());
                }
            }

        }
    }
}
