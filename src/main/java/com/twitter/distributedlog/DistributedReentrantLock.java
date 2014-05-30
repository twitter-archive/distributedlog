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
package com.twitter.distributedlog;

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private final ScheduledExecutorService executorService;
    private final String lockPath;
    private Future<?> asyncLockAcquireFuture = null;
    private LockingException asyncLockAcquireException = null;

    private final AtomicInteger lockCount = new AtomicInteger(0);
    private final AtomicIntegerArray lockAcqTracker;
    private final DistributedLock internalLock;
    private final long lockTimeout;
    private boolean logUnbalancedWarning = true;

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
    private static OpStatsLogger prepareStats = null;
    private static OpStatsLogger checkLockStats = null;
    private static OpStatsLogger syncDeleteStats = null;
    private static OpStatsLogger asyncDeleteStats = null;

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
        ScheduledExecutorService executorService,
        ZooKeeperClient zkc,
        String lockPath,
        long lockTimeout,
        String clientId,
        StatsLogger statsLogger) throws IOException {
        this.executorService = executorService;
        this.lockPath = lockPath;
        this.lockTimeout = lockTimeout;
        this.internalLock = new DistributedLock(zkc, lockPath, clientId);
        this.lockAcqTracker = new AtomicIntegerArray(LockReason.MAXREASON.value);

        if (null == lockStatsLogger) {
            lockStatsLogger = statsLogger.scope("lock");
        }
        if (null == acquireStats) {
            acquireStats = lockStatsLogger.getOpStatsLogger("acquire");
        }
        if (null == releaseStats) {
            releaseStats = lockStatsLogger.getOpStatsLogger("release");
        }
        if (null == prepareStats) {
            prepareStats = lockStatsLogger.getOpStatsLogger("prepare");
        }
        if (null == checkLockStats) {
            checkLockStats = lockStatsLogger.getOpStatsLogger("check_lock");
        }
        if (null == syncDeleteStats) {
            syncDeleteStats = lockStatsLogger.getOpStatsLogger("sync_delete");
        }
        if (null == asyncDeleteStats) {
            asyncDeleteStats = lockStatsLogger.getOpStatsLogger("async_delete");
        }
    }

    void acquire(LockReason reason) throws LockingException {
        Stopwatch stopwatch = new Stopwatch().start();
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

    void doAcquire(LockReason reason) throws LockingException {
        LOG.trace("Lock Acquire {}, {}", lockPath, reason);
        while (true) {
            if (lockCount.get() == 0) {
                synchronized (this) {
                    if (lockCount.get() > 0) {
                        lockCount.incrementAndGet();
                        break;
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
        Stopwatch stopwatch = new Stopwatch().start();
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

    public synchronized boolean checkWriteLock(boolean acquiresync, LockReason reason) throws LockingException {
        if (!haveLock()) {
            if (null != asyncLockAcquireException) {
                throw asyncLockAcquireException;
            }
            // We may have just lost the lock because of a ZK session timeout
            // not necessarily because someone else acquired the lock.
            // In such cases just try to reacquire. If that fails, it will throw
            if (acquiresync) {
                acquire(reason);
            } else {
                asyncLockAcquire(reason);
            }
            return true;
        }

        return false;
    }

    boolean haveLock() {
        return internalLock.isLockHeld();
    }

    public void close() {
        Future<?> futureToCancel;
        synchronized (this) {
            futureToCancel = asyncLockAcquireFuture;
        }
        if (null != futureToCancel) {
            futureToCancel.cancel(true);
        }
        internalLock.cleanup(true);
    }

    private synchronized void setAsyncLockAcquireException(LockingException e) {
        this.asyncLockAcquireException = e;
    }

    private synchronized void asyncLockAcquire(final LockReason reason) {
        if (null == asyncLockAcquireFuture) {
            asyncLockAcquireFuture = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        setAsyncLockAcquireException(null);
                        acquire(reason);
                    } catch (LockingException exc) {
                        setAsyncLockAcquireException(exc);
                    } finally {
                        synchronized (this) {
                            asyncLockAcquireFuture = null;
                        }
                    }
                }
            });
        }
    }

    static class DeleteCallback implements AsyncCallback.VoidCallback {

        final Stopwatch stopwatch;

        DeleteCallback() {
            stopwatch = new Stopwatch().start();
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (KeeperException.Code.OK.intValue() == rc) {
                asyncDeleteStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                LOG.info("Deleted lock znode {} successfully!", path);
            } else {
                asyncDeleteStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                LOG.info("Deleted lock znode {} : ", path, KeeperException.create(KeeperException.Code.get(rc)));
            }
        }
    }

    /**
     * Twitter commons version with some modifications.
     * <p/>
     * TODO: Cannot use the same version to avoid a transitive dependence on
     * TODO: two different versions of zookeeper.
     * TODO: When science upgrades to ZK 3.4.X we should remove this
     */
    private static class DistributedLock {

        private final ZooKeeperClient zkClient;
        private final String lockPath;
        private final String clientId;

        private final AtomicBoolean aborted = new AtomicBoolean(false);
        private volatile CountDownLatch syncPoint;
        private volatile boolean holdsLock = false;
        private volatile String currentId;
        private volatile String currentNode;
        private volatile String watchedNode;
        private volatile String currentOwner = DistributedLogConstants.UNKNOWN_CLIENT_ID;
        private volatile LockWatcher watcher;
        private final AtomicInteger epoch = new AtomicInteger(0);

        /**
         * Creates a distributed lock using the given {@code zkClient} to coordinate locking.
         *
         * @param zkClient The ZooKeeper client to use.
         * @param lockPath The path used to manage the lock under.
         */
        public DistributedLock(ZooKeeperClient zkClient, String lockPath, String clientId) {
            this.zkClient = zkClient;
            this.lockPath = lockPath;
            this.clientId = clientId;
            this.syncPoint = new CountDownLatch(1);
        }

        private static int parseMemberID(String nodeName) {
            int id = -1;
            String[] parts = nodeName.split("_");
            if (parts.length > 0) {
                try {
                    id = Integer.parseInt(parts[parts.length - 1]);
                } catch (NumberFormatException nfe) {
                    id = -1;
                }
            }
            return id;
        }

        private String parseClientID(String nodeName)
                throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException,
                KeeperException, UnsupportedEncodingException {
            String[] parts = nodeName.split("_");

            String currentOwnerTemp;
            if (parts.length == 3) {
                currentOwnerTemp = URLDecoder.decode(parts[1], "UTF-8");
            } else {
                currentOwnerTemp = new String(
                    zkClient.get().getData(lockPath + "/" + nodeName, false, null), "UTF-8");
            }
            return currentOwnerTemp;
        }

        private List<String> getLockMembers()
                throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException,
                KeeperException {
            List<String> sortedMembers = zkClient.get().getChildren(lockPath, null);

            Collections.sort(sortedMembers, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    int l1 = parseMemberID(o1);
                    int l2 = parseMemberID(o2);
                    return l1 - l2;
                }
            });

            return sortedMembers;
        }

        private void checkOwnerIfNeed(long timeout)
                throws OwnershipAcquireFailedException, InterruptedException, ZooKeeperClient.ZooKeeperConnectionException,
                KeeperException {
            if (DistributedLogConstants.LOCK_IMMEDIATE == timeout) {
                List<String> members = getLockMembers();
                if (members.size() > 0) {
                    try {
                        String clientId = parseClientID(members.get(0));
                        throw new OwnershipAcquireFailedException(lockPath, clientId);
                    } catch (UnsupportedEncodingException e) {
                        // no idea about current owner, then fallback.
                    } catch (KeeperException.NoNodeException nne) {
                        // the owner just disappeared, then fallback.
                    }
                }
            }
        }

        private int prepare()
                throws InterruptedException, KeeperException, ZooKeeperClient.ZooKeeperConnectionException {
            Stopwatch stopwatch = new Stopwatch().start();
            boolean success = false;
            try {
                int res = doPrepare();
                success = true;
                return res;
            } finally {
                if (success) {
                    prepareStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                } else {
                    prepareStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                }
            }
        }

        private int doPrepare()
            throws InterruptedException, KeeperException, ZooKeeperClient.ZooKeeperConnectionException {

            LOG.trace("Working with locking path: {}", lockPath);

            // Increase the epoch each time acquire the lock
            int curEpoch = epoch.incrementAndGet();

            // Create an EPHEMERAL_SEQUENTIAL node.
            try {
                currentNode =
                    zkClient.get().create(lockPath + "/member_" + URLEncoder.encode(clientId, "UTF-8") + "_",
                            clientId.getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } catch (UnsupportedEncodingException e) {
                currentNode =
                    zkClient.get().create(lockPath + "/member_",
                            clientId.getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            }

            // We only care about our actual id since we want to compare ourselves to siblings.
            if (currentNode.contains("/")) {
                currentId = currentNode.substring(currentNode.lastIndexOf("/") + 1);
            }
            LOG.trace("Received ID from zk: {}", currentId);
            this.watcher = new LockWatcher(curEpoch);
            return curEpoch;
        }

        public boolean tryLock(long timeout, TimeUnit unit) throws LockingException {
            if (holdsLock) {
                throw new LockingException(lockPath, "Error, already holding a lock. Call unlock first!");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Try locking {} at {}.", lockPath, System.currentTimeMillis());
            }
            try {
                checkOwnerIfNeed(timeout);
                prepare();
                // only wait the lock for non-immediate lock
                watcher.checkForLock(DistributedLogConstants.LOCK_IMMEDIATE != timeout);
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
                if (!holdsLock) {
                    throw new OwnershipAcquireFailedException(lockPath, currentOwner);
                }
            } catch (InterruptedException e) {
                cancelAttempt(true, this.epoch.get());
                return false;
            } catch (KeeperException e) {
                LOG.error("ZooKeeper Exception while trying to acquire lock {} : ", lockPath, e);
                // No need to clean up since the node wasn't created yet.
                throw new LockingException(lockPath, "ZooKeeper Exception while trying to acquire lock " + lockPath, e);
            } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                // No need to clean up since the node wasn't created yet.
                throw new LockingException(lockPath, "ZooKeeper Exception while trying to acquire lock " + lockPath, e);
            } finally {
                if (!holdsLock) {
                    cancelAttempt(false, this.epoch.get());
                }
            }
            return true;
        }

        public void unlock() {
            if (currentId == null) {
                LOG.error("Error, neither attempting to lock nor holding a lock! {}", lockPath);
                return;
            }
            // Try aborting!
            if (!holdsLock) {
                aborted.set(true);
                LOG.info("Not holding lock, aborting acquisition attempt!");
            } else {
                LOG.trace("Cleaning up this locks ephemeral node. {} {}", currentId, currentNode);
                cleanup(true);
            }
        }

        private String getCurrentId() {
            return currentId;
        }

        public boolean isLockHeld() {
            return holdsLock;
        }

        private void cancelAttempt(boolean sync, int epoch) {
            if (this.epoch.getAndIncrement() == epoch) {
                LOG.info("Cancelling lock attempt!");
                cleanup(sync);
                // Bubble up failure...
                holdsLock = false;
                syncPoint.countDown();
            }
        }

        private void cleanup(boolean sync) {
            LOG.trace("Cleaning up!");
            String nodeToDelete = currentNode;
            try {
                if (null != nodeToDelete) {
                    if (sync) {
                        Stopwatch stopwatch = new Stopwatch().start();
                        boolean success = false;
                        try {
                            Stat stat = zkClient.get().exists(nodeToDelete, false);
                            if (stat != null) {
                                zkClient.get().delete(nodeToDelete, -1);
                            } else {
                                LOG.warn("Called cleanup but nothing to cleanup!");
                            }
                            success = true;
                        } finally {
                            if (success) {
                                syncDeleteStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                            } else {
                                syncDeleteStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                            }
                        }

                    } else {
                        zkClient.get().delete(nodeToDelete, -1, new DeleteCallback(), null);
                    }
                }
            } catch (KeeperException e) {
                LOG.warn("Failed to clean up lock node {} : ", nodeToDelete, e);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted on cleaning up lock node {} : ", nodeToDelete, e);
            } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                LOG.warn("Lost zookeeper connection on cleaning up node {} : ", nodeToDelete, e);
            }
            holdsLock = false;
            aborted.set(false);
            currentId = null;
            currentNode = null;
            watcher = null;
            syncPoint = new CountDownLatch(1);
        }

        class LockWatcher implements Watcher, AsyncCallback.VoidCallback {

            // Enforce a epoch number to avoid a race on canceling attempt
            final int epoch;

            LockWatcher(int epoch) {
                this.epoch = epoch;
            }

            public void checkForLock(boolean wait) {
                Stopwatch stopwatch = new Stopwatch().start();
                boolean success = false;
                try {
                    doCheckForLock(wait);
                    success = true;
                } finally {
                    if (success) {
                        checkLockStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                    } else {
                        checkLockStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                    }
                }
            }

            void asyncCheckForLock(final boolean wait, final AsyncCallback.VoidCallback callback, Object ctx) {
                try {
                    zkClient.get().getChildren(lockPath, false, new AsyncCallback.Children2Callback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                            if (KeeperException.Code.OK.intValue() != rc) {
                                callback.processResult(rc, path, ctx);
                                return;
                            }
                            if (children.isEmpty()) {
                                LOG.error("Error, memeber list is empty for lock {}.", lockPath);
                                callback.processResult(KeeperException.Code.RUNTIMEINCONSISTENCY.intValue(), path, ctx);
                                return;
                            }
                            // sort the children
                            Collections.sort(children, new Comparator<String>() {
                                public int compare(String o1, String o2) {
                                    int l1 = parseMemberID(o1);
                                    int l2 = parseMemberID(o2);
                                    return l1 - l2;
                                }
                            });
                            String cid = getCurrentId();
                            int memberIndex = children.indexOf(cid);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("{} is the number {} member in the list.", cid, memberIndex);
                            }
                            // If we hold the lock
                            if (memberIndex == 0) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("{} acquired the lock {}.", cid, lockPath);
                                }
                                holdsLock = true;
                                callback.processResult(rc, path, ctx);
                            } else if (memberIndex > 0) {
                                String[] parts = children.get(0).split("_");
                                if (parts.length != 3) {
                                    callback.processResult(KeeperException.Code.RUNTIMEINCONSISTENCY.intValue(), path, ctx);
                                    return;
                                }
                                String currentOwnerTemp;
                                try {
                                    currentOwnerTemp = URLDecoder.decode(parts[1], "UTF-8");
                                } catch (UnsupportedEncodingException e) {
                                    callback.processResult(KeeperException.Code.RUNTIMEINCONSISTENCY.intValue(), path, ctx);
                                    return;
                                }
                                currentOwner = currentOwnerTemp;

                                if (wait) {
                                    final String nextLowestNode = children.get(memberIndex - 1);
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Current LockWatcher for {} with ephemeral node {}, is waiting for {} to release lock at {}.",
                                                  new Object[] { lockPath, cid, nextLowestNode, System.currentTimeMillis() });
                                    }
                                    watchedNode = String.format("%s/%s", lockPath, nextLowestNode);
                                    try {
                                        zkClient.get().exists(watchedNode, LockWatcher.this, new StatCallback() {
                                            @Override
                                            public void processResult(int rc, String path, Object ctx, Stat stat) {
                                                if (KeeperException.Code.OK.intValue() == rc) {
                                                    callback.processResult(rc, path, ctx);
                                                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                                    asyncCheckForLock(wait, callback, ctx);
                                                } else {
                                                    callback.processResult(rc, path, ctx);
                                                }
                                            }
                                        }, ctx);
                                    } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                                        callback.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
                                    } catch (InterruptedException e) {
                                        callback.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
                                    }
                                }
                            } else {
                                LOG.error("Member {} doesn't exist in the members list {}.", cid, children);
                                callback.processResult(KeeperException.Code.RUNTIMEINCONSISTENCY.intValue(), path, ctx);
                            }
                        }
                    }, ctx);
                } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                    callback.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), lockPath, ctx);
                } catch (InterruptedException e) {
                    callback.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), lockPath, ctx);
                }
            }

            void doCheckForLock(boolean wait) {
                try {
                    List<String> sortedMembers = getLockMembers();

                    // Unexpected behavior if there are no children!
                    if (sortedMembers.isEmpty()) {
                        // TODO: changed to unexpected exception in dlog proxy
                        throw new IOException("Error, member list is empty!");
                    }

                    int memberIndex = sortedMembers.indexOf(getCurrentId());

                    // If we hold the lock
                    if (memberIndex == 0) {
                        synchronized (DistributedLock.this) {
                            holdsLock = true;
                            syncPoint.countDown();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("{} claimed ownership on lock {} at {}.",
                                          new Object[] { getCurrentId(), lockPath, System.currentTimeMillis() });
                            }
                        }
                    } else if (memberIndex > 0) {
                        String currentOwnerTemp = parseClientID(sortedMembers.get(0));

                        // Accessed concurrently in the thread that acquires the lock
                        // checkForLock can be called by background threads as well
                        synchronized(DistributedLock.this) {
                            currentOwner = currentOwnerTemp;
                        }

                        if (wait) {
                            final String nextLowestNode = sortedMembers.get(memberIndex - 1);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Current LockWatcher for {} with ephemeral node {}, is waiting for {} to release lock at {}.",
                                          new Object[] { lockPath, getCurrentId(), nextLowestNode, System.currentTimeMillis() });
                            }
                            watchedNode = String.format("%s/%s", lockPath, nextLowestNode);
                            Stat stat = zkClient.get().exists(watchedNode, this);
                            if (stat == null) {
                                checkForLock(true);
                            }
                        }
                    } else {
                        throw new IOException("Error, current id " + getCurrentId() + " is not in the members list.");
                    }
                } catch (InterruptedException e) {
                    handleException("interrupted", e);
                } catch (KeeperException e) {
                    handleException("an exception while accessing ZK", e);
                } catch (UnsupportedEncodingException e) {
                    handleException("invalid ownership data", e);
                } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                    handleException("zookeeper connection lost", e);
                } catch (IOException e) {
                    handleException("unexpected empty memberlist", e);
                }
            }

            private void handleException(String reason, Exception e) {
                if (DistributedLock.this.epoch.get() == epoch) {
                    LOG.warn("Current LockWatcher with ephemeral node {} got {}. Trying to cancel lock acquisition.",
                             new Object[] { getCurrentId(), reason, e });
                    cancelAttempt(true, epoch);
                }
            }

            @Override
            public void process(WatchedEvent event) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received event {} from lock {} at {} : watcher epoch {}, lock epoch {}.",
                              new Object[] { event, lockPath, System.currentTimeMillis(), epoch, DistributedLock.this.epoch.get() });
                }
                // this handles the case where we have aborted a lock and deleted ourselves but still have a
                // watch on the nextLowestNode. This is a workaround since ZK doesn't support unsub.
                if (!event.getPath().equals(watchedNode)) {
                    LOG.debug("Ignoring call for node:" + watchedNode);
                    return;
                }
                if (event.getType() == Watcher.Event.EventType.None) {
                    switch (event.getState()) {
                        case SyncConnected:
                            LOG.info("Reconnected...");
                            break;
                        case Expired:
                            LOG.warn(String.format("Current ZK session expired![%s]", getCurrentId()));
                            // if session expired, just notify the waiter. as the lock acquire doesn't succeed,
                            // the caller of acquire will clean up the lock.
                            if (DistributedLock.this.epoch.get() == epoch) {
                                syncPoint.countDown();
                            }
                            break;
                        default:
                            break;
                    }
                } else if (event.getType() == Event.EventType.NodeDeleted) {
                    if (DistributedLock.this.epoch.get() == epoch) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Membership changed for lock {} at {}.", lockPath, System.currentTimeMillis());
                        }
                        asyncCheckForLock(true, this, null);
                    }
                } else {
                    LOG.warn("Unexpected ZK event: {}", event.getType().name());
                }
            }

            @Override
            public void processResult(int rc, String path, Object ctx) {
                // when callback, either acquire succeed or fail, we need to notify the waiter.
                if (DistributedLock.this.epoch.get() == epoch) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Notify lock waiters on {} at {} : watcher epoch {}, lock epoch {}",
                                  new Object[] { lockPath, System.currentTimeMillis(), epoch, DistributedLock.this.epoch.get() });
                    }
                    syncPoint.countDown();
                }
            }
        }
    }
}
