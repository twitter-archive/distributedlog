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

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
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


    DistributedReentrantLock(
        ScheduledExecutorService executorService,
        ZooKeeperClient zkc,
        String lockPath,
        long lockTimeout,
        String clientId) throws IOException {

        this.executorService = executorService;
        this.lockPath = lockPath;
        this.lockTimeout = lockTimeout;
        this.lockAcqTracker = new AtomicIntegerArray(LockReason.MAXREASON.value);

        try {
            if (zkc.get().exists(lockPath, false) == null) {
                zkc.get().create(lockPath, null,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            internalLock = new DistributedLock(zkc, lockPath, clientId);
        } catch (KeeperException ke) {
            throw new IOException("Exception when creating zookeeper lock " + lockPath, ke);
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on creating zookeeper lock " + lockPath, e);
        }
    }

    void acquire(LockReason reason) throws LockingException {
        LOG.debug("Lock Acquire {}, {}", lockPath, reason);
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
        LOG.debug("Lock Release {}, {}", lockPath, reason);
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

    /**
     * Twitter commons version with some modifications.
     * <p/>
     * TODO: Cannot use the same version to avoid a transitive dependence on
     * TODO: two different versions of zookeeper.
     * TODO: When science upgrades to ZK 3.4.X we should remove this
     */
    private static class DistributedLock implements AsyncCallback.VoidCallback {

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

        private synchronized int prepare()
            throws InterruptedException, KeeperException, ZooKeeperClient.ZooKeeperConnectionException {

            LOG.debug("Working with locking path: {}", lockPath);

            // Increase the epoch each time acquire the lock
            int curEpoch = epoch.incrementAndGet();

            // Create an EPHEMERAL_SEQUENTIAL node.
            currentNode =
                zkClient.get().create(lockPath + "/member_", clientId.getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            // We only care about our actual id since we want to compare ourselves to siblings.
            if (currentNode.contains("/")) {
                currentId = currentNode.substring(currentNode.lastIndexOf("/") + 1);
            }
            LOG.debug("Received ID from zk: {}", currentId);
            this.watcher = new LockWatcher(curEpoch);
            return curEpoch;
        }

        public synchronized boolean tryLock(long timeout, TimeUnit unit) throws LockingException {
            if (holdsLock) {
                throw new LockingException(lockPath, "Error, already holding a lock. Call unlock first!");
            }
            try {
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

        public synchronized void unlock() {
            if (currentId == null) {
                LOG.error("Error, neither attempting to lock nor holding a lock! {}", lockPath);
                return;
            }
            // Try aborting!
            if (!holdsLock) {
                aborted.set(true);
                LOG.info("Not holding lock, aborting acquisition attempt!");
            } else {
                LOG.debug("Cleaning up this locks ephemeral node. {} {}", currentId, currentNode);
                cleanup(true);
            }
        }

        private synchronized String getCurrentId() {
            return currentId;
        }

        public synchronized boolean isLockHeld() {
            return holdsLock;
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (KeeperException.Code.OK.intValue() == rc) {
                LOG.info("Deleted lock znode {} successfully!", path);
            } else {
                LOG.info("Deleted lock znode {} : ", path, KeeperException.create(KeeperException.Code.get(rc)));
            }
        }

        private synchronized void cancelAttempt(boolean sync, int epoch) {
            if (this.epoch.getAndIncrement() == epoch) {
                LOG.info("Cancelling lock attempt!");
                cleanup(sync);
                // Bubble up failure...
                holdsLock = false;
                syncPoint.countDown();
            }
        }

        private void cleanup(boolean sync) {
            LOG.debug("Cleaning up!");
            String nodeToDelete = currentNode;
            try {
                if (null != nodeToDelete) {
                    if (sync) {
                        Stat stat = zkClient.get().exists(nodeToDelete, false);
                        if (stat != null) {
                            zkClient.get().delete(nodeToDelete, -1);
                        } else {
                            LOG.warn("Called cleanup but nothing to cleanup!");
                        }
                    } else {
                        zkClient.get().delete(nodeToDelete, -1, this, null);
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

        class LockWatcher implements Watcher {

            // Enforce a epoch number to avoid a race on canceling attempt
            final int epoch;

            LockWatcher(int epoch) {
                this.epoch = epoch;
            }

            public synchronized void checkForLock(boolean wait) {
                try {
                    List<String> sortedMembers = zkClient.get().getChildren(lockPath, null);

                    Collections.sort(sortedMembers, new Comparator<String>() {
                        public int compare(String o1,
                                           String o2) {
                            Integer l1 = Integer.valueOf(o1.replace("member_", ""));
                            Integer l2 = Integer.valueOf(o2.replace("member_", ""));
                            return l1 - l2;
                        }
                    });

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
                        }
                    } else {
                        String currentOwnerTemp = new String(
                            zkClient.get().getData(lockPath + "/" + sortedMembers.get(0), false, null), "UTF-8");

                        // Accessed concurrently in the thread that acquires the lock
                        // checkForLock can be called by background threads as well
                        synchronized(DistributedLock.this) {
                            currentOwner = currentOwnerTemp;
                        }

                        if (wait) {
                            final String nextLowestNode = sortedMembers.get(memberIndex - 1);
                            LOG.debug("Current LockWatcher with ephemeral node {}, is waiting for {} to release lock.",
                                      getCurrentId(), nextLowestNode);

                            watchedNode = String.format("%s/%s", lockPath, nextLowestNode);
                            Stat stat = zkClient.get().exists(watchedNode, true);
                            if (stat == null) {
                                checkForLock(true);
                            }
                        }
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
            public synchronized void process(WatchedEvent event) {
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
                            cancelAttempt(true, epoch);
                            break;
                        default:
                            break;
                    }
                } else if (event.getType() == Event.EventType.NodeDeleted) {
                    if (DistributedLock.this.epoch.get() == epoch) {
                        checkForLock(true);
                    }
                } else {
                    LOG.warn(String.format("Unexpected ZK event: %s", event.getType().name()));
                }
            }
        }
    }
}
