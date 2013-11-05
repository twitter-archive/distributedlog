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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;

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
//
class DistributedReentrantLock {

    static final Logger LOG = LoggerFactory.getLogger(DistributedReentrantLock.class);

    private final ZooKeeperClient zkc;
    private final String lockpath;

    private AtomicInteger lockCount = new AtomicInteger(0);
    private DistributedLock internalLock = null;
    private final long lockTimeout;

    DistributedReentrantLock(ZooKeeperClient zkc, String lockpath, long lockTimeout, String clientId) throws IOException {
        this.lockpath = lockpath;
        this.lockTimeout = lockTimeout;

        this.zkc = zkc;
        try {
            if (zkc.get().exists(lockpath, false) == null) {
                zkc.get().create(lockpath, null,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            internalLock = new DistributedLock(zkc, lockpath, clientId);
        } catch (Exception e) {
            throw new IOException("Exception accessing Zookeeper", e);
        }
    }

    void acquire(String reason) throws LockingException {
        LOG.debug("Lock Acquire {}, {}", lockpath, reason);
        while (true) {
            if (lockCount.get() == 0) {
                synchronized (this) {
                    if (lockCount.get() > 0) {
                        lockCount.incrementAndGet();
                        return;
                    }
                    if (internalLock.tryLock(lockTimeout, TimeUnit.MILLISECONDS)) {
                        lockCount.set(1);
                        break;
                    } else {
                        throw new LockingException("Failed to acquire lock within timeout");
                    }
                }
            } else {
                int ret = lockCount.getAndIncrement();
                if (ret == 0) {
                    lockCount.decrementAndGet();
                    continue; // try again;
                } else {
                    return;
                }
            }
        }
    }

    void release(String reason) throws IOException {
        LOG.debug("Lock Release {}, {}", lockpath, reason);
        try {
            if (lockCount.decrementAndGet() <= 0) {
                if (lockCount.get() < 0) {
                    LOG.warn("Unbalanced lock handling for {}, lockCount is {} ",
                        reason, lockCount.get());
                }
                synchronized (this) {
                    if (lockCount.get() <= 0) {
                        if (internalLock.isLockHeld()) {
                            LOG.info("Lock Release {}, {}", lockpath, reason);
                            internalLock.unlock();
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IOException("Exception accessing Zookeeper", e);
        }
    }

    public boolean checkWriteLock() throws LockingException {
        if (!haveLock()) {
            LOG.info("Lost writer lock");
            // We may have just lost the lock because of a ZK session timeout
            // not necessarily because someone else acquired the lock.
            // In such cases just try to reacquire. If that fails, it will throw
            acquire("checkWriteLock");
            return true;
        }

        return false;
    }

    boolean haveLock() {
        return lockCount.get() > 0;
    }

    public void close() {
        try {
            internalLock.cleanup();
            zkc.get().exists(lockpath, false);
        } catch (Exception e) {
            LOG.debug("Could not remove watch on lock");
        }
    }

    /**
     * Twitter commons version with some modifications.
     * <p/>
     * TODO: Cannot use the same version to avoid a transitive dependence on two different versions of zookeeper.
     * TODO: When science upgrades to ZK 3.4.X we should remove this
     */
    private static class DistributedLock {

        private final ZooKeeperClient zkClient;
        private final String lockPath;
        private final String clientId;

        private final AtomicBoolean aborted = new AtomicBoolean(false);
        private CountDownLatch syncPoint;
        private boolean holdsLock = false;
        private String currentId;
        private String currentNode;
        private String watchedNode;
        private String currentOwner = DistributedLogConstants.UNKNOWN_CLIENT_ID;
        private LockWatcher watcher;

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

        private synchronized void prepare()
            throws LockingException, InterruptedException, KeeperException, ZooKeeperClient.ZooKeeperConnectionException {

            LOG.debug("Working with locking path: {}", lockPath);

            // Create an EPHEMERAL_SEQUENTIAL node.
            currentNode =
                zkClient.get().create(lockPath + "/member_", clientId.getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            // We only care about our actual id since we want to compare ourselves to siblings.
            if (currentNode.contains("/")) {
                currentId = currentNode.substring(currentNode.lastIndexOf("/") + 1);
            }
            LOG.debug("Received ID from zk: {}", currentId);
            this.watcher = new LockWatcher();
        }

        public synchronized void lock() throws LockingException {
            if (holdsLock) {
                throw new LockingException("Error, already holding a lock. Call unlock first!");
            }
            try {
                prepare();
                watcher.checkForLock();
                syncPoint.await();
                if (!holdsLock) {
                    throw new LockingException("Error, couldn't acquire the lock!");
                }
            } catch (InterruptedException e) {
                cancelAttempt();
                throw new LockingException("InterruptedException while trying to acquire lock!", e);
            } catch (Exception e) {
                // No need to clean up since the node wasn't created yet.
                throw new LockingException("ZooKeeper Exception while trying to acquire lock", e);
            }
        }

        public synchronized boolean tryLock(long timeout, TimeUnit unit) throws LockingException {
            if (holdsLock) {
                throw new LockingException("Error, already holding a lock. Call unlock first!");
            }
            try {
                prepare();
                watcher.checkForLock();
                if (DistributedLogConstants.LOCK_IMMEDIATE != timeout) {
                    boolean success = syncPoint.await(timeout, unit);
                    // assert success => holdsLock
                    assert(!success || holdsLock);
                }
                if (!holdsLock) {
                    throw new OwnershipAcquireFailedException("Error, couldn't acquire the lock!", currentOwner);
                }
            } catch (InterruptedException e) {
                cancelAttempt();
                return false;
            } catch (Exception e) {
                // No need to clean up since the node wasn't created yet.
                throw new LockingException("ZooKeeper Exception while trying to acquire lock", e);
            }
            return true;
        }

        public synchronized void unlock() throws LockingException {
            if (currentId == null) {
                throw new LockingException("Error, neither attempting to lock nor holding a lock!");
            }
            // Try aborting!
            if (!holdsLock) {
                aborted.set(true);
                LOG.info("Not holding lock, aborting acquisition attempt!");
            } else {
                LOG.debug("Cleaning up this locks ephemeral node. {} {}", currentId, currentNode);
                cleanup();
            }
        }

        private synchronized String getCurrentId() {
            return currentId;
        }

        public synchronized boolean isLockHeld() {
            return holdsLock;
        }

        private synchronized void cancelAttempt() {
            LOG.info("Cancelling lock attempt!");
            cleanup();
            // Bubble up failure...
            holdsLock = false;
            syncPoint.countDown();
        }

        private void cleanup() {
            LOG.debug("Cleaning up!");
            try {
                Stat stat = zkClient.get().exists(currentNode, false);
                if (stat != null) {
                    zkClient.get().delete(currentNode, -1);
                } else {
                    LOG.warn("Called cleanup but nothing to cleanup!");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            holdsLock = false;
            aborted.set(false);
            currentId = null;
            currentNode = null;
            watcher = null;
            syncPoint = new CountDownLatch(1);
        }

        class LockWatcher implements Watcher {

            public synchronized void checkForLock() {
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
                        throw new RuntimeException("Error, member list is empty!");
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

                        final String nextLowestNode = sortedMembers.get(memberIndex - 1);
                        LOG.debug(String.format("Current LockWatcher with ephemeral node [%s], is " +
                            "waiting for [%s] to release lock.", getCurrentId(), nextLowestNode));

                        watchedNode = String.format("%s/%s", lockPath, nextLowestNode);
                        Stat stat = zkClient.get().exists(watchedNode, this);
                        if (stat == null) {
                            checkForLock();
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.warn(String.format("Current LockWatcher with ephemeral node [%s] " +
                        "got interrupted. Trying to cancel lock acquisition.", getCurrentId()), e);
                    cancelAttempt();
                } catch (Exception e) {
                    LOG.warn(String.format("Current LockWatcher with ephemeral node [%s] " +
                            "got a exception while accessing ZK. Trying to cancel lock acquisition.", getCurrentId()), e);
                    cancelAttempt();
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
                            cancelAttempt();
                            break;
                        default:
                            break;
                    }
                } else if (event.getType() == Event.EventType.NodeDeleted) {
                    checkForLock();
                } else {
                    LOG.warn(String.format("Unexpected ZK event: %s", event.getType().name()));
                }
            }
        }
    }
}
