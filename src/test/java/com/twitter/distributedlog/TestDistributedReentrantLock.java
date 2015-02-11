package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.DistributedReentrantLock.DistributedLock;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static com.twitter.distributedlog.DistributedReentrantLock.DistributedLock.asyncParseClientID;

/**
 * Distributed Lock Tests
 */
public class TestDistributedReentrantLock extends TestDistributedLogBase {

    static final Logger logger = LoggerFactory.getLogger(TestDistributedReentrantLock.class);

    @Rule
    public TestName runtime = new TestName();

    private final static int sessionTimeoutMs = 2000;

    private ZooKeeperClient zkc;
    private ZooKeeperClient zkc0; // used for checking
    private OrderedSafeExecutor lockStateExecutor;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI("/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .zkAclId(null)
                .build();
        zkc0 = ZooKeeperClientBuilder.newBuilder()
                .name("zkc0")
                .uri(DLMTestUtil.createDLMURI("/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .zkAclId(null)
                .build();
        lockStateExecutor = new OrderedSafeExecutor(1);
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
        zkc0.close();
        lockStateExecutor.shutdown();
    }

    private static void createLockPath(ZooKeeper zk, String lockPath) throws Exception {
        zk.create(lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private static List<String> getLockWaiters(ZooKeeperClient zkc, String lockPath) throws Exception {
        List<String> children = zkc.get().getChildren(lockPath, false);
        Collections.sort(children, DistributedLock.MEMBER_COMPARATOR);
        return children;
    }

    static class TestLockFactory {
        final String lockPath;
        final String clientId;
        final OrderedSafeExecutor lockStateExecutor;

        public TestLockFactory(String name, ZooKeeperClient defaultZkc, OrderedSafeExecutor lockStateExecutor) throws Exception {
            this.lockPath = "/" + name + System.currentTimeMillis();
            this.clientId = name;
            createLockPath(defaultZkc.get(), lockPath);
            this.lockStateExecutor = lockStateExecutor;
        }
        public DistributedReentrantLock createLock(int id, ZooKeeperClient zkc) throws Exception {
            return new DistributedReentrantLock(this.lockStateExecutor, zkc, this.lockPath, Long.MAX_VALUE, 
                    this.clientId + id, NullStatsLogger.INSTANCE);
        }
        public String getLockPath() {
            return lockPath;
        }
    }

    static class CountDownThrowFailPointAction extends FailpointUtils.AbstractFailPointAction {

        final AtomicInteger successCounter;
        final AtomicInteger failureCounter;

        CountDownThrowFailPointAction(int successCount, int failureCount) {
            this.successCounter = new AtomicInteger(successCount);
            this.failureCounter = new AtomicInteger(failureCount);
        }

        @Override
        public boolean checkFailPoint() throws IOException {
            int successCount = successCounter.getAndDecrement();
            if (successCount > 0) {
                return true;
            }
            int count = failureCounter.getAndDecrement();
            if (count > 0) {
                throw new IOException("counter = " + count);
            }
            return true;
        }
    }

    @Test(timeout = 60000)
    public void testZooKeeperConnectionLossOnLockCreation() throws Exception {
        String lockPath = "/test-zookeeper-connection-loss-on-lock-creation-" + System.currentTimeMillis();
        String clientId = "zookeeper-connection-loss";

        createLockPath(zkc.get(), lockPath);

        FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_ZooKeeperConnectionLoss,
                new CountDownThrowFailPointAction(0, Integer.MAX_VALUE));
        try {
            try {
                new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
                        Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE, 0);
                fail("Should fail on creating lock if couldn't establishing connections to zookeeper");
            } catch (IOException ioe) {
                // expected.
            }
        } finally {
            FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_ZooKeeperConnectionLoss);
        }

        FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_ZooKeeperConnectionLoss,
                new CountDownThrowFailPointAction(0, Integer.MAX_VALUE));
        try {
            try {
                new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
                        Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE, 3);
                fail("Should fail on creating lock if couldn't establishing connections to zookeeper after 3 retries");
            } catch (IOException ioe) {
                // expected.
            }
        } finally {
            FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_ZooKeeperConnectionLoss);
        }

        FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_ZooKeeperConnectionLoss,
                new CountDownThrowFailPointAction(0, 3));
        try {
            DistributedReentrantLock lock = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
                Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE, 5);
            lock.acquire(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

            Pair<String, Long> lockId1 = lock.getInternalLock().getLockId();

            List<String> children = getLockWaiters(zkc, lockPath);
            assertEquals(1, children.size());
            assertTrue(lock.haveLock());
            assertEquals(1, lock.getLockCount());
            assertEquals(lockId1, Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

            lock.release(DistributedReentrantLock.LockReason.PERSTREAMWRITER);
            lock.close();
        } finally {
            FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_ZooKeeperConnectionLoss);
        }
    }

    @Test(timeout = 60000)
    public void testBasicAcquireRelease() throws Exception {
        String lockPath = "/test-basic-acquire-release-" + System.currentTimeMillis();
        String clientId = "basic-acquire-release";

        createLockPath(zkc.get(), lockPath);

        DistributedReentrantLock lock = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
                Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE);
        lock.acquire(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

        Pair<String, Long> lockId1 = lock.getInternalLock().getLockId();

        List<String> children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertTrue(lock.haveLock());
        assertEquals(1, lock.getLockCount());
        assertEquals(lockId1, Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        lock.release(DistributedReentrantLock.LockReason.PERSTREAMWRITER);

        children = getLockWaiters(zkc, lockPath);
        assertEquals(0, children.size());
        assertFalse(lock.haveLock());
        assertEquals(0, lock.getLockCount());

        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);

        Pair<String, Long> lockId2 = lock.getInternalLock().getLockId();

        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertTrue(lock.haveLock());
        assertEquals(1, lock.getLockCount());
        assertEquals(lockId2, Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        assertEquals(lockId1, lockId2);

        lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);

        children = getLockWaiters(zkc, lockPath);
        assertEquals(0, children.size());
        assertFalse(lock.haveLock());
        assertEquals(0, lock.getLockCount());

        lock.close();
        assertEquals(0, lock.getLockCount());

        try {
            lock.acquire(DistributedReentrantLock.LockReason.RECOVER);
            fail("Should fail on acquiring a closed lock");
        } catch (LockingException le) {
            // expected.
        }
        children = getLockWaiters(zkc, lockPath);
        assertEquals(0, children.size());
        assertFalse(lock.haveLock());
        assertEquals(0, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testCheckWriteLockFailureWhenLockIsAcquiredByOthers() throws Exception {
        String lockPath = "/test-check-write-lock-failure-when-lock-is-acquired-by-others-" + System.currentTimeMillis();
        String clientId = "test-check-write-lock-failure";

        createLockPath(zkc.get(), lockPath);

        DistributedReentrantLock lock0 =
                new DistributedReentrantLock(lockStateExecutor, zkc0, lockPath,
                        Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE);
        lock0.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);

        Pair<String, Long> lockId0_1 = lock0.getInternalLock().getLockId();

        List<String> children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertTrue(lock0.haveLock());
        assertEquals(1, lock0.getLockCount());
        assertEquals(lock0.getInternalLock().getLockId(),
                Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        // expire the session
        ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);

        lock0.checkOwnershipAndReacquire(true);

        Pair<String, Long> lockId0_2 = lock0.getInternalLock().getLockId();
        assertFalse("New lock should be created under different session", lockId0_1.equals(lockId0_2));

        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertTrue(lock0.haveLock());
        assertEquals(1, lock0.getLockCount());
        assertEquals(lock0.getInternalLock().getLockId(),
                Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        final DistributedReentrantLock lock1 =
                new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
                        Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE);
        final CountDownLatch lockLatch = new CountDownLatch(1);
        Thread lockThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
                    lockLatch.countDown();
                } catch (LockingException e) {
                    logger.error("Failed on locking lock1 : ", e);
                }
            }
        }, "lock-thread");
        lockThread.start();

        // ensure lock1 is waiting for lock0
        do {
            Thread.sleep(1000);
            children = getLockWaiters(zkc, lockPath);
        } while (children.size() < 2);

        // expire the session
        ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);

        lockLatch.await();
        lockThread.join();

        try {
            lock0.checkOwnershipAndReacquire(true);
            fail("Should fail on checking write lock since lock is acquired by lock1");
        } catch (LockingException le) {
            // expected
        }

        try {
            lock0.checkOwnershipAndReacquire(false);
            fail("Should fail on checking write lock since lock is acquired by lock1");
        } catch (LockingException le) {
            // expected
        }
    }

    /**
     * If no lock is acquired between session expired and re-acquisition, check write lock will acquire the lock.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockReacquireSuccessAfterCheckWriteLock() throws Exception {
        testLockReacquireSuccess(true);
    }

    /**
     * If no lock is acquired between session expired and re-acquisition, check write lock will acquire the lock.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockReacquireSuccessWithoutCheckWriteLock() throws Exception {
        testLockReacquireSuccess(false);
    }

    private void testLockReacquireSuccess(boolean checkOwnershipAndReacquire) throws Exception {
        String lockPath = "/test-lock-re-acquire-success-" + checkOwnershipAndReacquire
                + "-" + System.currentTimeMillis();
        String clientId = "test-lock-re-acquire";

        createLockPath(zkc.get(), lockPath);

        DistributedReentrantLock lock0 =
                new DistributedReentrantLock(lockStateExecutor, zkc0, lockPath,
                        Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE);
        lock0.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);

        Pair<String, Long> lockId0_1 = lock0.getInternalLock().getLockId();

        List<String> children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertTrue(lock0.haveLock());
        assertEquals(1, lock0.getLockCount());
        assertEquals(lock0.getInternalLock().getLockId(),
                Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);

        if (checkOwnershipAndReacquire) {
            lock0.checkOwnershipAndReacquire(true);
            lock0.checkOwnershipAndReacquire(false);
        } else {
            // session expire will trigger lock re-acquisition
            Future<String> asyncLockAcquireFuture;
            do {
                Thread.sleep(1000);
                asyncLockAcquireFuture = lock0.getLockReacquireFuture();
            } while (null == asyncLockAcquireFuture && lock0.getReacquireCount() < 1);
            if (null != asyncLockAcquireFuture) {
                Await.result(asyncLockAcquireFuture);
            }
            lock0.checkOwnershipAndReacquire(false);
        }
        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertTrue(lock0.haveLock());
        assertEquals(1, lock0.getLockCount());
        assertEquals(lock0.getInternalLock().getLockId(),
                Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));
        assertEquals(clientId, lock0.getInternalLock().getLockId().getLeft());
        assertFalse(lockId0_1.equals(lock0.getInternalLock().getLockId()));

        lock0.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        assertEquals(0, lock0.getLockCount());

        children = getLockWaiters(zkc, lockPath);
        assertEquals(0, children.size());

        lock0.close();
        assertEquals(0, lock0.getLockCount());
    }

    /**
     * If lock is acquired between session expired and re-acquisition, check write lock will be failed.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockReacquireFailureAfterCheckWriteLock() throws Exception {
        testLockReacquireFailure(true);
    }

    /**
     * If lock is acquired between session expired and re-acquisition, check write lock will be failed.
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockReacquireFailureWithoutCheckWriteLock() throws Exception {
        testLockReacquireFailure(false);
    }

    private void testLockReacquireFailure(boolean checkOwnershipAndReacquire) throws Exception {
        String lockPath = "/test-lock-re-acquire-failure-" + checkOwnershipAndReacquire
                + "-" + System.currentTimeMillis();
        String clientId = "test-lock-re-acquire";

        createLockPath(zkc.get(), lockPath);

        DistributedReentrantLock lock0 =
                new DistributedReentrantLock(lockStateExecutor, zkc0, lockPath,
                        Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE);
        lock0.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);

        final CountDownLatch lock1DoneLatch = new CountDownLatch(1);
        final DistributedReentrantLock lock1 =
                new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
                        Long.MAX_VALUE, clientId, NullStatsLogger.INSTANCE);
        Thread lock1Thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
                    lock1DoneLatch.countDown();
                } catch (LockingException e) {
                    logger.error("Error on acquiring lock1 : ", e);
                }
            }
        }, "lock1-thread");
        lock1Thread.start();

        List<String> children;
        do {
            Thread.sleep(1000);
            children = getLockWaiters(zkc, lockPath);
        } while (children.size() < 2);
        assertEquals(2, children.size());
        assertTrue(lock0.haveLock());
        assertEquals(1, lock0.getLockCount());
        assertFalse(lock1.haveLock());
        assertEquals(0, lock1.getLockCount());
        assertEquals(lock0.getInternalLock().getLockId(),
                Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
        assertEquals(lock1.getInternalLock().getLockId(),
                Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(1))));

        ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);
        lock1DoneLatch.await();
        assertFalse(lock0.haveLock());
        assertEquals(1, lock0.getLockCount());
        assertTrue(lock1.haveLock());
        assertEquals(1, lock1.getLockCount());

        if (checkOwnershipAndReacquire) {
            try {
                lock0.checkOwnershipAndReacquire(true);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
            try {
                lock0.checkOwnershipAndReacquire(false);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
            try {
                lock0.acquire(DistributedReentrantLock.LockReason.DELETELOG);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
        } else {
            // session expire will trigger lock re-acquisition
            Future<String> asyncLockAcquireFuture;
            do {
                Thread.sleep(1000);
                asyncLockAcquireFuture = lock0.getLockReacquireFuture();
            } while (null == asyncLockAcquireFuture);

            try {
                Await.result(asyncLockAcquireFuture);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
            try {
                lock0.checkOwnershipAndReacquire(false);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
            try {
                lock0.acquire(DistributedReentrantLock.LockReason.DELETELOG);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
        }
        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertFalse(lock0.haveLock());
        assertEquals(1, lock0.getLockCount());
        assertTrue(lock1.haveLock());
        assertEquals(1, lock1.getLockCount());
        assertEquals(lock1.getInternalLock().getLockId(),
                Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));

        lock0.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        assertEquals(0, lock0.getLockCount());
        lock1.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        assertEquals(0, lock1.getLockCount());

        children = getLockWaiters(zkc, lockPath);
        assertEquals(0, children.size());

        try {
            lock0.acquire(DistributedReentrantLock.LockReason.RECOVER);
            fail("Should fail check write lock since lock is already held by other people");
        } catch (OwnershipAcquireFailedException oafe) {
            assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
        }
        assertEquals(0, lock0.getLockCount());

        lock0.close();
        assertEquals(0, lock0.getLockCount());
        lock1.close();
        assertEquals(0, lock1.getLockCount());
    }

    @Test(timeout = 60000)
    public void testLockReacquire() throws Exception {
        String lockPath = "/reacquirePath";
        Utils.zkCreateFullPathOptimistic(zkc, lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        String clientId = "lockHolder";
        DistributedReentrantLock lock = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            conf.getLockTimeoutMilliSeconds(), clientId, NullStatsLogger.INSTANCE);
        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);

        // try and cleanup the underlying lock
        lock.getInternalLock().unlock();

        // This should reacquire the lock
        lock.checkOwnershipAndReacquire(true);

        assertEquals(true, lock.haveLock());
        assertEquals(true, lock.getInternalLock().isLockHeld());

        DistributedReentrantLock lock2 = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            0, clientId + "_2", NullStatsLogger.INSTANCE);

        boolean exceptionEncountered = false;
        try {
            lock2.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        } catch (OwnershipAcquireFailedException exc) {
            assertEquals(clientId, exc.getCurrentOwner());
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lock.close();
        lock2.close();
    }

    @Test(timeout = 60000)
    public void testLockReacquireMultiple() throws Exception {
        String lockPath = "/reacquirePathMultiple";
        Utils.zkCreateFullPathOptimistic(zkc, lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        String clientId = "lockHolder";
        DistributedReentrantLock lock = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            conf.getLockTimeoutMilliSeconds(), clientId, NullStatsLogger.INSTANCE);
        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lock.acquire(DistributedReentrantLock.LockReason.RECOVER);

        // try and cleanup the underlying lock
        lock.getInternalLock().unlock();

        // This should reacquire the lock
        lock.checkOwnershipAndReacquire(true);

        assertEquals(true, lock.haveLock());
        assertEquals(true, lock.getInternalLock().isLockHeld());

        DistributedReentrantLock lock2 = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            0, clientId + "_2", NullStatsLogger.INSTANCE);

        boolean exceptionEncountered = false;
        try {
            lock2.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        } catch (OwnershipAcquireFailedException exc) {
            assertEquals(clientId, exc.getCurrentOwner());
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        lock2.close();

        lock.doRelease(DistributedReentrantLock.LockReason.WRITEHANDLER, true);
        assertEquals(true, lock.haveLock());
        assertEquals(true, lock.getInternalLock().isLockHeld());
        lock.doRelease(DistributedReentrantLock.LockReason.RECOVER, true);
        assertEquals(false, lock.haveLock());
        assertEquals(false, lock.getInternalLock().isLockHeld());

        DistributedReentrantLock lock3 = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            0, clientId + "_3", NullStatsLogger.INSTANCE);

        lock3.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        assertEquals(true, lock3.haveLock());
        assertEquals(true, lock3.getInternalLock().isLockHeld());
        lock3.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lock3.close();
    }

    void assertLatchesSet(CountDownLatch[] latches, int endIndex) {
        for (int i = 0; i < endIndex; i++) {
            assertEquals("latch " + i + " should have been set", 0, latches[i].getCount());
        }
        for (int i = endIndex; i < latches.length; i++) {
            assertEquals("latch " + i + " should not have been set", 1, latches[i].getCount());
        }
    }

    // Assert key lock state (is locked, is internal locked, lock count, etc.) for two dlocks.
    void assertLockState(DistributedReentrantLock lock0, boolean owned0, boolean intOwned0, int count0, 
                         DistributedReentrantLock lock1, boolean owned1, boolean intOwned1, int count1,
                         int waiters, String lockPath) throws Exception {
        assertEquals(owned0, lock0.haveLock());
        assertEquals(intOwned0, lock0.getInternalLock().isLockHeld());
        assertEquals(count0, lock0.getLockCount());
        assertEquals(owned1, lock1.haveLock());
        assertEquals(intOwned1, lock1.getInternalLock().isLockHeld());
        assertEquals(count1, lock1.getLockCount());
        assertEquals(waiters, getLockWaiters(zkc, lockPath).size());
    }

    @Test(timeout = 60000)
    public void testAsyncAcquireBasics() throws Exception {
        TestLockFactory locks = new TestLockFactory(runtime.getMethodName(), zkc, lockStateExecutor);

        int count = 3;
        ArrayList<Future<Void>> results = new ArrayList<Future<Void>>(count); 
        DistributedReentrantLock[] lockArray = new DistributedReentrantLock[count]; 
        final CountDownLatch[] latches = new CountDownLatch[count]; 
        DistributedReentrantLock.LockReason reason = DistributedReentrantLock.LockReason.READHANDLER;

        // Set up <count> waiters, save async results, count down a latch when lock is acquired in 
        // the future.
        for (int i = 0; i < count; i++) {
            latches[i] = new CountDownLatch(1);
            lockArray[i] = locks.createLock(i, zkc);
            final int index = i;
            results.add(lockArray[i].asyncAcquire(reason).addEventListener(
                new FutureEventListener<Void>() {
                    @Override
                    public void onSuccess(Void complete) {
                        latches[index].countDown();
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        fail("unexpected failure " + cause);
                    }
                }
            ));
        }

        // Now await ownership and release ownership of locks one by one (in the order they were 
        // acquired).
        for (int i = 0; i < count; i++) {
            latches[i].await();
            assertLatchesSet(latches, i+1);
            Await.result(results.get(i));
            lockArray[i].release(reason);
        }
    }

    @Test(timeout = 60000)
    public void testAsyncAcquireAsyncThenSyncOnSameLock() throws Exception {
        TestLockFactory locks = new TestLockFactory(runtime.getMethodName(), zkc, lockStateExecutor);
        final DistributedReentrantLock lock0 = locks.createLock(0, zkc);
        final DistributedReentrantLock lock1 = locks.createLock(1, zkc0);

        lock0.acquire(DistributedReentrantLock.LockReason.READHANDLER);
        Future<Void> result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        final CountDownLatch lock1Latch = new CountDownLatch(1);

        Thread lock1Thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire(DistributedReentrantLock.LockReason.READHANDLER);
                    lock1Latch.countDown();
                } catch (LockingException e) {
                    fail("shouldn't fail to acquire");
                }
            }
        }, "lock1-thread");
        lock1Thread.start();

        // Initial state.
        assertLockState(lock0, true, true, 1, lock1, false, false, 0, 2, locks.getLockPath());

        // Release lock0. lock1 acquire should succeed and complete the future. Need 
        // to wait for confirmation from the latch since its async.
        lock0.release(DistributedReentrantLock.LockReason.READHANDLER);
        lock1Latch.await();
        Await.result(result);
        assertLockState(lock0, false, false, 0, lock1, true, true, 2, 1, locks.getLockPath());

        // Release lock1 once, still have one holder.
        lock1.release(DistributedReentrantLock.LockReason.READHANDLER);
        assertLockState(lock0, false, false, 0, lock1, true, true, 1, 1, locks.getLockPath());

        // Release lock1 again, back to base state.
        lock1.release(DistributedReentrantLock.LockReason.READHANDLER);
        assertLockState(lock0, false, false, 0, lock1, false, false, 0, 0, locks.getLockPath());

        lock0.close();
        lock1.close();
    }

    @Test(timeout = 60000)
    public void testAsyncAcquireSyncThenAsyncOnSameLock() throws Exception {
        TestLockFactory locks = new TestLockFactory(runtime.getMethodName(), zkc, lockStateExecutor);
        final DistributedReentrantLock lock0 = locks.createLock(0, zkc);
        final DistributedReentrantLock lock1 = locks.createLock(1, zkc0);

        lock0.acquire(DistributedReentrantLock.LockReason.READHANDLER);

        // Initial state.
        assertLockState(lock0, true, true, 1, lock1, false, false, 0, 1, locks.getLockPath());

        Thread lock1Thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire(DistributedReentrantLock.LockReason.READHANDLER);
                } catch (LockingException e) {
                    fail("shouldn't fail to acquire");
                }
            }
        }, "lock1-thread");
        lock1Thread.start();

        // Wait for lock count to increase, indicating background acquire has succeeded.
        while (getLockWaiters(zkc, locks.getLockPath()).size() < 2) {
            Thread.sleep(1000);
        }
        assertLockState(lock0, true, true, 1, lock1, false, false, 0, 2, locks.getLockPath());

        lock0.release(DistributedReentrantLock.LockReason.READHANDLER);
        Future<Void> result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        Await.result(result);

        assertLockState(lock0, false, false, 0, lock1, true, true, 2, 1, locks.getLockPath());

        // Release lock1 once, still have one holder.
        lock1.release(DistributedReentrantLock.LockReason.READHANDLER);
        assertLockState(lock0, false, false, 0, lock1, true, true, 1, 1, locks.getLockPath());

        // Release lock1 again, back to base state.
        lock1.release(DistributedReentrantLock.LockReason.READHANDLER);
        assertLockState(lock0, false, false, 0, lock1, false, false, 0, 0, locks.getLockPath());

        lock0.close();
        lock1.close();
    }

    @Test(timeout = 60000)
    public void testAsyncAcquireExpireDuringWait() throws Exception {
        TestLockFactory locks = new TestLockFactory(runtime.getMethodName(), zkc, lockStateExecutor);
        final DistributedReentrantLock lock0 = locks.createLock(0, zkc);
        final DistributedReentrantLock lock1 = locks.createLock(1, zkc0);

        // Method asyncAcquire doesn't return until the request has been added to the waiters list, so when we expire the 
        // session we can be sure we're waiting.
        lock0.acquire(DistributedReentrantLock.LockReason.READHANDLER);
        Future<Void> result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);

        // Expire causes acquire future to be failed and unset.
        ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);
        try {
            Await.result(result);
            fail("future should have been failed");
        } catch (DistributedReentrantLock.LockSessionExpiredException ex) {
        } 

        assertLockState(lock0, true, true, 1, lock1, false, false, 0, 1, locks.getLockPath());

        // Lock can still be used.
        result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        lock0.release(DistributedReentrantLock.LockReason.READHANDLER);
        Await.result(result);
    }

    @Test(timeout = 60000)
    public void testAsyncAcquireUnlockDuringWait() throws Exception {
        TestLockFactory locks = new TestLockFactory(runtime.getMethodName(), zkc, lockStateExecutor);
        final DistributedReentrantLock lock0 = locks.createLock(0, zkc);
        final DistributedReentrantLock lock1 = locks.createLock(1, zkc0);

        lock0.acquire(DistributedReentrantLock.LockReason.READHANDLER);

        // Internal lock unlock also causes acquire future to be failed and unset.
        Future<Void> result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        lock1.getInternalLock().unlock();
        try {
            Await.result(result);
            fail("future should have been failed");
        } catch (DistributedReentrantLock.LockClosedException ex) {
        }

        assertLockState(lock0, true, true, 1, lock1, false, false, 0, 1, locks.getLockPath());

        // Lock can still be used.
        result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        lock0.release(DistributedReentrantLock.LockReason.READHANDLER);
        Await.result(result);
    }

    @Test(timeout = 60000)
    public void testAsyncAcquireCloseDuringWait() throws Exception {
        TestLockFactory locks = new TestLockFactory(runtime.getMethodName(), zkc, lockStateExecutor);
        final DistributedReentrantLock lock0 = locks.createLock(0, zkc);
        final DistributedReentrantLock lock1 = locks.createLock(1, zkc0);

        lock0.acquire(DistributedReentrantLock.LockReason.READHANDLER);

        lock0.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        Future<Void> result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        lock1.close();
        try {
            Await.result(result);
            fail("future should have been failed");
        } catch (DistributedReentrantLock.LockClosedException ex) {
        }

        assertLockState(lock0, true, true, 2, lock1, false, false, 0, 1, locks.getLockPath());

        try {
            result = lock1.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
            fail("future should have been failed");
        } catch (OwnershipAcquireFailedException oe) {
        } catch (LockingException le) {
        }
    }

    @Test(timeout = 60000)
    public void testAsyncAcquireCloseAfterAcquire() throws Exception {
        TestLockFactory locks = new TestLockFactory(runtime.getMethodName(), zkc, lockStateExecutor);
        final DistributedReentrantLock lock0 = locks.createLock(0, zkc);
        
        Future<Void> result = lock0.asyncAcquire(DistributedReentrantLock.LockReason.READHANDLER);
        Await.result(result);
        lock0.close();

        // Already have this, stays satisfied.
        Await.result(result);

        // But we no longer have the lock.
        assertEquals(false, lock0.haveLock());
        assertEquals(false, lock0.getInternalLock().isLockHeld());
    }
}
