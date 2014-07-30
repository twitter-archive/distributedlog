package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.DistributedReentrantLock.DistributedLock;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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

    static class CountDownThrowFailPointAction implements FailpointUtils.FailPointAction {

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

    private void testLockReacquireSuccess(boolean checkWriteLock) throws Exception {
        String lockPath = "/test-lock-re-acquire-success-" + checkWriteLock
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

        if (checkWriteLock) {
            lock0.checkWriteLock(true);
            lock0.checkWriteLock(false);
        } else {
            // session expire will trigger lock re-acquisition
            Future<String> asyncLockAcquireFuture;
            do {
                Thread.sleep(1000);
                asyncLockAcquireFuture = lock0.getAsyncLockAcquireFuture();
            } while (null == asyncLockAcquireFuture);
            Await.result(asyncLockAcquireFuture);
            lock0.checkWriteLock(false);
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

    private void testLockReacquireFailure(boolean checkWriteLock) throws Exception {
        String lockPath = "/test-lock-re-acquire-failure-" + checkWriteLock
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

        if (checkWriteLock) {
            try {
                lock0.checkWriteLock(true);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
            try {
                lock0.checkWriteLock(false);
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
                asyncLockAcquireFuture = lock0.getAsyncLockAcquireFuture();
            } while (null == asyncLockAcquireFuture);

            try {
                Await.result(asyncLockAcquireFuture);
                fail("Should fail check write lock since lock is already held by other people");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(lock1.getInternalLock().getLockId().getLeft(), oafe.getCurrentOwner());
            }
            try {
                lock0.checkWriteLock(false);
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
        lock.checkWriteLock(true);

        assertEquals(lock.haveLock(), true);
        assertEquals(lock.getInternalLock().isLockHeld(), true);

        DistributedReentrantLock lock2 = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            0, clientId + "_2", NullStatsLogger.INSTANCE);

        boolean exceptionEncountered = false;
        try {
            lock2.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        } catch (OwnershipAcquireFailedException exc) {
            assertEquals(exc.getCurrentOwner(), clientId);
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
        lock.checkWriteLock(true);

        assertEquals(lock.haveLock(), true);
        assertEquals(lock.getInternalLock().isLockHeld(), true);

        DistributedReentrantLock lock2 = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            0, clientId + "_2", NullStatsLogger.INSTANCE);

        boolean exceptionEncountered = false;
        try {
            lock2.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        } catch (OwnershipAcquireFailedException exc) {
            assertEquals(exc.getCurrentOwner(), clientId);
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        lock2.close();

        lock.doRelease(DistributedReentrantLock.LockReason.WRITEHANDLER, true);
        assertEquals(lock.haveLock(), true);
        assertEquals(lock.getInternalLock().isLockHeld(), true);
        lock.doRelease(DistributedReentrantLock.LockReason.RECOVER, true);
        assertEquals(lock.haveLock(), false);
        assertEquals(lock.getInternalLock().isLockHeld(), false);

        DistributedReentrantLock lock3 = new DistributedReentrantLock(lockStateExecutor, zkc, lockPath,
            0, clientId + "_3", NullStatsLogger.INSTANCE);

        lock3.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        assertEquals(lock3.haveLock(), true);
        assertEquals(lock3.getInternalLock().isLockHeld(), true);
        lock3.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lock3.close();
    }
}
