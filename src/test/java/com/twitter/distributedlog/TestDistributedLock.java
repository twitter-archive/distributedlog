package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.DistributedReentrantLock.DistributedLock;
import com.twitter.distributedlog.DistributedReentrantLock.EpochChangedException;
import com.twitter.distributedlog.DistributedReentrantLock.LockStateChangedException;
import com.twitter.util.Await;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static com.twitter.distributedlog.DistributedReentrantLock.DistributedLock.*;

/**
 * Distributed Lock Tests
 */
public class TestDistributedLock extends ZooKeeperClusterTestCase {

    @Rule
    public TestName testNames = new TestName();

    static final Logger logger = LoggerFactory.getLogger(TestDistributedLock.class);

    private final static int sessionTimeoutMs = 2000;

    private ZooKeeperClient zkc;
    private ZooKeeperClient zkc0; // used for checking
    private OrderedSafeExecutor lockStateExecutor;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI(zkPort, "/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .zkServers(zkServers)
                .zkAclId(null)
                .build();
        zkc0 = ZooKeeperClientBuilder.newBuilder()
                .name("zkc0")
                .uri(DLMTestUtil.createDLMURI(zkPort, "/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .zkServers(zkServers)
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

    private static String createLockNodeV1(ZooKeeper zk, String lockPath, String clientId) throws Exception {
        return zk.create(getLockPathPrefixV1(lockPath), serializeClientId(clientId),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private static String createLockNodeV2(ZooKeeper zk, String lockPath, String clientId) throws Exception {
        return zk.create(getLockPathPrefixV2(lockPath, clientId), serializeClientId(clientId),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private static String createLockNodeV3(ZooKeeper zk, String lockPath, String clientId) throws Exception {
        return zk.create(getLockPathPrefixV3(lockPath, clientId, zk.getSessionId()), serializeClientId(clientId),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private static String createLockNodeWithBadNodeName(ZooKeeper zk, String lockPath, String clientId, String badNodeName)
            throws Exception {
        return zk.create(lockPath + "/" + badNodeName, serializeClientId(clientId),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private static List<String> getLockWaiters(ZooKeeperClient zkc, String lockPath) throws Exception {
        List<String> children = zkc.get().getChildren(lockPath, false);
        Collections.sort(children, DistributedLock.MEMBER_COMPARATOR);
        return children;
    }

    @Test(timeout = 60000)
    public void testParseClientID() throws Exception {
        ZooKeeper zk = zkc.get();

        String lockPath = "/test-parse-clientid";
        String clientId = "test-parse-clientid-" + System.currentTimeMillis();
        Pair<String, Long> lockId = Pair.of(clientId, zk.getSessionId());

        createLockPath(zk, lockPath);

        // Correct data
        String node1 = getLockIdFromPath(createLockNodeV1(zk, lockPath, clientId));
        String node2 = getLockIdFromPath(createLockNodeV2(zk, lockPath, clientId));
        String node3 = getLockIdFromPath(createLockNodeV3(zk, lockPath, clientId));

        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node1)));
        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node2)));
        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node3)));

        // Bad Lock Node Name
        String node4 = getLockIdFromPath(createLockNodeWithBadNodeName(zk, lockPath, clientId, "member"));
        String node5 = getLockIdFromPath(createLockNodeWithBadNodeName(zk, lockPath, clientId, "member_badnode"));
        String node6 = getLockIdFromPath(createLockNodeWithBadNodeName(zk, lockPath, clientId, "member_badnode_badnode"));
        String node7 = getLockIdFromPath(createLockNodeWithBadNodeName(zk, lockPath, clientId, "member_badnode_badnode_badnode"));
        String node8 = getLockIdFromPath(createLockNodeWithBadNodeName(zk, lockPath, clientId, "member_badnode_badnode_badnode_badnode"));

        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node4)));
        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node5)));
        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node6)));
        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node7)));
        assertEquals(lockId, Await.result(asyncParseClientID(zk, lockPath, node8)));

        // Malformed Node Name
        String node9 = getLockIdFromPath(createLockNodeWithBadNodeName(zk, lockPath, clientId, "member_malformed_s12345678_999999"));
        assertEquals(Pair.of("malformed", 12345678L), Await.result(asyncParseClientID(zk, lockPath, node9)));
    }

    @Test(timeout = 60000)
    public void testParseMemberID() throws Exception {
        assertEquals(Integer.MAX_VALUE, parseMemberID("badnode"));
        assertEquals(Integer.MAX_VALUE, parseMemberID("badnode_badnode"));
        assertEquals(0, parseMemberID("member_000000"));
        assertEquals(123, parseMemberID("member_000123"));
    }

    @Test(timeout = 60000)
    public void testExecuteLockAction() throws Exception {
        String lockPath = "/test-execute-lock-action";
        String clientId = "test-execute-lock-action-" + System.currentTimeMillis();

        DistributedLock lock = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null);

        final AtomicInteger counter = new AtomicInteger(0);

        // lock action would be executed in same epoch
        final CountDownLatch latch1 = new CountDownLatch(1);
        lock.executeLockAction(lock.getEpoch().get(), new DistributedReentrantLock.LockAction() {
            @Override
            public void execute() {
                counter.incrementAndGet();
                latch1.countDown();
            }

            @Override
            public String getActionName() {
                return "increment1";
            }
        });
        latch1.await();
        assertEquals("counter should be increased in same epoch", 1, counter.get());

        // lock action would not be executed in same epoch
        final CountDownLatch latch2 = new CountDownLatch(1);
        lock.executeLockAction(lock.getEpoch().get() + 1, new DistributedReentrantLock.LockAction() {
            @Override
            public void execute() {
                counter.incrementAndGet();
            }

            @Override
            public String getActionName() {
                return "increment2";
            }
        });
        lock.executeLockAction(lock.getEpoch().get(), new DistributedReentrantLock.LockAction() {
            @Override
            public void execute() {
                latch2.countDown();
            }

            @Override
            public String getActionName() {
                return "countdown";
            }
        });
        latch2.await();
        assertEquals("counter should not be increased in different epochs", 1, counter.get());

        // lock action would not be executed in same epoch and promise would be satisfied with exception
        Promise<BoxedUnit> promise = new Promise<BoxedUnit>();
        lock.executeLockAction(lock.getEpoch().get() + 1, new DistributedReentrantLock.LockAction() {
            @Override
            public void execute() {
                counter.incrementAndGet();
            }

            @Override
            public String getActionName() {
                return "increment3";
            }
        }, promise);
        try {
            Await.result(promise);
            fail("Should satisfy promise with epoch changed exception.");
        } catch (EpochChangedException ece) {
            // expected
        }
        assertEquals("counter should not be increased in different epochs", 1, counter.get());

        lockStateExecutor.shutdown();
    }

    /**
     * Test lock after unlock is called.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockAfterUnlock() throws Exception {
        String lockPath = "/test-lock-after-unlock";
        String clientId = "test-lock-after-unlock";

        DistributedLock lock = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null);
        lock.unlock();
        assertEquals(State.CLOSED, lock.getLockState());

        try {
            lock.tryLock(0, TimeUnit.MILLISECONDS);
            fail("Should fail on tryLock since lock state has changed.");
        } catch (LockStateChangedException lsce) {
            // expected
        }
        assertEquals(State.CLOSED, lock.getLockState());

        try {
            lock.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            fail("Should fail on tryLock immediately if lock state has changed.");
        } catch (LockStateChangedException lsce) {
            // expected
        }
        assertEquals(State.CLOSED, lock.getLockState());
    }

    class DelayFailpointAction extends FailpointUtils.AbstractFailPointAction {
        long timeout;
        DelayFailpointAction(long timeout) {
            this.timeout = timeout;
        }
        @Override
        public boolean checkFailPoint() throws IOException {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException ie) {
            }
            return true;
        }
    }

    /**
     * Test unlock timeout.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testUnlockTimeout() throws Exception {
        String name = testNames.getMethodName();
        String lockPath = "/" + name;
        String clientId = name;

        createLockPath(zkc.get(), lockPath);

        DistributedLock lock = new DistributedLock(
                zkc, lockPath, clientId, lockStateExecutor, null,
                1*1000 /* op timeout */, NullStatsLogger.INSTANCE,
                new DistributedReentrantLock.DistributedLockContext());

        lock.tryLock(0, TimeUnit.MILLISECONDS);
        assertEquals(State.CLAIMED, lock.getLockState());

        try {
            FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_LockUnlockCleanup,
                                        new DelayFailpointAction(60*60*1000));

            lock.unlock();
            assertEquals(State.CLOSED, lock.getLockState());
        } finally {
            FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_LockUnlockCleanup);
        }
    }

    /**
     * Test try acquire timeout.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testTryAcquireTimeout() throws Exception {
        String name = testNames.getMethodName();
        String lockPath = "/" + name;
        String clientId = name;

        createLockPath(zkc.get(), lockPath);

        DistributedLock lock = new DistributedLock(
                zkc, lockPath, clientId, lockStateExecutor, null,
                1 /* op timeout */, NullStatsLogger.INSTANCE,
                new DistributedReentrantLock.DistributedLockContext());

        try {
            FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_LockTryAcquire,
                                        new DelayFailpointAction(60*60*1000));

            lock.tryLock(0, TimeUnit.MILLISECONDS);
            assertEquals(State.CLOSED, lock.getLockState());
        } catch (LockingException le) {
        } catch (Exception e) {
            fail("expected locking exception");
        } finally {
            FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_LockTryAcquire);
        }
    }

    @Test(timeout = 60000)
    public void testBasicLockUnlock0() throws Exception {
        testBasicLockUnlock(0);
    }

    @Test(timeout = 60000)
    public void testBasicLockUnlock1() throws Exception {
        testBasicLockUnlock(Long.MAX_VALUE);
    }

    /**
     * Test Basic Lock and Unlock
     *
     * - lock should succeed if there is no lock held
     * - lock should fail on a success lock
     * - unlock should release the held lock
     *
     * @param timeout
     *          timeout to wait for the lock
     * @throws Exception
     */
    private void testBasicLockUnlock(long timeout) throws Exception {
        String lockPath = "/test-basic-lock-unlock-" + timeout + System.currentTimeMillis();
        String clientId = "test-basic-lock-unlock";

        createLockPath(zkc.get(), lockPath);

        DistributedLock lock = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null);
        // lock
        lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        // verification after lock
        assertEquals(State.CLAIMED, lock.getLockState());
        List<String> children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));

        // lock should fail on a success lock
        try {
            lock.tryLock(timeout, TimeUnit.MILLISECONDS);
            fail("Should fail on locking a failure lock.");
        } catch (LockStateChangedException lsce) {
            // expected
        }
        assertEquals(State.CLAIMED, lock.getLockState());
        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));

        // unlock
        lock.unlock();
        // verification after unlock
        assertEquals(State.CLOSED, lock.getLockState());
        assertEquals(0, getLockWaiters(zkc, lockPath).size());
    }

    /**
     * Test lock on non existed lock.
     *
     * - lock should fail on a non existed lock.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockOnNonExistedLock() throws Exception {
        String lockPath = "/test-lock-on-non-existed-lock";
        String clientId = "test-lock-on-non-existed-lock";

        DistributedLock lock = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null);
        // lock
        try {
            lock.tryLock(0, TimeUnit.MILLISECONDS);
            fail("Should fail on locking a non-existed lock.");
        } catch (LockingException le) {
            Throwable cause = le.getCause();
            assertTrue(cause instanceof KeeperException);
            assertEquals(KeeperException.Code.NONODE, ((KeeperException) cause).code());
        }
        assertEquals(State.CLOSED, lock.getLockState());

        // lock should failed on a failure lock
        try {
            lock.tryLock(0, TimeUnit.MILLISECONDS);
            fail("Should fail on locking a failure lock.");
        } catch (LockStateChangedException lsce) {
            // expected
        }
        assertEquals(State.CLOSED, lock.getLockState());
    }

    @Test(timeout = 60000)
    public void testLockWhenSomeoneHeldLock0() throws Exception {
        testLockWhenSomeoneHeldLock(0);
    }

    @Test(timeout = 60000)
    public void testLockWhenSomeoneHeldLock1() throws Exception {
        testLockWhenSomeoneHeldLock(500);
    }

    /**
     * Test lock if the lock is already held by someone else. Any lock in this situation will
     * fail with current owner.
     *
     * @param timeout
     *          timeout to wait for the lock
     * @throws Exception
     */
    private void testLockWhenSomeoneHeldLock(long timeout) throws Exception {
        String lockPath = "/test-lock-nowait-" + timeout + "-" + System.currentTimeMillis();
        String clientId0 = "test-lock-nowait-0-" + System.currentTimeMillis();
        String clientId1 = "test-lock-nowait-1-" + System.currentTimeMillis();
        String clientId2 = "test-lock-nowait-2-" + System.currentTimeMillis();

        createLockPath(zkc.get(), lockPath);

        DistributedLock lock0 = new DistributedLock(zkc0, lockPath, clientId0, lockStateExecutor, null);
        DistributedLock lock1 = new DistributedLock(zkc, lockPath, clientId1, lockStateExecutor, null);

        lock0.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        // verification after lock0 lock
        assertEquals(State.CLAIMED, lock0.getLockState());
        List<String> children = getLockWaiters(zkc0, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        try {
            lock1.tryLock(timeout, TimeUnit.MILLISECONDS);
            fail("lock1 should fail on locking since lock0 is holding the lock.");
        } catch (OwnershipAcquireFailedException oafe) {
            assertEquals(lock0.getLockId().getLeft(), oafe.getCurrentOwner());
        }
        // verification after lock1 tryLock
        assertEquals(State.CLAIMED, lock0.getLockState());
        assertEquals(State.CLOSED, lock1.getLockState());
        children = getLockWaiters(zkc0, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        lock0.unlock();
        // verification after unlock lock0
        assertEquals(State.CLOSED, lock0.getLockState());
        assertEquals(0, getLockWaiters(zkc, lockPath).size());

        DistributedLock lock2 = new DistributedLock(zkc, lockPath, clientId2, lockStateExecutor, null);
        lock2.tryLock(timeout, TimeUnit.MILLISECONDS);
        // verification after lock2 lock
        assertEquals(State.CLOSED, lock0.getLockState());
        assertEquals(State.CLOSED, lock1.getLockState());
        assertEquals(State.CLAIMED, lock2.getLockState());
        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock2.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));

        lock2.unlock();
    }

    @Test(timeout = 60000)
    public void testLockWhenPreviousLockZnodeStillExists() throws Exception {
        String lockPath = "/test-lock-when-previous-lock-znode-still-exists-" +
                System.currentTimeMillis();
        String clientId = "client-id";

        ZooKeeper zk = zkc.get();

        createLockPath(zk, lockPath);

        final DistributedLock lock0 = new DistributedLock(zkc0, lockPath, clientId, lockStateExecutor, null);
        // lock0 lock
        lock0.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        // simulate lock0 expires but znode still exists
        final DistributedReentrantLock.DistributedLockContext context1 =
                new DistributedReentrantLock.DistributedLockContext();
        context1.addLockId(lock0.getLockId());

        final DistributedLock lock1 = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null,
                60000, NullStatsLogger.INSTANCE, context1);
        lock1.tryLock(0L, TimeUnit.MILLISECONDS);
        assertEquals(State.CLAIMED, lock1.getLockState());
        lock1.unlock();

        final DistributedReentrantLock.DistributedLockContext context2 =
                new DistributedReentrantLock.DistributedLockContext();
        context2.addLockId(lock0.getLockId());

        final DistributedLock lock2 = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null,
                60000, NullStatsLogger.INSTANCE, context2);
        lock2.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(State.CLAIMED, lock2.getLockState());
        lock2.unlock();

        lock0.unlock();
    }

    @Test(timeout = 60000)
    public void testWaitForLockUnlock() throws Exception {
        testWaitForLockReleased("/test-wait-for-lock-unlock", true);
    }

    @Test(timeout = 60000)
    public void testWaitForLockExpired() throws Exception {
        testWaitForLockReleased("/test-wait-for-lock-expired", false);
    }

    /**
     * Test lock wait for the lock owner to release the lock. The lock waiter should acquire lock successfully
     * if the lock owner unlock or it is expired.
     *
     * @param lockPath
     *          lock path
     * @param isUnlock
     *          whether to unlock or expire the lock
     * @throws Exception
     */
    private void testWaitForLockReleased(String lockPath, boolean isUnlock) throws Exception {
        String clientId0 = "test-wait-for-lock-released-0-" + System.currentTimeMillis();
        String clientId1 = "test-wait-for-lock-released-1-" + System.currentTimeMillis();

        createLockPath(zkc.get(), lockPath);

        final DistributedLock lock0 = new DistributedLock(zkc0, lockPath, clientId0, lockStateExecutor, null);
        final DistributedLock lock1 = new DistributedLock(zkc, lockPath, clientId1, lockStateExecutor, null);

        lock0.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        // verification after lock0 lock
        assertEquals(State.CLAIMED, lock0.getLockState());
        List<String> children = getLockWaiters(zkc0, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        final CountDownLatch lock1DoneLatch = new CountDownLatch(1);

        Thread lock1Thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    lock1DoneLatch.countDown();
                } catch (LockingException e) {
                    logger.error("Failed on locking lock1 : ", e);
                }
            }
        }, "lock1-thread");
        lock1Thread.start();

        // ensure lock1 is waiting for lock0
        children = awaitWaiters(2, zkc, lockPath);

        if (isUnlock) {
            lock0.unlock();
        } else {
            ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);
        }

        lock1DoneLatch.await();
        lock1Thread.join();

        // verification after lock2 lock
        if (isUnlock) {
            assertEquals(State.CLOSED, lock0.getLockState());
        } else {
            assertEquals(State.EXPIRED, lock0.getLockState());
        }
        assertEquals(State.CLAIMED, lock1.getLockState());
        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));

        lock1.unlock();
    }

    /**
     * Test session expired after claimed the lock: lock state should be changed to expired and notify
     * the lock listener about expiry.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockListenerOnExpired() throws Exception {
        String lockPath = "/test-lock-listener-on-expired";
        String clientId = "test-lock-listener-on-expired-" + System.currentTimeMillis();

        createLockPath(zkc.get(), lockPath);

        final CountDownLatch expiredLatch = new CountDownLatch(1);
        DistributedReentrantLock.LockListener listener = new DistributedReentrantLock.LockListener() {
            @Override
            public void onExpired() {
                expiredLatch.countDown();
            }
        };
        final DistributedLock lock = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, listener);
        lock.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        // verification after lock
        assertEquals(State.CLAIMED, lock.getLockState());
        List<String> children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));

        ZooKeeperClientUtils.expireSession(zkc, zkServers, sessionTimeoutMs);
        expiredLatch.await();
        assertEquals(State.EXPIRED, lock.getLockState());
        children = getLockWaiters(zkc, lockPath);
        assertEquals(0, children.size());

        try {
            lock.tryLock(0, TimeUnit.MILLISECONDS);
            fail("Should fail on tryLock since lock state has changed.");
        } catch (LockStateChangedException lsce) {
            // expected
        }

        lock.unlock();
    }

    @Test(timeout = 60000)
    public void testSessionExpiredBeforeLock0() throws Exception {
        testSessionExpiredBeforeLock(0);
    }

    @Test(timeout = 60000)
    public void testSessionExpiredBeforeLock1() throws Exception {
        testSessionExpiredBeforeLock(Long.MAX_VALUE);
    }

    /**
     * Test Session Expired Before Lock does locking. The lock should be closed since
     * all zookeeper operations would be failed.
     *
     * @param timeout
     *          timeout to wait for the lock
     * @throws Exception
     */
    private void testSessionExpiredBeforeLock(long timeout) throws Exception {
        String lockPath = "/test-session-expired-before-lock-" + timeout + "-" + System.currentTimeMillis();
        String clientId = "test-session-expired-before-lock-" + System.currentTimeMillis();

        createLockPath(zkc.get(), lockPath);
        final AtomicInteger expireCounter = new AtomicInteger(0);
        final CountDownLatch expiredLatch = new CountDownLatch(1);
        DistributedReentrantLock.LockListener listener = new DistributedReentrantLock.LockListener() {
            @Override
            public void onExpired() {
                expireCounter.incrementAndGet();
            }
        };
        final DistributedLock lock = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, listener);
        // expire session
        ZooKeeperClientUtils.expireSession(zkc, zkServers, sessionTimeoutMs);
        // submit a runnable to lock state executor to ensure any state changes happened when session expired
        lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
            @Override
            public void safeRun() {
                expiredLatch.countDown();
            }
        });
        expiredLatch.await();
        // no watcher was registered if never acquired lock successfully
        assertEquals(State.INIT, lock.getLockState());
        try {
            lock.tryLock(timeout, TimeUnit.MILLISECONDS);
            fail("Should fail locking using an expired lock");
        } catch (LockingException le) {
            assertTrue(le.getCause() instanceof KeeperException.SessionExpiredException);
        }
        assertEquals(State.CLOSED, lock.getLockState());
        List<String> children = getLockWaiters(zkc, lockPath);
        assertEquals(0, children.size());
    }

    @Test(timeout = 60000)
    public void testSessionExpiredForLockWaiter() throws Exception {
        String lockPath = "/test-session-expired-for-lock-waiter";
        String clientId0 = "test-session-expired-for-lock-waiter-0";
        String clientId1 = "test-session-expired-for-lock-waiter-1";

        createLockPath(zkc.get(), lockPath);

        final DistributedLock lock0 = new DistributedLock(zkc0, lockPath, clientId0, lockStateExecutor, null);
        lock0.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(State.CLAIMED, lock0.getLockState());
        List<String> children = getLockWaiters(zkc0, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        final DistributedLock lock1 = new DistributedLock(zkc, lockPath, clientId1, lockStateExecutor, null);
        final CountDownLatch lock1DoneLatch = new CountDownLatch(1);

        Thread lock1Thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                } catch (OwnershipAcquireFailedException oafe) {
                    lock1DoneLatch.countDown();
                } catch (LockingException e) {
                    logger.error("Failed on locking lock1 : ", e);
                }
            }
        }, "lock1-thread");
        lock1Thread.start();

        // check lock1 is waiting for lock0
        children = awaitWaiters(2, zkc, lockPath);

        assertEquals(2, children.size());
        assertEquals(State.CLAIMED, lock0.getLockState());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
        awaitState(State.WAITING, lock1);
        assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(1))));

        // expire lock1
        ZooKeeperClientUtils.expireSession(zkc, zkServers, sessionTimeoutMs);

        lock1DoneLatch.countDown();
        lock1Thread.join();
        assertEquals(State.CLAIMED, lock0.getLockState());
        assertEquals(State.EXPIRED, lock1.getLockState());
        children = getLockWaiters(zkc0, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
    }

    public void awaitState(State state, DistributedLock lock) throws InterruptedException {
        while (lock.getLockState() != state) {
            Thread.sleep(50);
        }
    }

    public List<String> awaitWaiters(int waiters, ZooKeeperClient zkc, String lockPath) throws Exception {
        List<String> children = getLockWaiters(zkc, lockPath);
        while (children.size() < waiters) {
            Thread.sleep(50);
            children = getLockWaiters(zkc, lockPath);
        }
        return children;
    }

    @Test(timeout = 60000)
    public void testLockUseSameClientIdButDifferentSessions0() throws Exception {
        testLockUseSameClientIdButDifferentSessions(true);
    }

    @Test(timeout = 60000)
    public void testLockUseSameClientIdButDifferentSessions1() throws Exception {
        testLockUseSameClientIdButDifferentSessions(false);
    }

    private void testLockUseSameClientIdButDifferentSessions(boolean isUnlock) throws Exception {
        String lockPath = "/test-lock-use-same-client-id-but-different-sessions-" + isUnlock + System.currentTimeMillis();
        String clientId = "test-lock-use-same-client-id-but-different-sessions";

        createLockPath(zkc.get(), lockPath);

        final DistributedLock lock0 = new DistributedLock(zkc0, lockPath, clientId, lockStateExecutor, null);

        lock0.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        // lock1_0 couldn't claim ownership since owner is in a different zk session.
        final DistributedLock lock1_0 = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null);
        try {
            lock1_0.tryLock(0, TimeUnit.MILLISECONDS);
            fail("Should fail locking since the lock is held in a different zk session.");
        } catch (OwnershipAcquireFailedException oafe) {
            assertEquals(clientId, oafe.getCurrentOwner());
        }
        assertEquals(State.CLOSED, lock1_0.getLockState());
        List<String> children = getLockWaiters(zkc0, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        // lock1_1 would wait the ownership
        final DistributedLock lock1_1 = new DistributedLock(zkc, lockPath, clientId, lockStateExecutor, null);
        final CountDownLatch lock1DoneLatch = new CountDownLatch(1);

        Thread lock1Thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1_1.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    lock1DoneLatch.countDown();
                } catch (LockingException e) {
                    logger.error("Failed on locking lock1 : ", e);
                }
            }
        }, "lock1-thread");
        lock1Thread.start();

        // check lock1 is waiting for lock0
        children = awaitWaiters(2, zkc, lockPath);

        logger.info("Found {} lock waiters : {}", children.size(), children);

        assertEquals(2, children.size());
        assertEquals(State.CLAIMED, lock0.getLockState());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
        awaitState(State.WAITING, lock1_1);
        assertEquals(lock1_1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(1))));

        if (isUnlock) {
            lock0.unlock();
        } else {
            ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);
        }
        lock1DoneLatch.await();
        lock1Thread.join();

        // verification
        if (isUnlock) {
            assertEquals(State.CLOSED, lock0.getLockState());
        } else {
            assertEquals(State.EXPIRED, lock0.getLockState());
        }
        assertEquals(State.CLAIMED, lock1_1.getLockState());
        children = getLockWaiters(zkc, lockPath);
        assertEquals(1, children.size());
        assertEquals(lock1_1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));

        lock1_1.unlock();
    }

    /**
     * Immediate lock and unlock first lock
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockWhenSiblingUseDifferentLockId0() throws Exception {
        testLockWhenSiblingUseDifferentLockId(0, true);
    }

    /**
     * Immediate lock and expire first lock
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockWhenSiblingUseDifferentLockId1() throws Exception {
        testLockWhenSiblingUseDifferentLockId(0, false);
    }

    /**
     * Wait Lock and unlock lock0_0 and lock1
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockWhenSiblingUseDifferentLockId2() throws Exception {
        testLockWhenSiblingUseDifferentLockId(Long.MAX_VALUE, true);
    }

    /**
     * Wait Lock and expire first & third lock
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLockWhenSiblingUseDifferentLockId3() throws Exception {
        testLockWhenSiblingUseDifferentLockId(Long.MAX_VALUE, false);
    }

    private void testLockWhenSiblingUseDifferentLockId(long timeout, final boolean isUnlock) throws Exception {
        String lockPath = "/test-lock-when-sibling-use-different-lock-id-" + timeout
                + "-" + isUnlock + "-" + System.currentTimeMillis();
        String clientId0 = "client-id-0";
        String clientId1 = "client-id-1";

        createLockPath(zkc.get(), lockPath);

        final DistributedLock lock0_0 = new DistributedLock(zkc0, lockPath, clientId0, lockStateExecutor, null);
        final DistributedLock lock0_1 = new DistributedLock(zkc0, lockPath, clientId0, lockStateExecutor, null);
        final DistributedLock lock1   = new DistributedLock(zkc, lockPath, clientId1, lockStateExecutor, null);

        lock0_0.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        // lock1 wait for the lock ownership.
        final CountDownLatch lock1DoneLatch = new CountDownLatch(1);
        Thread lock1Thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    lock1DoneLatch.countDown();
                } catch (LockingException e) {
                    logger.error("Failed on locking lock1 : ", e);
                }
            }
        }, "lock1-thread");
        lock1Thread.start();

        // check lock1 is waiting for lock0_0
        List<String> children = awaitWaiters(2, zkc, lockPath);

        assertEquals(2, children.size());
        assertEquals(State.CLAIMED, lock0_0.getLockState());
        assertEquals(lock0_0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
        awaitState(State.WAITING, lock1);
        assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(1))));

        final CountDownLatch lock0DoneLatch = new CountDownLatch(1);
        final AtomicReference<String> ownerFromLock0 = new AtomicReference<String>(null);
        Thread lock0Thread = null;
        if (timeout == 0) {
            try {
                lock0_1.tryLock(0, TimeUnit.MILLISECONDS);
                fail("Should fail on locking if sibling is using differnt lock id.");
            } catch (OwnershipAcquireFailedException oafe) {
                assertEquals(clientId0, oafe.getCurrentOwner());
            }
            assertEquals(State.CLOSED, lock0_1.getLockState());
            children = getLockWaiters(zkc, lockPath);
            assertEquals(2, children.size());
            assertEquals(State.CLAIMED, lock0_0.getLockState());
            assertEquals(lock0_0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
            assertEquals(State.WAITING, lock1.getLockState());
            assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(1))));
        } else {
            lock0Thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        lock0_1.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                        if (isUnlock) {
                            lock0DoneLatch.countDown();
                        }
                    } catch (OwnershipAcquireFailedException oafe) {
                        if (!isUnlock) {
                            ownerFromLock0.set(oafe.getCurrentOwner());
                            lock0DoneLatch.countDown();
                        }
                    } catch (LockingException le) {
                        logger.error("Failed on locking lock0_1 : ", le);
                    }
                }
            }, "lock0-thread");
            lock0Thread.start();

            // check lock1 is waiting for lock0_0
            children = awaitWaiters(3, zkc, lockPath);

            assertEquals(3, children.size());
            assertEquals(State.CLAIMED, lock0_0.getLockState());
            assertEquals(lock0_0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
            awaitState(State.WAITING, lock1);
            assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(1))));
            awaitState(State.WAITING, lock0_1);
            assertEquals(lock0_1.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(2))));
        }

        if (isUnlock) {
            lock0_0.unlock();
        } else {
            ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);
        }

        lock1DoneLatch.await();
        lock1Thread.join();

        // check the state of lock0_0
        if (isUnlock) {
            assertEquals(State.CLOSED, lock0_0.getLockState());
        } else {
            assertEquals(State.EXPIRED, lock0_0.getLockState());
        }

        if (timeout == 0) {
            children = getLockWaiters(zkc, lockPath);
            assertEquals(1, children.size());
            assertEquals(State.CLAIMED, lock1.getLockState());
            assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));
        } else {
            assertNotNull(lock0Thread);
            if (!isUnlock) {
                // both lock0_0 and lock0_1 would be expired
                lock0DoneLatch.await();
                lock0Thread.join();

                assertEquals(clientId0, ownerFromLock0.get());
                assertEquals(State.EXPIRED, lock0_1.getLockState());

                children = getLockWaiters(zkc, lockPath);
                assertEquals(1, children.size());
                assertEquals(State.CLAIMED, lock1.getLockState());
                assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));
            } else {
                children = getLockWaiters(zkc, lockPath);
                assertEquals(2, children.size());
                assertEquals(State.CLAIMED, lock1.getLockState());
                assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc.get(), lockPath, children.get(0))));
                assertEquals(State.WAITING, lock0_1.getLockState());
                assertEquals(lock0_1.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(1))));
            }
        }

        lock1.unlock();

        if (timeout != 0 && isUnlock) {
            lock0DoneLatch.await();
            lock0Thread.join();

            children = getLockWaiters(zkc, lockPath);
            assertEquals(1, children.size());
            assertEquals(State.CLAIMED, lock0_1.getLockState());
            assertEquals(lock0_1.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
        }
    }

    @Test(timeout = 60000)
    public void testLockWhenSiblingUseSameLockId0() throws Exception {
        testLockWhenSiblingUseSameLockId(0, true);
    }

    @Test(timeout = 60000)
    public void testLockWhenSiblingUseSameLockId1() throws Exception {
        testLockWhenSiblingUseSameLockId(0, false);
    }

    @Test(timeout = 60000)
    public void testLockWhenSiblingUseSameLockId2() throws Exception {
        testLockWhenSiblingUseSameLockId(Long.MAX_VALUE, true);
    }

    @Test(timeout = 60000)
    public void testLockWhenSiblingUseSameLockId3() throws Exception {
        testLockWhenSiblingUseSameLockId(Long.MAX_VALUE, false);
    }

    private void testLockWhenSiblingUseSameLockId(long timeout, final boolean isUnlock) throws Exception {
        String lockPath = "/test-lock-when-sibling-use-same-lock-id-" + timeout
                + "-" + isUnlock + "-" + System.currentTimeMillis();
        String clientId = "client-id";

        createLockPath(zkc.get(), lockPath);

        final DistributedLock lock0 = new DistributedLock(zkc0, lockPath, clientId, lockStateExecutor, null);
        final DistributedLock lock1 = new DistributedLock(zkc0, lockPath, clientId, lockStateExecutor, null);

        lock0.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        List<String> children = getLockWaiters(zkc0, lockPath);
        assertEquals(1, children.size());
        assertEquals(State.CLAIMED, lock0.getLockState());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));

        lock1.tryLock(timeout, TimeUnit.MILLISECONDS);
        children = getLockWaiters(zkc0, lockPath);
        assertEquals(2, children.size());
        assertEquals(State.CLAIMED, lock0.getLockState());
        assertEquals(lock0.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
        assertEquals(State.CLAIMED, lock1.getLockState());
        assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(1))));

        if (isUnlock) {
            lock0.unlock();
            assertEquals(State.CLOSED, lock0.getLockState());
            children = getLockWaiters(zkc0, lockPath);
            assertEquals(1, children.size());
            assertEquals(State.CLAIMED, lock1.getLockState());
            assertEquals(lock1.getLockId(), Await.result(asyncParseClientID(zkc0.get(), lockPath, children.get(0))));
            lock1.unlock();
        } else {
            ZooKeeperClientUtils.expireSession(zkc0, zkServers, sessionTimeoutMs);
            final CountDownLatch latch = new CountDownLatch(1);
            lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                @Override
                public void safeRun() {
                    latch.countDown();
                }
            });
            latch.await();
            children = getLockWaiters(zkc, lockPath);
            assertEquals(0, children.size());
            assertEquals(State.EXPIRED, lock0.getLockState());
            assertEquals(State.EXPIRED, lock1.getLockState());
        }
    }

}
