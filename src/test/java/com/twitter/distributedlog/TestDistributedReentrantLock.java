package com.twitter.distributedlog;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;

import static org.junit.Assert.assertEquals;

public class TestDistributedReentrantLock {
    static final Logger LOG = LoggerFactory.getLogger(TestAsyncReaderWriter.class);

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration().setLockTimeout(10);
    private ZooKeeperClient zkc;
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numBookies = 3;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
            .connectionTimeoutMs(30000)
            .sessionTimeoutMs(30000)
            .uri(DLMTestUtil.createDLMURI(""))
            .build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test
    public void testLockReacquire() throws Exception {
        String lockPath = "/reacquirePath";
        Utils.zkCreateFullPathOptimistic(zkc, lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        String clientId = "lockHolder";
        DistributedReentrantLock lock = new DistributedReentrantLock(Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("TestLock-executor-%d").build()), zkc, lockPath,
            conf.getLockTimeoutMilliSeconds(), clientId, NullStatsLogger.INSTANCE);
        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);

        // try and cleanup the underlying lock
        lock.getInternalLock().unlock();

        // This should reacquire the lock
        lock.checkWriteLock(true, DistributedReentrantLock.LockReason.WRITEHANDLER);

        assertEquals(lock.haveLock(), true);
        assertEquals(lock.getInternalLock().isLockHeld(), true);

        DistributedReentrantLock lock2 = new DistributedReentrantLock(Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("TestLock-executor-%d").build()), zkc, lockPath,
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

    @Test
    public void testLockReacquireMultiple() throws Exception {
        String lockPath = "/reacquirePathMultiple";
        Utils.zkCreateFullPathOptimistic(zkc, lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        String clientId = "lockHolder";
        DistributedReentrantLock lock = new DistributedReentrantLock(Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("TestLock-executor-%d").build()), zkc, lockPath,
            conf.getLockTimeoutMilliSeconds(), clientId, NullStatsLogger.INSTANCE);
        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lock.acquire(DistributedReentrantLock.LockReason.RECOVER);

        // try and cleanup the underlying lock
        lock.getInternalLock().unlock();

        // This should reacquire the lock
        lock.checkWriteLock(true, DistributedReentrantLock.LockReason.WRITEHANDLER);

        assertEquals(lock.haveLock(), true);
        assertEquals(lock.getInternalLock().isLockHeld(), true);

        DistributedReentrantLock lock2 = new DistributedReentrantLock(Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("TestLock-executor-%d").build()), zkc, lockPath,
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

        DistributedReentrantLock lock3 = new DistributedReentrantLock(Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("TestLock-executor-%d").build()), zkc, lockPath,
            0, clientId + "_3", NullStatsLogger.INSTANCE);

        lock3.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        assertEquals(lock3.haveLock(), true);
        assertEquals(lock3.getInternalLock().isLockHeld(), true);
        lock3.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lock3.close();
    }
}
