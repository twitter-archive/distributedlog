package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestLedgerAllocatorPool {

    private static final String zkServers = "127.0.0.1:7000";
    private static final String ledgersPath = "/ledgers";

    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numBookies = 3;

    private ZooKeeperClient zkc;
    private BookKeeperClient bkc;
    private DistributedLogConfiguration dlConf = new DistributedLogConfiguration();

    @BeforeClass
    public static void setupBookKeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownBookKeeper() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    private URI createURI(String path) {
        return URI.create("distributedlog://" + zkServers + path);
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder().uri(createURI("/"))
                .sessionTimeoutMs(10000).zkAclId(null).build();
        bkc = BookKeeperClientBuilder.newBuilder().name("bkc")
                .dlConfig(dlConf).ledgersPath(ledgersPath).zkc(zkc).build();
    }

    @After
    public void teardown() throws Exception {
        bkc.close();
        zkc.close();
    }

    private void validatePoolSize(LedgerAllocatorPool pool, int pendingSize, int allocatingSize, int obtainingSize) {
        assertEquals(pendingSize, pool.pendingListSize());
        assertEquals(allocatingSize, pool.allocatingListSize());
        assertEquals(obtainingSize, pool.obtainMapSize());
    }

    @Test(timeout = 60000)
    public void testCreateAllocatorIfNotEnough() throws Exception {
        String allocationPath = "/createAllocatorIfNotEnough";
        LedgerAllocatorPool pool = new LedgerAllocatorPool(allocationPath, 0, dlConf, zkc, bkc);
        validatePoolSize(pool, 0, 0, 0);
        pool.allocate();
        validatePoolSize(pool, 0, 1, 0);
        Transaction txn = zkc.get().transaction();
        LedgerHandle lh = pool.tryObtain(txn);
        validatePoolSize(pool, 0, 0, 1);
        txn.setData("/unexistedpath", "data".getBytes(UTF_8), -1);
        try {
            txn.commit();
        } catch (KeeperException ke) {
            // abort the obtain
            pool.abortObtain(lh);
        }
        validatePoolSize(pool, 1, 0, 0);

        // new transaction to obtain the ledger again
        txn = zkc.get().transaction();
        pool.allocate();
        validatePoolSize(pool, 0, 1, 0);
        LedgerHandle newLh = pool.tryObtain(txn);
        validatePoolSize(pool, 0, 0, 1);
        assertEquals(lh.getId(), newLh.getId());
        assertTrue(lh == newLh);
        List<OpResult> results = txn.commit();
        // confirm the obtain
        pool.confirmObtain(newLh, results.get(0));
        validatePoolSize(pool, 1, 0, 0);

        pool.close(true);
    }

    @Test(timeout = 60000)
    public void testObtainWhenNoAllocator() throws Exception {
        String allocationPath = "/obtainWhenNoAllocator";
        LedgerAllocatorPool pool = new LedgerAllocatorPool(allocationPath, 0, dlConf, zkc, bkc);
        Transaction txn = zkc.get().transaction();
        try {
            pool.tryObtain(txn);
            fail("Should fail obtain ledger handle if there is no allocator.");
        } catch (IOException ioe) {
            // expected.
        }

        pool.close(true);
    }

    @Test(timeout = 60000)
    public void testConcurrentAllocation() throws Exception {
        final int numAllocators = 5;
        String allocationPath = "/concurrentAllocation";
        final LedgerAllocatorPool pool = new LedgerAllocatorPool(allocationPath, numAllocators, dlConf, zkc, bkc);

        final ConcurrentMap<Long, LedgerHandle> allocatedLedgers =
                new ConcurrentHashMap<Long, LedgerHandle>();
        final AtomicInteger numFailures = new AtomicInteger(0);
        Thread[] allocationThreads = new Thread[numAllocators];
        for (int i = 0; i < numAllocators; i++) {
            allocationThreads[i] = new Thread() {

                int numLedgers = 50;

                @Override
                public void run() {
                    try {
                        for (int i = 0; i < numLedgers; i++) {
                            pool.allocate();
                            Transaction txn = zkc.get().transaction();
                            LedgerHandle lh = pool.tryObtain(txn);
                            List<OpResult> results = txn.commit();
                            pool.confirmObtain(lh, results.get(0));
                            lh.close();
                            allocatedLedgers.putIfAbsent(lh.getId(), lh);
                        }
                    } catch (Exception ioe) {
                        numFailures.incrementAndGet();
                    }
                }
            };
        }

        for (Thread t : allocationThreads) {
            t.start();
        }

        for (Thread t : allocationThreads) {
            t.join();
        }

        assertEquals(0, numFailures.get());
        assertEquals(50 * numAllocators, allocatedLedgers.size());

        pool.close(true);
    }

}
