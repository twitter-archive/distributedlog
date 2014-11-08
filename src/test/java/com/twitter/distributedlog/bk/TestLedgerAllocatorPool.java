package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.jvm.numProcs;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestLedgerAllocatorPool extends TestDistributedLogBase {

    private static final String zkServers = "127.0.0.1:7000";
    private static final String ledgersPath = "/ledgers";

    private ZooKeeperClient zkc;
    private BookKeeperClient bkc;
    private DistributedLogConfiguration dlConf = new DistributedLogConfiguration();
    private ScheduledExecutorService allocationExecutor;

    private URI createURI(String path) {
        return URI.create("distributedlog://" + zkServers + path);
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder().uri(createURI("/"))
                .sessionTimeoutMs(10000).zkAclId(null).build();
        bkc = BookKeeperClientBuilder.newBuilder().name("bkc")
                .dlConfig(dlConf).ledgersPath(ledgersPath).zkc(zkc).build();
        allocationExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void teardown() throws Exception {
        bkc.close();
        zkc.close();
        allocationExecutor.shutdown();
    }

    private void validatePoolSize(LedgerAllocatorPool pool,
                                  int pendingSize,
                                  int allocatingSize,
                                  int obtainingSize,
                                  int rescueSize) {
        assertEquals(pendingSize, pool.pendingListSize());
        assertEquals(allocatingSize, pool.allocatingListSize());
        assertEquals(obtainingSize, pool.obtainMapSize());
        assertEquals(rescueSize, pool.rescueSize());
    }

    @Test(timeout = 60000)
    public void testNonAvailableAllocator() throws Exception {
        String allocationPath = "/nonAvailableAllocator";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(dlConf);
        confLocal.setEnsembleSize(2 * numBookies);
        confLocal.setWriteQuorumSize(2 * numBookies);

        int numAllocators = 3;
        LedgerAllocatorPool pool =
                new LedgerAllocatorPool(allocationPath, numAllocators, confLocal, zkc, bkc, allocationExecutor);
        for (int i = 0; i < numAllocators; i++) {
            try {
                pool.allocate();
                pool.tryObtain(zkc.get().transaction());
                fail("Should fail to allocate ledger if there are enought bookies");
            } catch (SimpleLedgerAllocator.AllocationException ae) {
                assertEquals(SimpleLedgerAllocator.Phase.ERROR, ae.getPhase());
            }
        }
        for (int i = 0; i < numAllocators; i++) {
            try {
                pool.allocate();
                pool.tryObtain(zkc.get().transaction());
                fail("Should fail to allocate ledger if there aren't available allocators");
            } catch (SimpleLedgerAllocator.AllocationException ae) {
                assertEquals(SimpleLedgerAllocator.Phase.ERROR, ae.getPhase());
            } catch (IOException ioe) {
                // expected
            }
        }
        pool.close(true);
    }

    @Test(timeout = 60000)
    public void testRescueAllocators() throws Exception {
        String allocationPath = "/rescueAllocators";

        int numAllocators = 3;
        LedgerAllocatorPool pool =
                new LedgerAllocatorPool(allocationPath, numAllocators, dlConf, zkc, bkc, allocationExecutor);
        for (int i = 0; i < numAllocators; i++) {
            Transaction txn = zkc.get().transaction();
            pool.allocate();
            LedgerHandle lh = pool.tryObtain(txn);
            List<OpResult> opResults = txn.commit();

            // introduce error to individual ledger allocator
            SimpleLedgerAllocator sla = pool.getLedgerAllocator(lh);
            String slaPath = sla.allocatePath;
            byte[] data = zkc.get().getData(slaPath, false, new Stat());
            zkc.get().setData(slaPath, data, -1);

            // confirm previous allocation
            pool.confirmObtain(lh, opResults.get(0));
        }
        // all allocators will be set to error because allocation path is changed.
        for (int i = 0; i < numAllocators; i++) {
            try {
                pool.allocate();
                pool.tryObtain(zkc.get().transaction());
                fail("Should fail to allocate ledger if there aren't available allocators");
            } catch (SimpleLedgerAllocator.AllocationException ae) {
                assertEquals(SimpleLedgerAllocator.Phase.ERROR, ae.getPhase());
            }
        }
        // wait for all allocators being rescued
        while (pool.pendingListSize() < numAllocators) {
            Thread.sleep(100);
        }
        // all allocators should be rescued
        for (int i = 0; i < 2 * numAllocators; i++) {
            Transaction txn = zkc.get().transaction();
            pool.allocate();
            LedgerHandle lh = pool.tryObtain(txn);
            List<OpResult> opResults = txn.commit();

            // confirm previous allocation
            pool.confirmObtain(lh, opResults.get(0));
        }
        pool.close(true);
    }

    @Test(timeout = 60000)
    public void testAllocateWhenNoAllocator() throws Exception {
        String allocationPath = "/allocateWhenNoAllocator";
        LedgerAllocatorPool pool = new LedgerAllocatorPool(allocationPath, 0, dlConf, zkc, bkc, allocationExecutor);
        try {
            pool.allocate();
            fail("Should fail to allocate ledger if there isn't allocator.");
        } catch (SimpleLedgerAllocator.AllocationException ae) {
            fail("Should fail to allocate ledger if there isn't allocator.");
        } catch (IOException ioe) {
            // expected
        }
        pool.close(true);
    }

    @Test(timeout = 60000)
    public void testObtainWhenNoAllocator() throws Exception {
        String allocationPath = "/obtainWhenNoAllocator";
        LedgerAllocatorPool pool = new LedgerAllocatorPool(allocationPath, 0, dlConf, zkc, bkc, allocationExecutor);
        Transaction txn = zkc.get().transaction();
        try {
            pool.tryObtain(txn);
            fail("Should fail obtain ledger handle if there is no allocator.");
        } catch (SimpleLedgerAllocator.AllocationException ae) {
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
        final LedgerAllocatorPool pool =
                new LedgerAllocatorPool(allocationPath, numAllocators, dlConf, zkc, bkc, allocationExecutor);
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
