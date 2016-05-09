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
package com.twitter.distributedlog.bk;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Transaction.OpListener;
import com.twitter.distributedlog.util.Utils;
import com.twitter.distributedlog.zk.ZKTransaction;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestLedgerAllocatorPool extends TestDistributedLogBase {

    private static final Logger logger = LoggerFactory.getLogger(TestLedgerAllocatorPool.class);

    private static final String ledgersPath = "/ledgers";
    private static final OpListener<LedgerHandle> NULL_LISTENER = new OpListener<LedgerHandle>() {
        @Override
        public void onCommit(LedgerHandle r) {
            // no-op
        }

        @Override
        public void onAbort(Throwable t) {
            // no-op
        }
    };

    @Rule
    public TestName runtime = new TestName();

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

    private ZKTransaction newTxn() {
        return new ZKTransaction(zkc);
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
                FutureUtils.result(pool.tryObtain(newTxn(), NULL_LISTENER));
                fail("Should fail to allocate ledger if there are enought bookies");
            } catch (SimpleLedgerAllocator.AllocationException ae) {
                assertEquals(SimpleLedgerAllocator.Phase.ERROR, ae.getPhase());
            }
        }
        for (int i = 0; i < numAllocators; i++) {
            try {
                pool.allocate();
                FutureUtils.result(pool.tryObtain(newTxn(), NULL_LISTENER));
                fail("Should fail to allocate ledger if there aren't available allocators");
            } catch (SimpleLedgerAllocator.AllocationException ae) {
                assertEquals(SimpleLedgerAllocator.Phase.ERROR, ae.getPhase());
            } catch (IOException ioe) {
                // expected
            }
        }
        Utils.close(pool);
    }

    @Test(timeout = 60000)
    public void testRescueAllocators() throws Exception {
        String allocationPath = "/rescueAllocators";

        int numAllocators = 3;
        LedgerAllocatorPool pool =
                new LedgerAllocatorPool(allocationPath, numAllocators, dlConf, zkc, bkc, allocationExecutor);
        List<ZKTransaction> pendingTxns = Lists.newArrayListWithExpectedSize(numAllocators);
        List<String> allocatePaths = Lists.newArrayListWithExpectedSize(numAllocators);
        for (int i = 0; i < numAllocators; i++) {
            ZKTransaction txn = newTxn();
            pool.allocate();
            LedgerHandle lh = FutureUtils.result(pool.tryObtain(txn, NULL_LISTENER));

            // get the corresponding ledger allocator
            SimpleLedgerAllocator sla = pool.getLedgerAllocator(lh);
            String slaPath = sla.allocatePath;

            logger.info("Allocated ledger {} from path {}", lh.getId(), slaPath);

            pendingTxns.add(txn);
            allocatePaths.add(slaPath);
        }

        for (int i = 0; i < numAllocators; i++) {
            ZKTransaction txn = pendingTxns.get(i);
            String slaPath = allocatePaths.get(i);

            // execute the transaction to confirm/abort obtain
            FutureUtils.result(txn.execute());

            // introduce error to individual ledger allocator
            byte[] data = zkc.get().getData(slaPath, false, new Stat());
            zkc.get().setData(slaPath, data, -1);
        }
        int numSuccess = 0;
        Set<String> allocatedPathSet = new HashSet<String>();
        while (numSuccess < 2 * numAllocators) {
            try {
                pool.allocate();
                ZKTransaction txn = newTxn();
                LedgerHandle lh = FutureUtils.result(pool.tryObtain(txn, NULL_LISTENER));

                // get the corresponding ledger allocator
                SimpleLedgerAllocator sla = pool.getLedgerAllocator(lh);
                String slaPath = sla.allocatePath;

                logger.info("Allocated ledger {} from path {}", lh.getId(), slaPath);
                allocatedPathSet.add(slaPath);

                FutureUtils.result(txn.execute());
                ++numSuccess;
            } catch (IOException ioe) {
                // continue
            }
        }
        assertEquals(2 * numAllocators, numSuccess);
        assertEquals(numAllocators, allocatedPathSet.size());
        Utils.close(pool);
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
        Utils.close(pool);
    }

    @Test(timeout = 60000)
    public void testObtainWhenNoAllocator() throws Exception {
        String allocationPath = "/obtainWhenNoAllocator";
        LedgerAllocatorPool pool = new LedgerAllocatorPool(allocationPath, 0, dlConf, zkc, bkc, allocationExecutor);
        ZKTransaction txn = newTxn();
        try {
            FutureUtils.result(pool.tryObtain(txn, NULL_LISTENER));
            fail("Should fail obtain ledger handle if there is no allocator.");
        } catch (SimpleLedgerAllocator.AllocationException ae) {
            fail("Should fail obtain ledger handle if there is no allocator.");
        } catch (IOException ioe) {
            // expected.
        }

        Utils.close(pool);
    }

    @Test(timeout = 60000)
    public void testAllocateMultipleLedgers() throws Exception {
        String allocationPath = "/" + runtime.getMethodName();
        int numAllocators = 5;
        final LedgerAllocatorPool pool =
                new LedgerAllocatorPool(allocationPath, numAllocators, dlConf, zkc, bkc, allocationExecutor);
        int numLedgers = 20;
        Set<LedgerHandle> allocatedLedgers = new HashSet<LedgerHandle>();
        for (int i = 0; i < numLedgers; i++) {
            pool.allocate();
            ZKTransaction txn = newTxn();
            LedgerHandle lh = FutureUtils.result(pool.tryObtain(txn, NULL_LISTENER));
            FutureUtils.result(txn.execute());
            allocatedLedgers.add(lh);
        }
        assertEquals(numLedgers, allocatedLedgers.size());
    }

    @Test
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
            final int tid = i;
            allocationThreads[i] = new Thread() {

                int numLedgers = 50;

                @Override
                public void run() {
                    try {
                        for (int i = 0; i < numLedgers; i++) {
                            pool.allocate();
                            ZKTransaction txn = newTxn();
                            LedgerHandle lh = FutureUtils.result(pool.tryObtain(txn, NULL_LISTENER));
                            FutureUtils.result(txn.execute());
                            lh.close();
                            allocatedLedgers.putIfAbsent(lh.getId(), lh);
                            logger.info("[thread {}] allocate {}th ledger {}",
                                    new Object[] { tid, i, lh.getId() });
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

        Utils.close(pool);
    }

}
