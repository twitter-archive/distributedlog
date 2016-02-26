package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator.AllocationException;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator.Phase;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Transaction.OpListener;
import com.twitter.distributedlog.util.Utils;
import com.twitter.distributedlog.zk.DefaultZKOp;
import com.twitter.distributedlog.zk.ZKTransaction;
import com.twitter.util.Future;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestLedgerAllocator extends TestDistributedLogBase {

    private static final Logger logger = LoggerFactory.getLogger(TestLedgerAllocator.class);

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

    private URI createURI(String path) {
        return URI.create("distributedlog://" + zkServers + path);
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder().uri(createURI("/"))
                .sessionTimeoutMs(10000).zkAclId(null).zkServers(zkServers).build();
        bkc = BookKeeperClientBuilder.newBuilder().name("bkc")
                .dlConfig(dlConf).ledgersPath(ledgersPath).zkc(zkc).build();
    }

    @After
    public void teardown() throws Exception {
        bkc.close();
        zkc.close();
    }

    private ZKTransaction newTxn() {
        return new ZKTransaction(zkc);
    }

    private SimpleLedgerAllocator createAllocator(String allocationPath) throws IOException {
        return createAllocator(allocationPath, dlConf);
    }

    private SimpleLedgerAllocator createAllocator(String allocationPath,
                                                  DistributedLogConfiguration conf) throws IOException {
        return FutureUtils.result(SimpleLedgerAllocator.of(allocationPath, null, conf, zkc, bkc));
    }

    @Test(timeout = 60000)
    public void testAllocation() throws Exception {
        String allocationPath = "/allocation1";
        SimpleLedgerAllocator allocator = createAllocator(allocationPath);
        allocator.allocate();
        ZKTransaction txn = newTxn();
        LedgerHandle lh = FutureUtils.result(allocator.tryObtain(txn, NULL_LISTENER));
        logger.info("Try obtaining ledger handle {}", lh.getId());
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
        txn.addOp(DefaultZKOp.of(Op.setData("/unexistedpath", "data".getBytes(UTF_8), -1)));
        try {
            FutureUtils.result(txn.execute());
            fail("Should fail the transaction when setting unexisted path");
        } catch (ZKException ke) {
            // expected
            logger.info("Should fail on executing transaction when setting unexisted path", ke);
        }
        data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));

        // Create new transaction to obtain the ledger again.
        txn = newTxn();
        // we could obtain the ledger if it was obtained
        LedgerHandle newLh = FutureUtils.result(allocator.tryObtain(txn, NULL_LISTENER));
        assertEquals(lh.getId(), newLh.getId());
        FutureUtils.result(txn.execute());
        data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
        Utils.close(allocator);
    }

    @Test(timeout = 60000)
    public void testBadVersionOnTwoAllocators() throws Exception {
        String allocationPath = "/allocation-bad-version";
        zkc.get().create(allocationPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(allocationPath, false, stat);
        Versioned<byte[]> allocationData = new Versioned<byte[]>(data, new ZkVersion(stat.getVersion()));

        SimpleLedgerAllocator allocator1 = new SimpleLedgerAllocator(allocationPath, allocationData, dlConf, zkc, bkc);
        SimpleLedgerAllocator allocator2 = new SimpleLedgerAllocator(allocationPath, allocationData, dlConf, zkc, bkc);
        allocator1.allocate();
        // wait until allocated
        ZKTransaction txn1 = newTxn();
        LedgerHandle lh = FutureUtils.result(allocator1.tryObtain(txn1, NULL_LISTENER));
        allocator2.allocate();
        ZKTransaction txn2 = newTxn();
        try {
            FutureUtils.result(allocator2.tryObtain(txn2, NULL_LISTENER));
            fail("Should fail allocating on second allocator as allocator1 is starting allocating something.");
        } catch (ZKException zke) {
            assertEquals(KeeperException.Code.BADVERSION, zke.getKeeperExceptionCode());
        }
        FutureUtils.result(txn1.execute());
        Utils.close(allocator1);
        Utils.close(allocator2);

        long eid = lh.addEntry("hello world".getBytes());
        lh.close();
        LedgerHandle readLh = bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes());
        Enumeration<LedgerEntry> entries = readLh.readEntries(eid, eid);
        int i = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("hello world", new String(entry.getEntry(), UTF_8));
            ++i;
        }
        assertEquals(1, i);
    }

    @Test(timeout = 60000)
    public void testAllocatorWithoutEnoughBookies() throws Exception {
        String allocationPath = "/allocator-without-enough-bookies";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setEnsembleSize(numBookies * 2);
        confLocal.setWriteQuorumSize(numBookies * 2);

        SimpleLedgerAllocator allocator1 = createAllocator(allocationPath, confLocal);
        allocator1.allocate();
        ZKTransaction txn1 = newTxn();

        try {
            FutureUtils.result(allocator1.tryObtain(txn1, NULL_LISTENER));
            fail("Should fail allocating ledger if there aren't enough bookies");
        } catch (AllocationException ioe) {
            // expected
            assertEquals(Phase.ERROR, ioe.getPhase());
        }
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
    }

    @Test(timeout = 60000)
    public void testSuccessAllocatorShouldDeleteUnusedledger() throws Exception {
        String allocationPath = "/allocation-delete-unused-ledger";
        zkc.get().create(allocationPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(allocationPath, false, stat);

        Versioned<byte[]> allocationData = new Versioned<byte[]>(data, new ZkVersion(stat.getVersion()));

        SimpleLedgerAllocator allocator1 = new SimpleLedgerAllocator(allocationPath, allocationData, dlConf, zkc, bkc);
        allocator1.allocate();
        // wait until allocated
        ZKTransaction txn1 = newTxn();
        LedgerHandle lh1 = FutureUtils.result(allocator1.tryObtain(txn1, NULL_LISTENER));

        // Second allocator kicks in
        stat = new Stat();
        data = zkc.get().getData(allocationPath, false, stat);
        allocationData = new Versioned<byte[]>(data, new ZkVersion(stat.getVersion()));
        SimpleLedgerAllocator allocator2 = new SimpleLedgerAllocator(allocationPath, allocationData, dlConf, zkc, bkc);
        allocator2.allocate();
        // wait until allocated
        ZKTransaction txn2 = newTxn();
        LedgerHandle lh2 = FutureUtils.result(allocator2.tryObtain(txn2, NULL_LISTENER));

        // should fail to commit txn1 as version is changed by second allocator
        try {
            FutureUtils.result(txn1.execute());
            fail("Should fail commit obtaining ledger handle from first allocator as allocator is modified by second allocator.");
        } catch (ZKException ke) {
            // as expected
        }
        FutureUtils.result(txn2.execute());
        Utils.close(allocator1);
        Utils.close(allocator2);

        // ledger handle should be deleted
        try {
            lh1.close();
            fail("LedgerHandle allocated by allocator1 should be deleted.");
        } catch (BKException bke) {
            // as expected
        }
        try {
            bkc.get().openLedger(lh1.getId(), BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes());
            fail("LedgerHandle allocated by allocator1 should be deleted.");
        } catch (BKException.BKNoSuchLedgerExistsException nslee) {
            // as expected
        }
        long eid = lh2.addEntry("hello world".getBytes());
        lh2.close();
        LedgerHandle readLh = bkc.get().openLedger(lh2.getId(), BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes());
        Enumeration<LedgerEntry> entries = readLh.readEntries(eid, eid);
        int i = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("hello world", new String(entry.getEntry(), UTF_8));
            ++i;
        }
        assertEquals(1, i);
    }

    @Test(timeout = 60000)
    public void testCloseAllocatorDuringObtaining() throws Exception {
        String allocationPath = "/allocation2";
        SimpleLedgerAllocator allocator = createAllocator(allocationPath);
        allocator.allocate();
        ZKTransaction txn = newTxn();
        // close during obtaining ledger.
        LedgerHandle lh = FutureUtils.result(allocator.tryObtain(txn, NULL_LISTENER));
        Utils.close(allocator);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
        // the ledger is not deleted
        bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                dlConf.getBKDigestPW().getBytes(UTF_8));
    }

    @Test(timeout = 60000)
    public void testCloseAllocatorAfterConfirm() throws Exception {
        String allocationPath = "/allocation2";
        SimpleLedgerAllocator allocator = createAllocator(allocationPath);
        allocator.allocate();
        ZKTransaction txn = newTxn();
        // close during obtaining ledger.
        LedgerHandle lh = FutureUtils.result(allocator.tryObtain(txn, NULL_LISTENER));
        FutureUtils.result(txn.execute());
        Utils.close(allocator);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
        // the ledger is not deleted.
        bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                dlConf.getBKDigestPW().getBytes(UTF_8));
    }

    @Test(timeout = 60000)
    public void testCloseAllocatorAfterAbort() throws Exception {
        String allocationPath = "/allocation3";
        SimpleLedgerAllocator allocator = createAllocator(allocationPath);
        allocator.allocate();
        ZKTransaction txn = newTxn();
        // close during obtaining ledger.
        LedgerHandle lh = FutureUtils.result(allocator.tryObtain(txn, NULL_LISTENER));
        txn.addOp(DefaultZKOp.of(Op.setData("/unexistedpath", "data".getBytes(UTF_8), -1)));
        try {
            FutureUtils.result(txn.execute());
            fail("Should fail the transaction when setting unexisted path");
        } catch (ZKException ke) {
            // expected
        }
        Utils.close(allocator);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
        // the ledger is not deleted.
        bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                dlConf.getBKDigestPW().getBytes(UTF_8));
    }

    @Test(timeout = 60000)
    public void testConcurrentAllocation() throws Exception {
        String allcationPath = "/" + runtime.getMethodName();
        SimpleLedgerAllocator allocator = createAllocator(allcationPath);
        allocator.allocate();
        ZKTransaction txn1 = newTxn();
        Future<LedgerHandle> obtainFuture1 = allocator.tryObtain(txn1, NULL_LISTENER);
        ZKTransaction txn2 = newTxn();
        Future<LedgerHandle> obtainFuture2 = allocator.tryObtain(txn2, NULL_LISTENER);
        assertTrue(obtainFuture2.isDefined());
        assertTrue(obtainFuture2.isThrow());
        try {
            FutureUtils.result(obtainFuture2);
            fail("Should fail the concurrent obtain since there is already a transaction obtaining the ledger handle");
        } catch (SimpleLedgerAllocator.ConcurrentObtainException cbe) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testObtainMultipleLedgers() throws Exception {
        String allocationPath = "/" + runtime.getMethodName();
        SimpleLedgerAllocator allocator = createAllocator(allocationPath);
        int numLedgers = 10;
        Set<LedgerHandle> allocatedLedgers = new HashSet<LedgerHandle>();
        for (int i = 0; i < numLedgers; i++) {
            allocator.allocate();
            ZKTransaction txn = newTxn();
            LedgerHandle lh = FutureUtils.result(allocator.tryObtain(txn, NULL_LISTENER));
            FutureUtils.result(txn.execute());
            allocatedLedgers.add(lh);
        }
        assertEquals(numLedgers, allocatedLedgers.size());
    }
}
