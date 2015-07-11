package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator.AllocationException;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator.ConcurrentObtainException;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator.Phase;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.zk.DataWithStat;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Enumeration;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestLedgerAllocator extends TestDistributedLogBase {

    private static final String ledgersPath = "/ledgers";

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

    @Test(timeout = 60000)
    public void testAllocation() throws Exception {
        String allocationPath = "/allocation1";
        SimpleLedgerAllocator allocator = new SimpleLedgerAllocator(allocationPath, dlConf, zkc, bkc);
        allocator.allocate();
        Transaction txn = zkc.get().transaction();
        LedgerHandle lh = allocator.tryObtain(txn);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
        txn.setData("/unexistedpath", "data".getBytes(UTF_8), -1);
        try {
            txn.commit();
        } catch (KeeperException ke) {
            // abort the allocation
            allocator.abortObtain(lh);
        }
        data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));

        // Create new transaction to obtain the ledger again.
        txn = zkc.get().transaction();
        // we could obtain the ledger if it was obtained
        LedgerHandle newLh = allocator.tryObtain(txn);
        assertEquals(lh.getId(), newLh.getId());
        List<OpResult> results = txn.commit();
        // confirm the obtain
        allocator.confirmObtain(newLh, results.get(0));
        data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
        allocator.close(true);
    }

    @Test(timeout = 60000)
    public void testBadVersionOnTwoAllocators() throws Exception {
        String allocationPath = "/allocation-bad-version";
        zkc.get().create(allocationPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        DataWithStat dataWithStat = new DataWithStat();
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(allocationPath, false, stat);
        dataWithStat.setDataWithStat(data, stat);

        SimpleLedgerAllocator allocator1 = new SimpleLedgerAllocator(allocationPath, dataWithStat, dlConf, zkc, bkc);
        SimpleLedgerAllocator allocator2 = new SimpleLedgerAllocator(allocationPath, dataWithStat, dlConf, zkc, bkc);
        allocator1.allocate();
        // wait until allocated
        Transaction txn1 = zkc.get().transaction();
        LedgerHandle lh = allocator1.tryObtain(txn1);
        allocator2.allocate();
        Transaction txn2 = zkc.get().transaction();
        try {
            allocator2.tryObtain(txn2);
            fail("Should fail allocating on second allocator as allocator1 is starting allocating something.");
        } catch (ConcurrentObtainException ioe2) {
            // as expected
            assertEquals(Phase.HANDING_OVER, ioe2.getPhase());
        } catch (AllocationException ae) {
            assertEquals(Phase.ERROR, ae.getPhase());
        }
        List<OpResult> results = null;
        try {
            results = txn1.commit();
        } catch (KeeperException ke) {
            allocator1.abortObtain(lh);
        }
        assertNotNull(results);
        allocator1.confirmObtain(lh, results.get(0));
        allocator1.close(true);
        allocator2.close(true);

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

        SimpleLedgerAllocator allocator1 =
                new SimpleLedgerAllocator(allocationPath, confLocal, zkc, bkc);
        allocator1.allocate();
        Transaction txn1 = zkc.get().transaction();

        try {
            allocator1.tryObtain(txn1);
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
        DataWithStat dataWithStat = new DataWithStat();
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(allocationPath, false, stat);
        dataWithStat.setDataWithStat(data, stat);

        SimpleLedgerAllocator allocator1 = new SimpleLedgerAllocator(allocationPath, dataWithStat, dlConf, zkc, bkc);
        allocator1.allocate();
        // wait until allocated
        Transaction txn1 = zkc.get().transaction();
        LedgerHandle lh1 = allocator1.tryObtain(txn1);

        // Second allocator kicks in
        stat = new Stat();
        data = zkc.get().getData(allocationPath, false, stat);
        dataWithStat.setDataWithStat(data, stat);
        SimpleLedgerAllocator allocator2 = new SimpleLedgerAllocator(allocationPath, dataWithStat, dlConf, zkc, bkc);
        allocator2.allocate();
        // wait until allocated
        Transaction txn2 = zkc.get().transaction();
        LedgerHandle lh2 = allocator2.tryObtain(txn2);

        // should fail to commit txn1 as version is changed by second allocator
        try {
            txn1.commit();
            fail("Should fail commit obtaining ledger handle from first allocator as allocator is modified by second allocator.");
        } catch (KeeperException ke) {
            // as expected
            allocator1.abortObtain(lh1);
        }
        List<OpResult> results = null;
        try {
            results = txn2.commit();
        } catch (KeeperException ke) {
            allocator2.abortObtain(lh2);
        }
        assertNotNull(results);
        allocator2.confirmObtain(lh2, results.get(0));
        allocator1.close(true);
        allocator2.close(true);

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
        SimpleLedgerAllocator allocator = new SimpleLedgerAllocator(allocationPath, dlConf, zkc, bkc);
        allocator.allocate();
        Transaction txn = zkc.get().transaction();
        // close during obtaining ledger.
        LedgerHandle lh = allocator.tryObtain(txn);
        allocator.close(true);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
        // the ledger is not deleted
        bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                dlConf.getBKDigestPW().getBytes(UTF_8));
    }

    @Test(timeout = 60000)
    public void testCloseAllocatorAfterConfirm() throws Exception {
        String allocationPath = "/allocation2";
        SimpleLedgerAllocator allocator = new SimpleLedgerAllocator(allocationPath, dlConf, zkc, bkc);
        allocator.allocate();
        Transaction txn = zkc.get().transaction();
        // close during obtaining ledger.
        LedgerHandle lh = allocator.tryObtain(txn);
        List<OpResult> results = txn.commit();
        allocator.confirmObtain(lh, results.get(0));
        allocator.close(true);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
        // the ledger is not deleted.
        bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                dlConf.getBKDigestPW().getBytes(UTF_8));
    }

    @Test(timeout = 60000)
    public void testCloseAllocatorAfterAbort() throws Exception {
        String allocationPath = "/allocation3";
        SimpleLedgerAllocator allocator = new SimpleLedgerAllocator(allocationPath, dlConf, zkc, bkc);
        allocator.allocate();
        Transaction txn = zkc.get().transaction();
        // close during obtaining ledger.
        LedgerHandle lh = allocator.tryObtain(txn);
        allocator.abortObtain(lh);
        allocator.close(true);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
        // the ledger is not deleted.
        try {
            bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                    dlConf.getBKDigestPW().getBytes(UTF_8));
            fail("Ledger " + lh.getId() + " should be deleted after closed.");
        } catch (BKException.BKNoSuchLedgerExistsException nslee) {
            // expected;
        }
    }
}
