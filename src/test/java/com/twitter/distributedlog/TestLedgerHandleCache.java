package com.twitter.distributedlog;

import com.twitter.distributedlog.metadata.BKDLConfig;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test {@link LedgerHandleCache}
 */
public class TestLedgerHandleCache {
    static final Logger LOG = LoggerFactory.getLogger(TestLedgerHandleCache.class);

    protected static DistributedLogConfiguration dlConf = new DistributedLogConfiguration().setLockTimeout(10);
    protected static BKDLConfig bkdlConfig = new BKDLConfig("127.0.0.1:7000", "/ledgers");
    private static ZooKeeperServerShim zks;
    private static LocalDLMEmulator bkutil;
    static int numBookies = 3;

    private ZooKeeperClient zkc;
    private BookKeeperClient bkc;

    @BeforeClass
    public static void setupBookkeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownBookKeeper() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .zkServers("127.0.0.1:7000").sessionTimeoutMs(10000).build();
        bkc = BookKeeperClientBuilder.newBuilder().name("bkc")
                .zkc(zkc).bkdlConfig(bkdlConfig).dlConfig(dlConf).build();
    }

    @After
    public void teardown() throws Exception {
        bkc.release();
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testOpenLedgerWhenBkcClosed() throws Exception {
        BookKeeperClient newBkc = BookKeeperClientBuilder.newBuilder().name("newBkc")
                .zkc(zkc).bkdlConfig(bkdlConfig).dlConfig(dlConf).build();
        LedgerHandleCache cache = new LedgerHandleCache(newBkc, "bkcClosed");
        // closed the bkc
        newBkc.release();
        // open ledger after bkc closed.
        try {
            cache.openLedger(new LogSegmentLedgerMetadata("", 2, 1, 1, 1), false);
            fail("Should throw IOException if bookkeeper client is closed.");
        } catch (BKException.BKBookieHandleNotAvailableException ie) {
            // expected
        }
        final AtomicInteger rcHolder = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        cache.asyncOpenLedger(new LogSegmentLedgerMetadata("", 2, 1, 1, 1), false, new BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>() {
            @Override
            public void operationComplete(int rc, LedgerDescriptor result) {
                rcHolder.set(rc);
                latch.countDown();
            }
        });
        latch.await();
        assertEquals(BKException.Code.BookieHandleNotAvailableException, rcHolder.get());
    }

    @Test(timeout = 60000)
    public void testOpenLedgerWhenZkClosed() throws Exception {
        ZooKeeperClient newZkc = ZooKeeperClientBuilder.newBuilder()
                .zkServers("127.0.0.1:7000").sessionTimeoutMs(10000).build();
        BookKeeperClient newBkc = BookKeeperClientBuilder.newBuilder().name("newBkc")
                .zkc(newZkc).bkdlConfig(bkdlConfig).dlConfig(dlConf).build();
        try {
            LedgerHandle lh = newBkc.get().createLedger(BookKeeper.DigestType.CRC32, "zkClosed".getBytes(UTF_8));
            lh.close();
            newZkc.close();
            LedgerHandleCache cache = new LedgerHandleCache(newBkc, "zkcClosed");
            // open ledger after zkc closed
            try {
                cache.openLedger(new LogSegmentLedgerMetadata("", 2, lh.getId(), 1, lh.getId()), false);
                fail("Should throw BKException.ZKException if zookeeper client is closed.");
            } catch (BKException.ZKException ze) {
                // expected
            }
            final AtomicInteger rcHolder = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(1);
            cache.asyncOpenLedger(new LogSegmentLedgerMetadata("", 2, lh.getId(), 1, lh.getId()), false, new BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>() {
                @Override
                public void operationComplete(int rc, LedgerDescriptor result) {
                    rcHolder.set(rc);
                    latch.countDown();
                }
            });
            latch.await();
            assertEquals(BKException.Code.ZKException, rcHolder.get());
        } finally {
            newBkc.release();
        }
    }

    @Test(timeout = 60000)
    public void testOperationsOnUnexistedLedger() throws Exception {
        LedgerDescriptor desc = new LedgerDescriptor(9999, 9999, false);
        LedgerHandleCache cache = new LedgerHandleCache(bkc, "unexistedLedgers");
        // read last confirmed
        try {
            cache.readLastConfirmed(desc);
            fail("Should throw IOException if ledger doesn't exist");
        } catch (BKException.BKNoSuchLedgerExistsException ioe) {
            // expected
        }
        final AtomicInteger rcHolder = new AtomicInteger(0);
        final CountDownLatch readLastConfirmedLatch = new CountDownLatch(1);
        cache.asyncReadLastConfirmed(desc, new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                rcHolder.set(rc);
                readLastConfirmedLatch.countDown();
            }
        }, null);
        readLastConfirmedLatch.await();
        assertEquals(BKException.Code.NoSuchLedgerExistsException, rcHolder.get());
        // read entries
        try {
            cache.readEntries(desc, 0, 10);
            fail("Should throw IOException if ledger doesn't exist");
        } catch (BKException.BKNoSuchLedgerExistsException ioe) {
            // expected.
        }
        final CountDownLatch readEntriesLatch = new CountDownLatch(1);
        cache.asyncReadEntries(desc, 0, 10, new AsyncCallback.ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                rcHolder.set(rc);
                readEntriesLatch.countDown();
            }
        }, null);
        readEntriesLatch.await();
        assertEquals(BKException.Code.NoSuchLedgerExistsException, rcHolder.get());
    }

}
