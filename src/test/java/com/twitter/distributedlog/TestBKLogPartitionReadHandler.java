package com.twitter.distributedlog;

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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link BKLogPartitionReadHandler}
 */
public class TestBKLogPartitionReadHandler {

    static final Logger LOG = LoggerFactory.getLogger(TestBKLogPartitionReadHandler.class);

    protected static DistributedLogConfiguration conf = new DistributedLogConfiguration().setLockTimeout(10);
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    private static int numBookies = 3;

    @BeforeClass
    public static void setupBookkeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownBookkeeper() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
    }

    private void prepareLogSegments(String name, int numSegments, int numEntriesPerSegment) throws Exception {
        BKLogPartitionWriteHandler writer = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (int sid = 0; sid < numSegments; ++sid) {
            BKPerStreamLogWriter out = writer.startLogSegment(txid);
            for (int eid = 0; eid < numEntriesPerSegment; ++eid) {
                LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
                out.write(record);
                ++txid;
            }
            out.close();
            writer.completeAndCloseLogSegment(out.getLedgerHandle().getId(),
                    1 + sid * numEntriesPerSegment, (sid + 1) * numEntriesPerSegment, numEntriesPerSegment);
        }
        writer.close();
    }

    @Test(timeout = 60000)
    public void testGetLedgerList() throws Exception {
        String dlName = "TestGetLedgerList";
        prepareLogSegments(dlName, 3, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> ledgerList = readHandler.getLedgerList(false, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        List<LogSegmentLedgerMetadata> ledgerList2 = readHandler.getFilteredLedgerList(true, false);
        List<LogSegmentLedgerMetadata> ledgerList3 = readHandler.getLedgerList(false, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        assertEquals(3, ledgerList.size());
        assertEquals(3, ledgerList2.size());
        assertEquals(3, ledgerList3.size());
        for (int i=0; i<3; i++) {
            assertEquals(ledgerList3.get(i), ledgerList2.get(i));
        }
    }

    @Test(timeout = 60000)
    public void testForceGetLedgerList() throws Exception {
        String dlName = "TestForceGetLedgerList";
        prepareLogSegments(dlName, 3, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> ledgerList = readHandler.getLedgerList(true, false, LogSegmentLedgerMetadata.COMPARATOR, false);
        final AtomicReference<List<LogSegmentLedgerMetadata>> resultHolder =
                new AtomicReference<List<LogSegmentLedgerMetadata>>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        readHandler.asyncGetLedgerList(LogSegmentLedgerMetadata.COMPARATOR, null, new BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>>() {
            @Override
            public void operationComplete(int rc, List<LogSegmentLedgerMetadata> result) {
                resultHolder.set(result);
                latch.countDown();
            }
        });
        latch.await();
        List<LogSegmentLedgerMetadata> newLedgerList = resultHolder.get();
        assertNotNull(newLedgerList);
        LOG.info("Force sync get list : {}", ledgerList);
        LOG.info("Async get list : {}", newLedgerList);
        assertEquals(3, ledgerList.size());
        assertEquals(3, newLedgerList.size());
        for (int i=0; i<3; i++) {
            assertEquals(ledgerList.get(i), newLedgerList.get(i));
        }
    }

    @Test(timeout = 60000)
    public void testGetFilteredLedgerListInWriteHandler() throws Exception {
        String dlName = "TestGetFilteredLedgerListInWriteHandler";
        prepareLogSegments(dlName, 3, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);

        // Get full list.
        BKLogPartitionWriteHandler writeHandler0 = ((BKDistributedLogManager) dlm).createWriteLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> cachedFullLedgerList =
                writeHandler0.getCachedFullLedgerList(LogSegmentLedgerMetadata.DESC_COMPARATOR);
        assertTrue(cachedFullLedgerList.size() <= 1);
        List<LogSegmentLedgerMetadata> fullLedgerList = writeHandler0.getFullLedgerListDesc(false, false);
        assertEquals(3, fullLedgerList.size());

        // Get filtered list.
        BKLogPartitionWriteHandler writeHandler1 = ((BKDistributedLogManager) dlm).createWriteLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> filteredLedgerListDesc = writeHandler1.getFilteredLedgerListDesc(false, false);
        assertEquals(1, filteredLedgerListDesc.size());
        assertEquals(fullLedgerList.get(0), filteredLedgerListDesc.get(0));
        List<LogSegmentLedgerMetadata> filteredLedgerList = writeHandler1.getFilteredLedgerList(false, false);
        assertEquals(1, filteredLedgerList.size());
        assertEquals(fullLedgerList.get(0), filteredLedgerList.get(0));
    }
}
