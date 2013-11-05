package com.twitter.distributedlog;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test {@link BKLogPartitionReadHandler}
 */
public class TestBKLogPartitionReadHandler {

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
            PerStreamLogWriter out = writer.startLogSegment(txid);
            for (int eid = 0; eid < numEntriesPerSegment; ++eid) {
                LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
                out.write(record);
                ++txid;
            }
            out.close();
            writer.completeAndCloseLogSegment(1 + sid * numEntriesPerSegment, (sid + 1) * numEntriesPerSegment);
        }
        writer.close();
    }

    @Test(timeout = 6000)
    public void testGetLedgerList() throws Exception {
        String dlName = "GetLedgerList";
        prepareLogSegments(dlName, 3, 3);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, dlName);
        BKLogPartitionReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadLedgerHandler(new PartitionId(0));
        List<LogSegmentLedgerMetadata> ledgerList = readHandler.getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, false);
        final AtomicReference<List<LogSegmentLedgerMetadata>> resultHolder =
                new AtomicReference<List<LogSegmentLedgerMetadata>>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        readHandler.getLedgerList(LogSegmentLedgerMetadata.COMPARATOR, null, new BookkeeperInternalCallbacks.GenericCallback<List<LogSegmentLedgerMetadata>>() {
            @Override
            public void operationComplete(int rc, List<LogSegmentLedgerMetadata> result) {
                resultHolder.set(result);
                latch.countDown();
            }
        });
        latch.await();
        List<LogSegmentLedgerMetadata> newLedgerList = resultHolder.get();
        assertNotNull(newLedgerList);
        assertEquals(3, ledgerList.size());
        assertEquals(3, newLedgerList.size());
        for (int i=0; i<3; i++) {
            assertEquals(ledgerList.get(i), newLedgerList.get(i));
        }
    }
}