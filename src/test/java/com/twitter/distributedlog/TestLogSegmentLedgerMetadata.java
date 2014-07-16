package com.twitter.distributedlog;

import org.apache.bookkeeper.client.BKException;
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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test {@link LogSegmentLedgerMetadata}
 */
public class TestLogSegmentLedgerMetadata {

    static final Logger LOG = LoggerFactory.getLogger(TestLogSegmentLedgerMetadata.class);

    static final int TEST_REGION_ID = 0xf - 1;

    private static ZooKeeperServerShim zks;
    private ZooKeeperClient zkc;

    @BeforeClass
    public static void setupZooKeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
    }

    @AfterClass
    public static void teardownZooKeeper() throws Exception {
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder().zkServers("127.0.0.1:7000").sessionTimeoutMs(10000).build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testReadMetadata() throws Exception {
        LogSegmentLedgerMetadata metadata1 = new LogSegmentLedgerMetadata("/metadata1",
                DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION, 1000, 1,
                TEST_REGION_ID, DistributedLogConstants.LOGSEGMENT_DEFAULT_STATUS);
        metadata1.write(zkc, "/metadata1");
        // synchronous read
        LogSegmentLedgerMetadata read1 = LogSegmentLedgerMetadata.read(zkc,
            "/metadata1",
            DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
        assertEquals(metadata1, read1);
        assertEquals(TEST_REGION_ID, read1.getRegionId());
        final AtomicReference<LogSegmentLedgerMetadata> resultHolder =
                new AtomicReference<LogSegmentLedgerMetadata>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        // asynchronous read
        LogSegmentLedgerMetadata.read(zkc,
            "/metadata1",
            DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION,
            new BookkeeperInternalCallbacks.GenericCallback<LogSegmentLedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LogSegmentLedgerMetadata result) {
                if (BKException.Code.OK != rc) {
                    LOG.error("Fail to read LogSegmentLedgerMetadata : ", BKException.create(rc));
                }
                resultHolder.set(result);
                latch.countDown();
            }
        });
        latch.await();
        assertEquals(metadata1, resultHolder.get());
        assertEquals(TEST_REGION_ID, resultHolder.get().getRegionId());
    }

    @Test(timeout = 60000)
    public void testReadMetadataCrossVersion() throws Exception {
        LogSegmentLedgerMetadata metadata1 = new LogSegmentLedgerMetadata(
            "/metadata2", 1, 1000, 1, TEST_REGION_ID, DistributedLogConstants.LOGSEGMENT_DEFAULT_STATUS);
        metadata1.write(zkc, "/metadata2");
        // synchronous read
        LogSegmentLedgerMetadata read1 = LogSegmentLedgerMetadata.read(zkc,
            "/metadata2",
            DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
        assertEquals(read1.getLedgerId(), metadata1.getLedgerId());
        assertEquals(read1.getFirstTxId(), metadata1.getFirstTxId());
        assertEquals(read1.getLastTxId(), metadata1.getLastTxId());
        assertEquals(read1.getLedgerSequenceNumber(), metadata1.getLedgerSequenceNumber());
        assertEquals(DistributedLogConstants.LOCAL_REGION_ID, read1.getRegionId());
    }

    @Test(timeout = 60000)
    public void testParseInvalidMetadata() throws Exception {
        try {
            LogSegmentLedgerMetadata.parseData("/metadata1", new byte[0], DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
            fail("Should fail to parse invalid metadata");
        } catch (IOException ioe) {
            // expected
        }
    }
}
