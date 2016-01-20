package com.twitter.distributedlog;

import com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataBuilder;
import com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion;
import com.twitter.distributedlog.LogSegmentMetadata.TruncationStatus;
import com.twitter.distributedlog.exceptions.UnsupportedMetadataVersionException;

import com.twitter.distributedlog.util.FutureUtils;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test {@link LogSegmentMetadata}
 */
public class TestLogSegmentMetadata extends ZooKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestLogSegmentMetadata.class);

    static final int TEST_REGION_ID = 0xf - 1;

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder().zkAclId(null).zkServers(zkServers).sessionTimeoutMs(10000).build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testReadMetadata() throws Exception {
        LogSegmentMetadata metadata1 = new LogSegmentMetadataBuilder("/metadata1",
            LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION, 1000, 1).setRegionId(TEST_REGION_ID).build();
        metadata1.write(zkc);
        LogSegmentMetadata read1 = FutureUtils.result(LogSegmentMetadata.read(zkc, "/metadata1"));
        assertEquals(metadata1, read1);
        assertEquals(TEST_REGION_ID, read1.getRegionId());
    }

    @Test(timeout = 60000)
    public void testReadMetadataCrossVersion() throws Exception {
        LogSegmentMetadata metadata1 = new LogSegmentMetadataBuilder("/metadata2",
            1, 1000, 1).setRegionId(TEST_REGION_ID).build();
        metadata1.write(zkc);
        // synchronous read
        LogSegmentMetadata read1 = FutureUtils.result(LogSegmentMetadata.read(zkc, "/metadata2", true));
        assertEquals(read1.getLedgerId(), metadata1.getLedgerId());
        assertEquals(read1.getFirstTxId(), metadata1.getFirstTxId());
        assertEquals(read1.getLastTxId(), metadata1.getLastTxId());
        assertEquals(read1.getLogSegmentSequenceNumber(), metadata1.getLogSegmentSequenceNumber());
        assertEquals(DistributedLogConstants.LOCAL_REGION_ID, read1.getRegionId());
    }

    @Test(timeout = 60000)
    public void testReadMetadataCrossVersionFailure() throws Exception {
        LogSegmentMetadata metadata1 = new LogSegmentMetadataBuilder("/metadata-failure",
            1, 1000, 1).setRegionId(TEST_REGION_ID).build();
        metadata1.write(zkc);
        // synchronous read
        try {
            LogSegmentMetadata read1 = FutureUtils.result(LogSegmentMetadata.read(zkc, "/metadata-failure"));
            fail("The previous statement should throw an exception");
        } catch (UnsupportedMetadataVersionException e) {
            // Expected
        }
    }


    @Test(timeout = 60000)
    public void testMutateTruncationStatus() {
        LogSegmentMetadata metadata =
                new LogSegmentMetadataBuilder(
                        "/metadata", LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES, 1L, 0L)
                        .setRegionId(0).setLogSegmentSequenceNo(1L).build();
        metadata = metadata.completeLogSegment("/completed-metadata", 1000L, 1000, 1000L, 0L, 0L);

        LogSegmentMetadata partiallyTruncatedSegment =
                metadata.mutator()
                        .setTruncationStatus(TruncationStatus.PARTIALLY_TRUNCATED)
                        .setMinActiveDLSN(new DLSN(1L, 500L, 0L))
                        .build();

        LogSegmentMetadata fullyTruncatedSegment =
                partiallyTruncatedSegment.mutator()
                        .setTruncationStatus(TruncationStatus.TRUNCATED)
                        .build();

        assertEquals(new DLSN(1L, 500L, 0L), fullyTruncatedSegment.getMinActiveDLSN());
    }

    @Test(timeout = 60000)
    public void testParseInvalidMetadata() throws Exception {
        try {
            LogSegmentMetadata.parseData("/metadata1", new byte[0], false);
            fail("Should fail to parse invalid metadata");
        } catch (IOException ioe) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testReadLogSegmentWithSequenceId() throws Exception {
        LogSegmentMetadata metadata =
                new LogSegmentMetadataBuilder(
                        "/metadata", LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID, 1L, 0L)
                        .setRegionId(0)
                        .setLogSegmentSequenceNo(1L)
                        .setStartSequenceId(999L)
                        .build();
        // write inprogress log segment with v5
        String data = metadata.getFinalisedData();
        LogSegmentMetadata parsedMetadata = LogSegmentMetadata.parseData("/metadatav5", data.getBytes(UTF_8), false);
        assertEquals(999L, parsedMetadata.getStartSequenceId());

        LogSegmentMetadata metadatav4 =
                new LogSegmentMetadataBuilder(
                        "/metadata", LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES, 1L, 0L)
                        .setRegionId(0)
                        .setLogSegmentSequenceNo(1L)
                        .setStartSequenceId(999L)
                        .build();
        String datav4 = metadatav4.getFinalisedData();
        LogSegmentMetadata parsedMetadatav4 =
                LogSegmentMetadata.parseData("/metadatav4", datav4.getBytes(UTF_8), false);
        assertTrue(parsedMetadatav4.getStartSequenceId() < 0);
    }
}
