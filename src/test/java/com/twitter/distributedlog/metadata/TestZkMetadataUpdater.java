package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestZkMetadataUpdater {

    static final Logger LOG = LoggerFactory.getLogger(TestZkMetadataUpdater.class);

    private static ZooKeeperServerShim zks;

    private ZooKeeperClient zkc;

    @BeforeClass
    public static void setupZooKeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
    }

    @AfterClass
    public static void shutdownZooKeeper() throws Exception {
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .uri(createURI("/"))
                .sessionTimeoutMs(10000).build();
    }

    @After
    public void tearDown() throws Exception {
        zkc.close();
    }

    private URI createURI(String path) {
        return URI.create("distributedlog://127.0.0.1:7000" + path);
    }

    Map<Long, LogSegmentLedgerMetadata> readLogSegments(String ledgerPath) throws Exception {
        List<String> children = zkc.get().getChildren(ledgerPath, false);
        LOG.info("Children under {} : {}", ledgerPath, children);
        Map<Long, LogSegmentLedgerMetadata> segments =
                new HashMap<Long, LogSegmentLedgerMetadata>(children.size());
        for (String child : children) {
            LogSegmentLedgerMetadata segment = LogSegmentLedgerMetadata.read(zkc, ledgerPath + "/" + child,
                    DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
            LOG.info("Read segment {} : {}", child, segment);
            segments.put(segment.getLedgerSequenceNumber(), segment);
        }
        return segments;
    }

    @Test
    public void testChangeSequenceNumber() throws Exception {
        String ledgerPath = "/ledgers";
        zkc.get().create(ledgerPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Map<Long, LogSegmentLedgerMetadata> completedLogSegments = new HashMap<Long, LogSegmentLedgerMetadata>();
        // Create 5 completed log segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentLedgerMetadata segment = DLMTestUtil.completedLogSegment(ledgerPath, i, (i - 1) * 100, i * 100 - 1, 100, i, 100, 0);
            completedLogSegments.put(((long)i), segment);
            LOG.info("Create completed segment {} : {}", segment.getZkPath(), segment);
            segment.write(zkc, segment.getZkPath());
        }
        // Create a smaller inprogress log segment
        long inprogressSeqNo = 3;
        LogSegmentLedgerMetadata segment = DLMTestUtil.inprogressLogSegment(ledgerPath, inprogressSeqNo, 5 * 100, inprogressSeqNo);
        LOG.info("Create inprogress segment {} : {}", segment.getZkPath(), segment);
        segment.write(zkc, segment.getZkPath());

        Map<Long, LogSegmentLedgerMetadata> segmentList = readLogSegments(ledgerPath);
        assertEquals(5, segmentList.size());

        // Dryrun
        MetadataUpdater dryrunUpdater = new DryrunZkMetadataUpdater(zkc);
        dryrunUpdater.changeSequenceNumber(segment, 6L);

        segmentList = readLogSegments(ledgerPath);
        assertEquals(5, segmentList.size());

        // Fix the inprogress log segments

        MetadataUpdater updater = ZkMetadataUpdater.createMetadataUpdater(zkc);
        updater.changeSequenceNumber(segment, 6L);

        segmentList = readLogSegments(ledgerPath);
        assertEquals(6, segmentList.size());

        // check first 5 log segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentLedgerMetadata s = segmentList.get((long)i);
            assertNotNull(s);
            assertEquals(completedLogSegments.get((long)i), s);
        }

        // get log segment 6
        LogSegmentLedgerMetadata segmentChanged = segmentList.get(6L);
        assertNotNull(segmentChanged);
        assertEquals(6L, segmentChanged.getLedgerSequenceNumber());
        assertTrue(segmentChanged.isInProgress());
        assertEquals(5 * 100, segmentChanged.getFirstTxId());
        assertEquals(3L, segmentChanged.getLedgerId());
    }
}
