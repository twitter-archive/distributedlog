package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.ZooKeeperClusterTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestZkMetadataUpdater extends ZooKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestZkMetadataUpdater.class);

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .uri(createURI("/"))
                .zkAclId(null)
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
        return DLMTestUtil.readLogSegments(zkc, ledgerPath);
    }

    @Test
    public void testChangeSequenceNumber() throws Exception {
        String ledgerPath = "/testChangeSequenceNumber";
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

    @Test
    public void testUpdateLastDLSN() throws Exception {
        String ledgerPath = "/testUpdateLastDLSN";
        zkc.get().create(ledgerPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // Create 1 completed log segment
        LogSegmentLedgerMetadata completedLogSegment = DLMTestUtil.completedLogSegment(ledgerPath, 1L, 0L, 99L, 100, 1L, 99L, 0L);
        completedLogSegment.write(zkc, completedLogSegment.getZkPath());
        // Create 1 inprogress log segment
        LogSegmentLedgerMetadata inprogressLogSegment = DLMTestUtil.inprogressLogSegment(ledgerPath, 2L, 100L, 2L);
        inprogressLogSegment.write(zkc, inprogressLogSegment.getZkPath());

        DLSN badLastDLSN = new DLSN(99L, 0L, 0L);
        DLSN goodLastDLSN1 = new DLSN(1L, 100L, 0L);
        DLSN goodLastDLSN2 = new DLSN(2L, 200L, 0L);

        LogRecordWithDLSN badRecord = DLMTestUtil.getLogRecordWithDLSNInstance(badLastDLSN, 100L);
        LogRecordWithDLSN goodRecord1 = DLMTestUtil.getLogRecordWithDLSNInstance(goodLastDLSN1, 100L);
        LogRecordWithDLSN goodRecord2 = DLMTestUtil.getLogRecordWithDLSNInstance(goodLastDLSN2, 200L);

        // Dryrun
        MetadataUpdater dryrunUpdater = new DryrunZkMetadataUpdater(zkc);
        try {
            dryrunUpdater.updateLastRecord(completedLogSegment, badRecord);
            fail("Should fail on updating dlsn that in different log segment");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        try {
            dryrunUpdater.updateLastRecord(inprogressLogSegment, goodRecord2);
            fail("Should fail on updating dlsn for an inprogress log segment");
        } catch (IllegalStateException ise) {
            // expected
        }
        LogSegmentLedgerMetadata updatedCompletedLogSegment =
                dryrunUpdater.updateLastRecord(completedLogSegment, goodRecord1);
        assertEquals(goodLastDLSN1, updatedCompletedLogSegment.getLastDLSN());
        assertEquals(goodRecord1.getTransactionId(), updatedCompletedLogSegment.getLastTxId());
        assertTrue(updatedCompletedLogSegment.isRecordLastPositioninThisSegment(goodRecord1));

        Map<Long, LogSegmentLedgerMetadata> segmentList = readLogSegments(ledgerPath);
        assertEquals(2, segmentList.size());

        LogSegmentLedgerMetadata readCompletedLogSegment = segmentList.get(1L);
        assertNotNull(readCompletedLogSegment);
        assertEquals(completedLogSegment, readCompletedLogSegment);

        LogSegmentLedgerMetadata readInprogressLogSegment = segmentList.get(2L);
        assertNotNull(readInprogressLogSegment);
        assertEquals(inprogressLogSegment, readInprogressLogSegment);

        // Fix the last dlsn
        MetadataUpdater updater = ZkMetadataUpdater.createMetadataUpdater(zkc);
        try {
            updater.updateLastRecord(completedLogSegment, badRecord);
            fail("Should fail on updating dlsn that in different log segment");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        try {
            updater.updateLastRecord(inprogressLogSegment, goodRecord2);
            fail("Should fail on updating dlsn for an inprogress log segment");
        } catch (IllegalStateException ise) {
            // expected
        }
        updatedCompletedLogSegment = updater.updateLastRecord(completedLogSegment, goodRecord1);
        assertEquals(goodLastDLSN1, updatedCompletedLogSegment.getLastDLSN());
        assertEquals(goodRecord1.getTransactionId(), updatedCompletedLogSegment.getLastTxId());
        assertTrue(updatedCompletedLogSegment.isRecordLastPositioninThisSegment(goodRecord1));

        segmentList = readLogSegments(ledgerPath);
        assertEquals(2, segmentList.size());

        readCompletedLogSegment = segmentList.get(1L);
        assertNotNull(readCompletedLogSegment);
        assertEquals(goodLastDLSN1, readCompletedLogSegment.getLastDLSN());
        assertEquals(goodRecord1.getTransactionId(), readCompletedLogSegment.getLastTxId());
        assertTrue(readCompletedLogSegment.isRecordLastPositioninThisSegment(goodRecord1));
        assertEquals(updatedCompletedLogSegment, readCompletedLogSegment);
        assertEquals(completedLogSegment.getCompletionTime(), readCompletedLogSegment.getCompletionTime());
        assertEquals(completedLogSegment.getFirstTxId(), readCompletedLogSegment.getFirstTxId());
        assertEquals(completedLogSegment.getLedgerId(), readCompletedLogSegment.getLedgerId());
        assertEquals(completedLogSegment.getLedgerSequenceNumber(), readCompletedLogSegment.getLedgerSequenceNumber());
        assertEquals(completedLogSegment.getRegionId(), readCompletedLogSegment.getRegionId());
        assertEquals(completedLogSegment.getZkPath(), readCompletedLogSegment.getZkPath());
        assertEquals(completedLogSegment.getZNodeName(), readCompletedLogSegment.getZNodeName());

        readInprogressLogSegment = segmentList.get(2L);
        assertNotNull(readInprogressLogSegment);
        assertEquals(inprogressLogSegment, readInprogressLogSegment);
    }

    @Test
    public void testChangeTruncationStatus() throws Exception {
        String ledgerPath = "/ledgers2";
        zkc.get().create(ledgerPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Map<Long, LogSegmentLedgerMetadata> completedLogSegments = new HashMap<Long, LogSegmentLedgerMetadata>();
        // Create 5 completed log segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentLedgerMetadata segment = DLMTestUtil.completedLogSegment(ledgerPath, i, (i - 1) * 100, i * 100 - 1, 100, i, 100, 0);
            completedLogSegments.put(((long)i), segment);
            LOG.info("Create completed segment {} : {}", segment.getZkPath(), segment);
            segment.write(zkc, segment.getZkPath());
        }

        Map<Long, LogSegmentLedgerMetadata> segmentList = readLogSegments(ledgerPath);
        assertEquals(5, segmentList.size());

        long segmentToModify = 1L;

        // Dryrun
        MetadataUpdater dryrunUpdater = new DryrunZkMetadataUpdater(zkc);
        dryrunUpdater.changeTruncationStatus(segmentList.get(segmentToModify), LogSegmentLedgerMetadata.TruncationStatus.TRUNCATED);

        segmentList = readLogSegments(ledgerPath);
        assertEquals(false, segmentList.get(segmentToModify).isTruncated());

        // change truncation for the 1st log segment
        MetadataUpdater updater = ZkMetadataUpdater.createMetadataUpdater(zkc);
        updater.changeTruncationStatus(segmentList.get(segmentToModify), LogSegmentLedgerMetadata.TruncationStatus.TRUNCATED);

        segmentList = readLogSegments(ledgerPath);
        assertEquals(true, segmentList.get(segmentToModify).isTruncated());
        assertEquals(false, segmentList.get(segmentToModify).isPartiallyTruncated());

        updater = ZkMetadataUpdater.createMetadataUpdater(zkc);
        updater.changeTruncationStatus(segmentList.get(segmentToModify), LogSegmentLedgerMetadata.TruncationStatus.ACTIVE);

        segmentList = readLogSegments(ledgerPath);
        assertEquals(false, segmentList.get(segmentToModify).isTruncated());
        assertEquals(false, segmentList.get(segmentToModify).isPartiallyTruncated());

        updater = ZkMetadataUpdater.createMetadataUpdater(zkc);
        updater.changeTruncationStatus(segmentList.get(segmentToModify), LogSegmentLedgerMetadata.TruncationStatus.PARTIALLY_TRUNCATED);

        segmentList = readLogSegments(ledgerPath);
        assertEquals(false, segmentList.get(segmentToModify).isTruncated());
        assertEquals(true, segmentList.get(segmentToModify).isPartiallyTruncated());

        updater = ZkMetadataUpdater.createMetadataUpdater(zkc);
        updater.changeTruncationStatus(segmentList.get(segmentToModify), LogSegmentLedgerMetadata.TruncationStatus.ACTIVE);

        segmentList = readLogSegments(ledgerPath);
        assertEquals(false, segmentList.get(segmentToModify).isTruncated());
        assertEquals(false, segmentList.get(segmentToModify).isPartiallyTruncated());

    }

}
