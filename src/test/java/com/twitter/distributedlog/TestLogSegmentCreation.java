package com.twitter.distributedlog;

import java.net.URI;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestLogSegmentCreation extends TestDistributedLogBase {

    static Logger LOG = LoggerFactory.getLogger(TestLogSegmentCreation.class);

    @Test(timeout = 60000)
    public void testCreateLogSegmentAfterLoseLock() throws Exception {
        URI uri = createDLMURI("/LogSegmentCreation");
        String name = "distrlog-createlogsegment-afterloselock";
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setLockTimeout(99999)
                .setOutputBufferSize(0)
                .setImmediateFlushEnabled(true)
                .setEnableLedgerAllocatorPool(true)
                .setLedgerAllocatorPoolName("test");
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(name);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }

        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        LOG.info("Segments : {}", segments);
        assertEquals(3, segments.size());

        final DistributedLogManager dlm1 = factory.createDistributedLogManagerWithSharedClients(name);
        final DistributedLogManager dlm2 = factory.createDistributedLogManagerWithSharedClients(name);

        BKUnPartitionedAsyncLogWriter writer1 = (BKUnPartitionedAsyncLogWriter) dlm1.startAsyncLogSegmentNonPartitioned();
        LOG.info("Created writer 1.");
        BKUnPartitionedSyncLogWriter writer2 = (BKUnPartitionedSyncLogWriter) dlm2.startLogSegmentNonPartitioned();
        LOG.info("Created writer 2.");
        writer2.write(DLMTestUtil.getLogRecordInstance(numSegments));
        writer2.closeAndComplete();

        LOG.info("ZK Version : {}", DistributedLogConstants.ZK_VERSION.getVersion());

        boolean isZK33 = DistributedLogConstants.ZK_VERSION.getVersion().equals(DistributedLogConstants.ZK33);

        try {
            writer1.write(DLMTestUtil.getLogRecordInstance(numSegments + 1)).get();
            if (!isZK33) {
                fail("Should fail on zk34 version.");
            }
        } catch (Throwable t) {
            LOG.error("Failed to write entry : ", t);
            if (isZK33) {
                fail("Shouldn't fail on zk33 version.");
            }
        }

        segments = dlm.getLogSegments();

        boolean hasInprogress = false;
        boolean hasDuplicatedSegment = false;
        long nextSeqNo = segments.get(0).getLedgerSequenceNumber();
        for (int i = 1; i < segments.size(); i++) {
            LogSegmentLedgerMetadata segment = segments.get(i);
            assertTrue(segment.getLedgerSequenceNumber() >= nextSeqNo);
            if (segment.getLedgerSequenceNumber() == nextSeqNo) {
                hasDuplicatedSegment = true;
            }
            nextSeqNo = segment.getLedgerSequenceNumber();
            if (segment.isInProgress()) {
                hasInprogress = true;
            }
        }
        if (isZK33) {
            assertEquals(5, segments.size());
            assertTrue(hasInprogress);
            assertTrue(hasDuplicatedSegment);
        } else {
            assertEquals(4, segments.size());
            assertFalse(hasInprogress);
            assertFalse(hasDuplicatedSegment);
        }

        LOG.info("Segments : duplicated = {}, inprogress = {}, {}",
                 new Object[] { hasDuplicatedSegment, hasInprogress, segments });

        dlm1.close();
        dlm2.close();
        dlm.close();

        factory.close();
    }
}
