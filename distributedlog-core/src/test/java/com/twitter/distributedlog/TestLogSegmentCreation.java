package com.twitter.distributedlog;

import java.net.URI;
import java.util.List;

import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.util.Await;
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
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();
        DistributedLogManager dlm = namespace.openLog(name);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        LOG.info("Segments : {}", segments);
        assertEquals(3, segments.size());

        final DistributedLogManager dlm1 = namespace.openLog(name);
        final DistributedLogManager dlm2 = namespace.openLog(name);

        BKAsyncLogWriter writer1 = (BKAsyncLogWriter) dlm1.startAsyncLogSegmentNonPartitioned();
        LOG.info("Created writer 1.");
        BKSyncLogWriter writer2 = (BKSyncLogWriter) dlm2.startLogSegmentNonPartitioned();
        LOG.info("Created writer 2.");
        writer2.write(DLMTestUtil.getLogRecordInstance(numSegments));
        writer2.closeAndComplete();

        LOG.info("ZK Version : {}", DistributedLogConstants.ZK_VERSION.getVersion());

        boolean isZK33 = DistributedLogConstants.ZK_VERSION.getVersion().equals(DistributedLogConstants.ZK33);

        try {
            Await.result(writer1.write(DLMTestUtil.getLogRecordInstance(numSegments + 1)));
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
        long nextSeqNo = segments.get(0).getLogSegmentSequenceNumber();
        for (int i = 1; i < segments.size(); i++) {
            LogSegmentMetadata segment = segments.get(i);
            assertTrue(segment.getLogSegmentSequenceNumber() >= nextSeqNo);
            if (segment.getLogSegmentSequenceNumber() == nextSeqNo) {
                hasDuplicatedSegment = true;
            }
            nextSeqNo = segment.getLogSegmentSequenceNumber();
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

        namespace.close();
    }
}
