package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadata;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.DLUtils;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.data.Stat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestLogSegmentsZK extends TestDistributedLogBase {

    static Logger LOG = LoggerFactory.getLogger(TestLogSegmentsZK.class);

    private static MaxLogSegmentSequenceNo getMaxLogSegmentSequenceNo(ZooKeeperClient zkc, URI uri, String streamName,
                                                                      DistributedLogConfiguration conf) throws Exception {
        Stat stat = new Stat();
        String logSegmentsPath = ZKLogMetadata.getLogSegmentsPath(
                uri, streamName, conf.getUnpartitionedStreamName());
        byte[] data = zkc.get().getData(logSegmentsPath, false, stat);
        Versioned<byte[]> maxLSSNData = new Versioned<byte[]>(data, new ZkVersion(stat.getVersion()));
        return new MaxLogSegmentSequenceNo(maxLSSNData);
    }

    private static void updateMaxLogSegmentSequenceNo(ZooKeeperClient zkc, URI uri, String streamName,
                                                      DistributedLogConfiguration conf, byte[] data) throws Exception {
        String logSegmentsPath = ZKLogMetadata.getLogSegmentsPath(
                uri, streamName, conf.getUnpartitionedStreamName());
        zkc.get().setData(logSegmentsPath, data, -1);
    }

    @Rule
    public TestName testName = new TestName();

    private URI createURI() throws Exception {
        return createDLMURI("/" + testName.getMethodName());
    }

    /**
     * Create Log Segment for an pre-create stream. No max ledger sequence number recorded.
     */
    @Test(timeout = 60000)
    public void testCreateLogSegmentOnPrecreatedStream() throws Exception {
        URI uri = createURI();
        String streamName = testName.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setLockTimeout(99999)
                .setOutputBufferSize(0)
                .setImmediateFlushEnabled(true)
                .setEnableLedgerAllocatorPool(true)
                .setLedgerAllocatorPoolName("test");
        BKDistributedLogNamespace namespace = BKDistributedLogNamespace.newBuilder().conf(conf).uri(uri).build();

        namespace.createLog(streamName);
        MaxLogSegmentSequenceNo max1 = getMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO, max1.getSequenceNumber());
        DistributedLogManager dlm = namespace.openLog(streamName);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }
        MaxLogSegmentSequenceNo max2 = getMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(3, max2.getSequenceNumber());
        dlm.close();
        namespace.close();
    }

    /**
     * Create Log Segment when no max sequence number recorded in /ledgers. e.g. old version.
     */
    @Test(timeout = 60000)
    public void testCreateLogSegmentMissingMaxSequenceNumber() throws Exception {
        URI uri = createURI();
        String streamName = testName.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setLockTimeout(99999)
                .setOutputBufferSize(0)
                .setImmediateFlushEnabled(true)
                .setEnableLedgerAllocatorPool(true)
                .setLedgerAllocatorPoolName("test");
        BKDistributedLogNamespace namespace = BKDistributedLogNamespace.newBuilder().conf(conf).uri(uri).build();

        namespace.createLog(streamName);
        MaxLogSegmentSequenceNo max1 = getMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO, max1.getSequenceNumber());
        DistributedLogManager dlm = namespace.openLog(streamName);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }
        MaxLogSegmentSequenceNo max2 = getMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(3, max2.getSequenceNumber());

        // nuke the max ledger sequence number
        updateMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf, new byte[0]);
        DistributedLogManager dlm1 = namespace.openLog(streamName);
        try {
            dlm1.startLogSegmentNonPartitioned();
            fail("Should fail with unexpected exceptions");
        } catch (UnexpectedException ue) {
            // expected
        } finally {
            dlm1.close();
        }

        // invalid max ledger sequence number
        updateMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf, "invalid-max".getBytes(UTF_8));
        DistributedLogManager dlm2 = namespace.openLog(streamName);
        try {
            dlm2.startLogSegmentNonPartitioned();
            fail("Should fail with unexpected exceptions");
        } catch (UnexpectedException ue) {
            // expected
        } finally {
            dlm2.close();
        }

        dlm.close();
        namespace.close();
    }

    /**
     * Create Log Segment while max sequence number isn't match with list of log segments.
     */
    @Test(timeout = 60000)
    public void testCreateLogSegmentUnmatchMaxSequenceNumber() throws Exception {
        URI uri = createURI();
        String streamName = testName.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setLockTimeout(99999)
                .setOutputBufferSize(0)
                .setImmediateFlushEnabled(true)
                .setEnableLedgerAllocatorPool(true)
                .setLedgerAllocatorPoolName("test");
        BKDistributedLogNamespace namespace = BKDistributedLogNamespace.newBuilder().conf(conf).uri(uri).build();

        namespace.createLog(streamName);
        MaxLogSegmentSequenceNo max1 = getMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO, max1.getSequenceNumber());
        DistributedLogManager dlm = namespace.openLog(streamName);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }
        MaxLogSegmentSequenceNo max2 = getMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(3, max2.getSequenceNumber());

        // update the max ledger sequence number
        updateMaxLogSegmentSequenceNo(namespace.getSharedWriterZKCForDL(), uri, streamName, conf,
                DLUtils.serializeLogSegmentSequenceNumber(99));

        DistributedLogManager dlm1 = namespace.openLog(streamName);
        try {
            BKSyncLogWriter out1 = (BKSyncLogWriter) dlm1.startLogSegmentNonPartitioned();
            out1.write(DLMTestUtil.getLogRecordInstance(numSegments+1));
            out1.closeAndComplete();
            fail("Should fail creating new log segment when encountered unmatch max ledger sequence number");
        } catch (DLIllegalStateException lse) {
            // expected
        } finally {
            dlm1.close();
        }

        DistributedLogManager dlm2 = namespace.openLog(streamName);
        List<LogSegmentMetadata> segments = dlm2.getLogSegments();
        try {
            assertEquals(3, segments.size());
            assertEquals(1L, segments.get(0).getLogSegmentSequenceNumber());
            assertEquals(2L, segments.get(1).getLogSegmentSequenceNumber());
            assertEquals(3L, segments.get(2).getLogSegmentSequenceNumber());
        } finally {
            dlm2.close();
        }

        dlm.close();
        namespace.close();
    }

    @Test(timeout = 60000)
    public void testCompleteLogSegmentConflicts() throws Exception {
        URI uri = createURI();
        String streamName = testName.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setLockTimeout(99999)
                .setOutputBufferSize(0)
                .setImmediateFlushEnabled(true)
                .setEnableLedgerAllocatorPool(true)
                .setLedgerAllocatorPoolName("test");
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder().conf(conf).uri(uri).build();

        namespace.createLog(streamName);
        DistributedLogManager dlm1 = namespace.openLog(streamName);
        DistributedLogManager dlm2 = namespace.openLog(streamName);

        // dlm1 is writing
        BKSyncLogWriter out1 = (BKSyncLogWriter) dlm1.startLogSegmentNonPartitioned();
        out1.write(DLMTestUtil.getLogRecordInstance(1));
        // before out1 complete, out2 is in on recovery
        // it completed the log segments which bump the version of /ledgers znode
        BKAsyncLogWriter out2 = (BKAsyncLogWriter) dlm2.startAsyncLogSegmentNonPartitioned();

        try {
            out1.closeAndComplete();
            fail("Should fail closeAndComplete since other people already completed it.");
        } catch (IOException ioe) {
        }
    }
}
