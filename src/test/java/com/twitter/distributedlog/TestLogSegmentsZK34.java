package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.zk.DataWithStat;
import org.apache.zookeeper.KeeperException;
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

public class TestLogSegmentsZK34 extends TestDistributedLogBase {

    static Logger LOG = LoggerFactory.getLogger(TestLogSegmentsZK34.class);

    private static MaxLedgerSequenceNo getMaxLedgerSequenceNo(ZooKeeperClient zkc, URI uri, String streamName,
                                                              DistributedLogConfiguration conf) throws Exception {
        Stat stat = new Stat();
        String partitionPath = BKDistributedLogManager.getPartitionPath(
                uri, streamName, conf.getUnpartitionedStreamName()) + "/ledgers";
        byte[] data = zkc.get().getData(partitionPath, false, stat);
        DataWithStat dataWithStat = new DataWithStat();
        dataWithStat.setDataWithStat(data, stat);
        return new MaxLedgerSequenceNo(dataWithStat);
    }

    private static void updateMaxLedgerSequenceNo(ZooKeeperClient zkc, URI uri, String streamName,
                                                  DistributedLogConfiguration conf, byte[] data) throws Exception {
        String partitionPath = BKDistributedLogManager.getPartitionPath(
                uri, streamName, conf.getUnpartitionedStreamName()) + "/ledgers";
        zkc.get().setData(partitionPath, data, -1);
    }

    @Rule
    public TestName testName = new TestName();

    private URI createURI() throws Exception {
        return DLMTestUtil.createDLMURI("/" + testName.getMethodName());
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
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        BKDistributedLogManager.createUnpartitionedStream(conf, factory.getSharedWriterZKCForDL(), uri, streamName);
        MaxLedgerSequenceNo max1 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO, max1.getSequenceNumber());
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }
        MaxLedgerSequenceNo max2 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(3, max2.getSequenceNumber());
        dlm.close();
        factory.close();
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
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        BKDistributedLogManager.createUnpartitionedStream(conf, factory.getSharedWriterZKCForDL(), uri, streamName);
        MaxLedgerSequenceNo max1 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO, max1.getSequenceNumber());
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }
        MaxLedgerSequenceNo max2 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(3, max2.getSequenceNumber());

        // nuke the max ledger sequence number
        updateMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf, new byte[0]);
        DistributedLogManager dlm1 = factory.createDistributedLogManagerWithSharedClients(streamName);
        try {
            BKUnPartitionedSyncLogWriter out1 = (BKUnPartitionedSyncLogWriter) dlm1.startLogSegmentNonPartitioned();
            out1.write(DLMTestUtil.getLogRecordInstance(numSegments));
            out1.closeAndComplete();
            MaxLedgerSequenceNo max3 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
            assertEquals(4, max3.getSequenceNumber());
        } finally {
            dlm1.close();
        }

        // invalid max ledger sequence number
        updateMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf, "invalid-max".getBytes(UTF_8));
        DistributedLogManager dlm2 = factory.createDistributedLogManagerWithSharedClients(streamName);
        try {
            BKUnPartitionedSyncLogWriter out2 = (BKUnPartitionedSyncLogWriter) dlm2.startLogSegmentNonPartitioned();
            out2.write(DLMTestUtil.getLogRecordInstance(numSegments+1));
            out2.closeAndComplete();
            MaxLedgerSequenceNo max4 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
            assertEquals(5, max4.getSequenceNumber());
        } finally {
            dlm2.close();
        }

        dlm.close();
        factory.close();
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
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        BKDistributedLogManager.createUnpartitionedStream(conf, factory.getSharedWriterZKCForDL(), uri, streamName);
        MaxLedgerSequenceNo max1 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO, max1.getSequenceNumber());
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        final int numSegments = 3;
        for (int i = 0; i < numSegments; i++) {
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(i));
            out.closeAndComplete();
        }
        MaxLedgerSequenceNo max2 = getMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf);
        assertEquals(3, max2.getSequenceNumber());

        // update the max ledger sequence number
        updateMaxLedgerSequenceNo(factory.getSharedWriterZKCForDL(), uri, streamName, conf, MaxLedgerSequenceNo.toBytes(99));

        DistributedLogManager dlm1 = factory.createDistributedLogManagerWithSharedClients(streamName);
        try {
            BKUnPartitionedSyncLogWriter out1 = (BKUnPartitionedSyncLogWriter) dlm1.startLogSegmentNonPartitioned();
            out1.write(DLMTestUtil.getLogRecordInstance(numSegments+1));
            out1.closeAndComplete();
            fail("Should fail creating new log segment when encountered unmatch max ledger sequence number");
        } catch (DLIllegalStateException lse) {
            // expected
        } finally {
            dlm1.close();
        }

        DistributedLogManager dlm2 = factory.createDistributedLogManagerWithSharedClients(streamName);
        List<LogSegmentLedgerMetadata> segments = dlm2.getLogSegments();
        try {
            assertEquals(3, segments.size());
            assertEquals(1L, segments.get(0).getLedgerSequenceNumber());
            assertEquals(2L, segments.get(1).getLedgerSequenceNumber());
            assertEquals(3L, segments.get(2).getLedgerSequenceNumber());
        } finally {
            dlm2.close();
        }

        dlm.close();
        factory.close();
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
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        BKDistributedLogManager.createUnpartitionedStream(conf, factory.getSharedWriterZKCForDL(), uri, streamName);
        DistributedLogManager dlm1 = factory.createDistributedLogManagerWithSharedClients(streamName);
        DistributedLogManager dlm2 = factory.createDistributedLogManagerWithSharedClients(streamName);

        // dlm1 is writing
        BKUnPartitionedSyncLogWriter out1 = (BKUnPartitionedSyncLogWriter) dlm1.startLogSegmentNonPartitioned();
        out1.write(DLMTestUtil.getLogRecordInstance(1));
        // before out1 complete, out2 is in on recovery
        BKUnPartitionedAsyncLogWriter out2 = (BKUnPartitionedAsyncLogWriter) dlm2.startAsyncLogSegmentNonPartitioned();
        // it completed the log segments which bump the version of /ledgers znode
        out2.recover();

        try {
            out1.closeAndComplete(true);
            fail("Should fail closeAndComplete since other people already completed it.");
        } catch (IOException ioe) {
        }
    }
}
