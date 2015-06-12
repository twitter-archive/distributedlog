package com.twitter.distributedlog;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestInterleavedReaders extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestInterleavedReaders.class);

    private int drainStreams(LogReader reader0, LogReader reader1) throws Exception {
        // Allow time for watches to fire
        Thread.sleep(15);
        int numTrans = 0;
        LogRecord record = reader0.readNext(false);
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader0.readNext(false);
        }
        record = reader1.readNext(false);
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader1.readNext(false);
        }
        return numTrans;
    }

    @Test
    public void testInterleavedReaders() throws Exception {
        String name = "distrlog-interleaved";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
        int numTrans = drainStreams(reader0, reader1);
        assertEquals((txid - 1), numTrans);

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
            numTrans += drainStreams(reader0, reader1);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testInterleavedReadersWithRollingEdge() throws Exception {
        String name = "distrlog-interleaved-rolling-edge";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);


        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
        int numTrans = drainStreams(reader0, reader1);
        assertEquals((txid - 1), numTrans);

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            if (j > 1) {
                ((BKPartitionAwareLogWriter) writer).setForceRolling(true);
            }
            for (int k = 1; k <= 2; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
                ((BKPartitionAwareLogWriter) writer).setForceRolling(false);
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
            numTrans += drainStreams(reader0, reader1);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testInterleavedReadersWithRolling() throws Exception {
        String name = "distrlog-interleaved-rolling";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);


        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
        int numTrans = drainStreams(reader0, reader1);
        assertEquals((txid - 1), numTrans);

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 2; j++) {
            for (int k = 1; k <= 6; k++) {
                if (k == 3) {
                    ((BKPartitionAwareLogWriter) writer).setForceRolling(true);
                }
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
                ((BKPartitionAwareLogWriter) writer).setForceRolling(false);
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
            numTrans += drainStreams(reader0, reader1);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testInterleavedReadersWithCleanup() throws Exception {
        String name = "distrlog-interleaved-cleanup";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        long txid = 1;
        Long retentionPeriodOverride = null;

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                if (k == 5) {
                    ((BKPartitionAwareLogWriter) writer).setForceRolling(true);
                    ((BKPartitionAwareLogWriter) writer).overRideMinTimeStampToKeep(retentionPeriodOverride);
                }
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
                if (k == 5) {
                    ((BKPartitionAwareLogWriter) writer).setForceRolling(false);
                    retentionPeriodOverride = System.currentTimeMillis();
                }
                Thread.sleep(5);
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
        }
        writer.close();

        DistributedLogManager dlmreader = createNewDLM(conf, name);
        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        int numTrans = drainStreams(reader0, reader1);
        assertEquals(32, numTrans);
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testInterleavedReadersWithRecovery() throws Exception {
        String name = "distrlog-interleaved-recovery";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
        int numTrans = drainStreams(reader0, reader1);
        assertEquals((txid - 1), numTrans);

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 2; j++) {
            for (int k = 1; k <= 6; k++) {
                if (k == 3) {
                    ((BKPartitionAwareLogWriter) writer).setForceRecovery(true);
                }
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
                ((BKPartitionAwareLogWriter) writer).setForceRecovery(false);
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
            numTrans += drainStreams(reader0, reader1);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        assertEquals(txid - 1,
            dlmreader.getLogRecordCount(new PartitionId(0)) + dlmreader.getLogRecordCount(new PartitionId(1)));
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testInterleavedReadersWithRollingEdgeUnPartitioned() throws Exception {
        String name = "distrlog-interleaved-rolling-edge-unpartitioned";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
        int numTrans = drainStreams(reader0, reader1);
        assertEquals((txid - 1), numTrans);

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            if (j > 1) {
                ((BKPartitionAwareLogWriter) writer).setForceRolling(true);
            }
            for (int k = 1; k <= 2; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
                ((BKPartitionAwareLogWriter) writer).setForceRolling(false);
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
            numTrans += drainStreams(reader0, reader1);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
    }

    @Test
    public void testFactorySharedClients() throws Exception {
        String name = "distrlog-factorysharedclients";
        testFactory(name, true);
    }

    @Test
    public void testFactorySharedZK() throws Exception {
        String name = "distrlog-factorysharedZK";
        testFactory(name, false);
    }

    private void testFactory(String name, boolean shareBK) throws Exception {
        int count = 3;
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, createDLMURI("/" + name));
        DistributedLogManager[] dlms = new DistributedLogManager[count];
        for (int s = 0; s < count; s++) {
            if (shareBK) {
                dlms[s] = factory.createDistributedLogManager(name + String.format("%d", s),
                        DistributedLogManagerFactory.ClientSharingOption.SharedClients);
            } else {
                dlms[s] = factory.createDistributedLogManager(name + String.format("%d", s),
                        DistributedLogManagerFactory.ClientSharingOption.SharedZKClientPerStreamBKClient);
            }
        }

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            BKUnPartitionedSyncLogWriter[] writers = new BKUnPartitionedSyncLogWriter[count];
            for (int s = 0; s < count; s++) {
                writers[s] = (BKUnPartitionedSyncLogWriter)(dlms[s].startLogSegmentNonPartitioned());
            }

            for (long j = 0; j < 1; j++) {
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                for (int s = 0; s < count; s++) {
                    writers[s].write(record);
                }
            }
            for (int s = 0; s < count; s++) {
                writers[s].closeAndComplete();
            }

            if (i < 2) {
                // Restart the zeroth stream and make sure that the other streams can
                // continue without restart
                dlms[0].close();
                if (shareBK) {
                    dlms[0] = factory.createDistributedLogManager(name + String.format("%d", 0),
                            DistributedLogManagerFactory.ClientSharingOption.SharedClients);
                } else {
                    dlms[0] = factory.createDistributedLogManager(name + String.format("%d", 0),
                            DistributedLogManagerFactory.ClientSharingOption.SharedZKClientPerStreamBKClient);
                }
            }

        }

        for (int s = 0; s < count; s++) {
            dlms[s].close();
        }

        factory.close();
    }
}
