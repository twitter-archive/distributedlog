package com.twitter.distributedlog;

import com.twitter.distributedlog.util.FutureUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestInterleavedReaders extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestInterleavedReaders.class);

    static {
        conf.setOutputBufferSize(0);
        conf.setImmediateFlushEnabled(true);
    }

    private int drainStreams(LogReader reader0, int num0, LogReader reader1, int num1)
            throws Exception {
        // Allow time for watches to fire
        Thread.sleep(15);
        int numTrans = 0;
        LogRecord record;
        int i = 0;
        while (i < num0) {
            record = reader0.readNext(false);
            if (null != record) {
                assertTrue((record.getTransactionId() % 2 == 0));
                DLMTestUtil.verifyLogRecord(record);
                numTrans++;
                i++;
                LOG.info("Read record {}", record);
            }
        }
        i = 0;
        while (i < num1) {
            record = reader1.readNext(false);
            if (null != record) {
                assertTrue((record.getTransactionId() % 2 == 1));
                DLMTestUtil.verifyLogRecord(record);
                numTrans++;
                i++;
                LOG.info("Read record {}", record);
            }
        }
        return numTrans;
    }

    @Test(timeout = 60000)
    public void testInterleavedReaders() throws Exception {
        String name = "distrlog-interleaved";
        BKDistributedLogManager dlmwrite0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmreader0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmwrite1 = createNewDLM(conf, name + "-1");
        BKDistributedLogManager dlmreader1 = createNewDLM(conf, name + "-1");

        LogReader reader0 = null;
        LogReader reader1 = null;
        long txid = 1;
        int numTrans = 0;

        BKAsyncLogWriter writer0 = dlmwrite0.startAsyncLogSegmentNonPartitioned();
        BKAsyncLogWriter writer1 = dlmwrite1.startAsyncLogSegmentNonPartitioned();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                FutureUtils.result(writer1.write(DLMTestUtil.getLogRecordInstance(txid++)));
                FutureUtils.result(writer0.write(DLMTestUtil.getLogRecordInstance(txid++)));
            }
            FutureUtils.result(writer1.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            FutureUtils.result(writer0.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            if (null == reader0) {
                reader0 = dlmreader0.getInputStream(1);
            }
            if (null == reader1) {
                reader1 = dlmreader1.getInputStream(1);
            }
            numTrans += drainStreams(reader0, 10, reader1, 10);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader0.close();
        dlmwrite0.close();
        dlmreader1.close();
        dlmwrite1.close();
    }

    @Test(timeout = 60000)
    public void testInterleavedReadersWithRollingEdge() throws Exception {
        String name = "distrlog-interleaved-rolling-edge";
        BKDistributedLogManager dlmwrite0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmreader0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmwrite1 = createNewDLM(conf, name + "-1");
        BKDistributedLogManager dlmreader1 = createNewDLM(conf, name + "-1");

        LogReader reader0 = null;
        LogReader reader1 = null;
        long txid = 1;
        int numTrans = 0;

        BKAsyncLogWriter writer0 = dlmwrite0.startAsyncLogSegmentNonPartitioned();
        BKAsyncLogWriter writer1 = dlmwrite1.startAsyncLogSegmentNonPartitioned();
        for (long j = 1; j <= 4; j++) {
            if (j > 1) {
                writer0.setForceRolling(true);
                writer1.setForceRolling(true);
            }
            for (int k = 1; k <= 2; k++) {
                FutureUtils.result(writer1.write(DLMTestUtil.getLogRecordInstance(txid++)));
                FutureUtils.result(writer0.write(DLMTestUtil.getLogRecordInstance(txid++)));
                writer0.setForceRolling(false);
                writer1.setForceRolling(false);
            }
            FutureUtils.result(writer1.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            FutureUtils.result(writer0.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            LOG.info("Completed {} write", j);
            if (null == reader0) {
                reader0 = dlmreader0.getInputStream(1);
            }
            if (null == reader1) {
                reader1 = dlmreader1.getInputStream(1);
            }
            numTrans += drainStreams(reader0, 2, reader1, 2);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader0.close();
        dlmwrite0.close();
        dlmreader1.close();
        dlmwrite1.close();
    }

    @Test(timeout = 60000)
    public void testInterleavedReadersWithRolling() throws Exception {
        String name = "distrlog-interleaved-rolling";
        BKDistributedLogManager dlmwrite0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmreader0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmwrite1 = createNewDLM(conf, name + "-1");
        BKDistributedLogManager dlmreader1 = createNewDLM(conf, name + "-1");

        LogReader reader0 = null;
        LogReader reader1 = null;
        long txid = 1;
        int numTrans = 0;

        BKAsyncLogWriter writer0 = dlmwrite0.startAsyncLogSegmentNonPartitioned();
        BKAsyncLogWriter writer1 = dlmwrite1.startAsyncLogSegmentNonPartitioned();
        for (long j = 1; j <= 2; j++) {
            for (int k = 1; k <= 6; k++) {
                if (k == 3) {
                    writer0.setForceRolling(true);
                    writer1.setForceRolling(true);
                }
                FutureUtils.result(writer1.write(DLMTestUtil.getLogRecordInstance(txid++)));
                FutureUtils.result(writer0.write(DLMTestUtil.getLogRecordInstance(txid++)));
                writer0.setForceRolling(false);
                writer1.setForceRolling(false);
            }
            FutureUtils.result(writer1.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            FutureUtils.result(writer0.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            if (null == reader0) {
                reader0 = dlmreader0.getInputStream(1);
            }
            if (null == reader1) {
                reader1 = dlmreader1.getInputStream(1);
            }
            numTrans += drainStreams(reader0, 6, reader1, 6);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader0.close();
        dlmwrite0.close();
        dlmreader1.close();
        dlmwrite1.close();
    }

    @Test(timeout = 60000)
    public void testInterleavedReadersWithCleanup() throws Exception {
        String name = "distrlog-interleaved-cleanup";
        BKDistributedLogManager dlmwrite0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmwrite1 = createNewDLM(conf, name + "-1");
        long txid = 1;
        Long retentionPeriodOverride = null;

        BKAsyncLogWriter writer0 = dlmwrite0.startAsyncLogSegmentNonPartitioned();
        BKAsyncLogWriter writer1 = dlmwrite1.startAsyncLogSegmentNonPartitioned();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                if (k == 5) {
                    writer0.setForceRolling(true);
                    writer0.overRideMinTimeStampToKeep(retentionPeriodOverride);
                    writer1.setForceRolling(true);
                    writer1.overRideMinTimeStampToKeep(retentionPeriodOverride);
                }
                DLSN dlsn1 = FutureUtils.result(writer1.write(DLMTestUtil.getLogRecordInstance(txid++)));
                LOG.info("writer1 write record {}", dlsn1);
                DLSN dlsn0 = FutureUtils.result(writer0.write(DLMTestUtil.getLogRecordInstance(txid++)));
                LOG.info("writer0 write record {}", dlsn0);
                if (k == 5) {
                    writer0.setForceRolling(false);
                    writer1.setForceRolling(false);
                    retentionPeriodOverride = System.currentTimeMillis();
                }
                Thread.sleep(5);
            }
            FutureUtils.result(writer1.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            FutureUtils.result(writer0.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
        }
        writer0.close();
        writer1.close();

        DistributedLogManager dlmreader0 = createNewDLM(conf, name + "-0");
        DistributedLogManager dlmreader1 = createNewDLM(conf, name + "-1");
        LogReader reader0 = dlmreader0.getInputStream(1);
        LogReader reader1 = dlmreader1.getInputStream(1);
        int numTrans = drainStreams(reader0, 15, reader1, 15);
        assertEquals(30, numTrans);
        reader0.close();
        reader1.close();
        dlmreader0.close();
        dlmwrite0.close();
        dlmreader1.close();
        dlmwrite1.close();
    }

    @Test(timeout = 60000)
    public void testInterleavedReadersWithRecovery() throws Exception {
        String name = "distrlog-interleaved-recovery";
        BKDistributedLogManager dlmwrite0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmreader0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmwrite1 = createNewDLM(conf, name + "-1");
        BKDistributedLogManager dlmreader1 = createNewDLM(conf, name + "-1");

        LogReader reader0 = null;
        LogReader reader1 = null;
        long txid = 1;
        int numTrans = 0;

        BKAsyncLogWriter writer0 = dlmwrite0.startAsyncLogSegmentNonPartitioned();
        BKAsyncLogWriter writer1 = dlmwrite1.startAsyncLogSegmentNonPartitioned();
        for (long j = 1; j <= 2; j++) {
            for (int k = 1; k <= 6; k++) {
                if (k == 3) {
                    writer0.setForceRecovery(true);
                    writer1.setForceRecovery(true);
                }
                DLSN dlsn1 = FutureUtils.result(writer1.write(DLMTestUtil.getLogRecordInstance(txid++)));
                LOG.info("writer1 write record {} - txid = {}", dlsn1, txid-1);
                DLSN dlsn0 = FutureUtils.result(writer0.write(DLMTestUtil.getLogRecordInstance(txid++)));
                LOG.info("writer0 write record {} - txid = {}", dlsn0, txid-1);
                writer0.setForceRecovery(false);
                writer1.setForceRecovery(false);
            }
            FutureUtils.result(writer1.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            FutureUtils.result(writer0.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            if (null == reader0) {
                reader0 = dlmreader0.getInputStream(1);
            }
            if (null == reader1) {
                reader1 = dlmreader1.getInputStream(1);
            }
            numTrans += drainStreams(reader0, 6, reader1, 6);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        assertEquals(txid - 1,
            dlmreader0.getLogRecordCount() + dlmreader1.getLogRecordCount());
        dlmreader0.close();
        dlmwrite0.close();
        dlmreader1.close();
        dlmwrite1.close();
    }

    @Test(timeout = 60000)
    public void testInterleavedReadersWithRollingEdgeUnPartitioned() throws Exception {
        String name = "distrlog-interleaved-rolling-edge-unpartitioned";
        BKDistributedLogManager dlmwrite0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmreader0 = createNewDLM(conf, name + "-0");
        BKDistributedLogManager dlmwrite1 = createNewDLM(conf, name + "-1");
        BKDistributedLogManager dlmreader1 = createNewDLM(conf, name + "-1");

        LogReader reader0 = null;
        LogReader reader1 = null;
        long txid = 1;
        int numTrans = 0;

        BKAsyncLogWriter writer0 = dlmwrite0.startAsyncLogSegmentNonPartitioned();
        BKAsyncLogWriter writer1 = dlmwrite1.startAsyncLogSegmentNonPartitioned();
        for (long j = 1; j <= 4; j++) {
            if (j > 1) {
                writer0.setForceRolling(true);
                writer1.setForceRolling(true);
            }
            for (int k = 1; k <= 2; k++) {
                FutureUtils.result(writer1.write(DLMTestUtil.getLogRecordInstance(txid++)));
                FutureUtils.result(writer0.write(DLMTestUtil.getLogRecordInstance(txid++)));
                writer0.setForceRolling(false);
                writer1.setForceRolling(false);
            }
            FutureUtils.result(writer1.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            FutureUtils.result(writer0.writeControlRecord(DLMTestUtil.getLogRecordInstance(txid-1)));
            if (null == reader0) {
                reader0 = dlmreader0.getInputStream(1);
            }
            if (null == reader1) {
                reader1 = dlmreader1.getInputStream(1);
            }
            numTrans += drainStreams(reader0, 2, reader1, 2);
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader0.close();
        dlmreader1.close();
    }

    @Test
    public void testFactorySharedClients() throws Exception {
        String name = "distrlog-factorysharedclients";
        testFactory(name, true);
    }

    @Test(timeout = 60000)
    public void testFactorySharedZK() throws Exception {
        String name = "distrlog-factorysharedZK";
        testFactory(name, false);
    }

    @SuppressWarnings("deprecation")
    private void testFactory(String name, boolean shareBK) throws Exception {
        int count = 3;
        URI uri = createDLMURI("/" + name);
        ensureURICreated(uri);
        BKDistributedLogNamespace namespace = BKDistributedLogNamespace.newBuilder()
                .conf(conf).uri(uri).build();
        DistributedLogManager[] dlms = new DistributedLogManager[count];
        for (int s = 0; s < count; s++) {
            if (shareBK) {
                dlms[s] = namespace.createDistributedLogManager(name + String.format("%d", s),
                        DistributedLogManagerFactory.ClientSharingOption.SharedClients);
            } else {
                dlms[s] = namespace.createDistributedLogManager(name + String.format("%d", s),
                        DistributedLogManagerFactory.ClientSharingOption.SharedZKClientPerStreamBKClient);
            }
        }

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            BKSyncLogWriter[] writers = new BKSyncLogWriter[count];
            for (int s = 0; s < count; s++) {
                writers[s] = (BKSyncLogWriter)(dlms[s].startLogSegmentNonPartitioned());
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
                    dlms[0] = namespace.createDistributedLogManager(name + String.format("%d", 0),
                            DistributedLogManagerFactory.ClientSharingOption.SharedClients);
                } else {
                    dlms[0] = namespace.createDistributedLogManager(name + String.format("%d", 0),
                            DistributedLogManagerFactory.ClientSharingOption.SharedZKClientPerStreamBKClient);
                }
            }

        }

        for (int s = 0; s < count; s++) {
            dlms[s].close();
        }

        namespace.close();
    }
}
