package com.twitter.distributedlog.v2;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.LogReadException;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogWriter;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.IdleReaderException;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestInterleavedReaders {
    static final Logger LOG = LoggerFactory.getLogger(TestBookKeeperDistributedLogManager.class);

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration();
    static {
        conf.setLockTimeout(10);
    }
    private ZooKeeper zkc;
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numBookies = 3;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", 7000);
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    private int drainStreams(LogReader reader0, LogReader reader1) throws Exception {
        // Allow time for watches to fire
        Thread.sleep(5);
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
        BKDistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        BKDistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        BKDistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        BKDistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        BKDistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        BKDistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        BKDistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
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

        BKDistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);
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
        BKDistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        BKDistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        BKDistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        BKDistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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

    private BKContinuousLogReader readNonBlocking(DistributedLogManager dlm, boolean notification, boolean forceBlockingRead, boolean closeReader, boolean forceStall) throws Exception {
        return readNonBlocking(dlm, notification, forceBlockingRead, closeReader, forceStall, DEFAULT_SEGMENT_SIZE);
    }

    private BKContinuousLogReader readNonBlocking(DistributedLogManager dlm, boolean notification, boolean forceBlockingRead, boolean closeReader, boolean forceStall, long segmentSize) throws Exception {
        BKContinuousLogReader reader = (BKContinuousLogReader)dlm.getInputStream(1);
        if (forceStall) {
            reader.disableReadAheadZKNotification();
        }

        long numTrans = 0;
        long lastTxId = -1;

        boolean exceptionEncountered = false;
        try {
            while (true) {
                if (forceBlockingRead) {
                    reader.setForceBlockingRead((Math.random() < 0.5));
                }

                LogRecord record = reader.readNext(true);
                if (null != record) {
                    DLMTestUtil.verifyLogRecord(record);
                    assert (lastTxId < record.getTransactionId());
                    lastTxId = record.getTransactionId();
                    numTrans++;
                    continue;
                }

                if (numTrans >= (3 * segmentSize)) {
                    break;
                }

                if (notification) {
                    final CountDownLatch syncLatch = new CountDownLatch(1);
                    reader.registerNotification(new ReaderNotification() {
                        @Override
                        public void notifyNextRecordAvailable() {
                            syncLatch.countDown();
                        }
                    });
                    syncLatch.await();
                } else {
                    TimeUnit.MILLISECONDS.sleep(2);
                }
            }
        } catch (LogReadException readexc) {
            exceptionEncountered = true;
        } catch (LogNotFoundException exc) {
            exceptionEncountered = true;
        }
        assert(!exceptionEncountered);
        if (closeReader) {
            reader.close();
            return null;
        } else {
            return reader;
        }
    }

    long writeRecordsForNonBlockingReads(DistributedLogManager dlm, boolean recover) throws Exception {
        return writeRecordsForNonBlockingReads(dlm, recover, DEFAULT_SEGMENT_SIZE);
    }

    long writeRecordsForNonBlockingReads(DistributedLogManager dlm, boolean recover, long segmentSize) throws Exception {
        LOG.info("writeRecordsForNonBlockingReads started");
        long txId = 1;
        for (long i = 0; i < 3; i++) {
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j < segmentSize; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txId++));
            }
            if (recover) {
                writer.setReadyToFlush();
                writer.flushAndSync();
                writer.write(DLMTestUtil.getLogRecordInstance(txId++));
                writer.setReadyToFlush();
                TimeUnit.MILLISECONDS.sleep(300);
                writer.abort();
                LOG.debug("Recovering Segments");
                BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
                blplm.recoverIncompleteLogSegments();
                blplm.close();
                LOG.debug("Recovered Segments");
            } else {
                writer.write(DLMTestUtil.getLogRecordInstance(txId++));
                writer.closeAndComplete();
            }
            LOG.info("writeRecordsForNonBlockingReads Finished writing a single segment");
            TimeUnit.MILLISECONDS.sleep(300);
        }
        return txId;
    }


    @Test(timeout = 10000)
    public void nonBlockingRead() throws Exception {
        String name = "distrlog-non-blocking-reader";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        writeRecordsForNonBlockingReads(dlm, false);
                    } catch (Exception exc) {
                        currentThread.interrupt();
                    }

                }
            }, 100, TimeUnit.MILLISECONDS);

        readNonBlocking(dlm, false, false, true, false);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void nonBlockingReadWithForceBlockingReads() throws Exception {
        String name = "distrlog-non-blocking-reader-force-blocking";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        writeRecordsForNonBlockingReads(dlm, false);
                    } catch (Exception exc) {
                        currentThread.interrupt();
                    }

                }
            }, 100, TimeUnit.MILLISECONDS);

        readNonBlocking(dlm, false, true, true, false);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void nonBlockingReadRecovery() throws Exception {
        String name = "distrlog-non-blocking-reader-recovery";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(16);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setReadAheadMaxEntries(10);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        writeRecordsForNonBlockingReads(dlm, true);
                    } catch (Exception exc) {
                        currentThread.interrupt();
                    }

                }
            }, 100, TimeUnit.MILLISECONDS);

        readNonBlocking(dlm, false, false, true, false);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void nonBlockingReadNotification() throws Exception {
        String name = "distrlog-non-blocking-reader-notification";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        writeRecordsForNonBlockingReads(dlm, false);
                    } catch (Exception exc) {
                        currentThread.interrupt();
                    }

                }
            }, 100, TimeUnit.MILLISECONDS);

        readNonBlocking(dlm, true, false, true, false);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }


    @Test(timeout = 10000)
    public void nonBlockingReadRecoveryWithNotification() throws Exception {
        String name = "distrlog-non-blocking-reader-recovery-notification";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(16);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setReadAheadMaxEntries(10);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        writeRecordsForNonBlockingReads(dlm, true);
                    } catch (Exception exc) {
                        currentThread.interrupt();
                    }

                }
            }, 100, TimeUnit.MILLISECONDS);

        readNonBlocking(dlm, true, false, true, false);

        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void nonBlockingReadIdleError() throws Exception {
        String name = "distrlog-non-blocking-reader-error";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(50);
        confLocal.setReaderIdleErrorThresholdMillis(100);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        writeRecordsForNonBlockingReads(dlm, false);
                    } catch (Exception exc) {
                        currentThread.interrupt();
                    }

                }
            }, 100, TimeUnit.MILLISECONDS);

        boolean exceptionEncountered = false;
        try {
            readNonBlocking(dlm, false, false, true, false);
        } catch (IdleReaderException exc) {
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void nonBlockingReadAheadStall() throws Exception {
        String name = "distrlog-non-blocking-reader-stall";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(3);
        confLocal.setReaderIdleWarnThresholdMillis(500);
        confLocal.setReaderIdleErrorThresholdMillis(30000);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        writeRecordsForNonBlockingReads(dlm, false, 3);
                    } catch (Exception exc) {
                        currentThread.interrupt();
                    }

                }
            }, 10, TimeUnit.MILLISECONDS);

        boolean exceptionEncountered = false;
        try {
            readNonBlocking(dlm, false, false, true, true, 3);
        } catch (IdleReaderException exc) {
            LOG.info("Exception encountered", exc);
            exceptionEncountered = true;
        }
        assert(!exceptionEncountered);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }


    @Test(timeout = 10000)
    public void nonBlockingReadNoInProgress() throws Exception {
        String name = "distrlog-non-blocking-reader-noinprogress";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(15000);
        confLocal.setReaderIdleErrorThresholdMillis(30000);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        long txId = writeRecordsForNonBlockingReads(dlm, false);

        BKContinuousLogReader reader = readNonBlocking(dlm, false, false, false, false);
        assert(!currentThread.isInterrupted());

        int numIterations = 30;
        long attempts = reader.getOpenReaderAttemptCount();
        for (int index = 0; index < numIterations; index++) {
            LogRecord record = reader.readNext(true);
            assert (null == record);
        }
        assert (reader.getOpenReaderAttemptCount() - attempts <= numIterations / 2);

        attempts = reader.getOpenReaderAttemptCount();
        for (int index = 0; index < numIterations; index++) {
            LogRecord record = reader.readNext(false);
            assert (null == record);
        }
        assert (reader.getOpenReaderAttemptCount() - attempts >= numIterations / 2);

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txId++));
        }
        writer.closeAndComplete();

        long numTrans = 0;
        long lastTxId = 0;
        while (true) {
            LogRecord record = reader.readNext(true);
            if (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numTrans++;
                continue;
            }

            if (numTrans >= DEFAULT_SEGMENT_SIZE) {
                break;
            }
        }

    }

    @Test(timeout = 10000)
    public void nonBlockingReadLedgerRolling() throws Exception {
        String name = "distrlog-non-blocking-reader-ledger-rolling";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setOutputBufferSize(1024);
        confLocal.setReaderIdleWarnThresholdMillis(120000);
        confLocal.setReaderIdleErrorThresholdMillis(300000);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        long txId = 1;
        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long j = 0; j < 10; j++) {
            writer.write(DLMTestUtil.getLargeLogRecordInstance(txId++));
        }
        writer.closeAndComplete();

        BKContinuousLogReader reader = (BKContinuousLogReader)dlm.getInputStream(1);
        long numTrans = 0;
        long lastTxId = -1;

        while (true) {
            LogRecord record = reader.readNext(true);
            if (null != record) {
                DLMTestUtil.verifyLargeLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numTrans++;
                continue;
            }

            if (numTrans >= 10) {
                break;
            }
        }

        LOG.info("Starting new segment");
        Assert.assertTrue(null == reader.readNext(true));
        writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLargeLogRecordInstance(txId++));
        Assert.assertTrue(null == reader.readNext(true));
        LOG.info("Reading new segment");

        for (long j = 1; j < 10; j++) {
            writer.write(DLMTestUtil.getLargeLogRecordInstance(txId++));
        }
        writer.flushAndSync();

        numTrans = 0;
        while (true) {
            LogRecord record = reader.readNext(true);
            if (null != record) {
                DLMTestUtil.verifyLargeLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numTrans++;
                continue;
            }

            if (numTrans >= 10) {
                break;
            }
        }
    }

    @Test(timeout = 10000)
    public void nonBlockingReadFromAFutureTxId() throws Exception {
        String name = "distrlog-non-blocking-reader-future-txId";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setOutputBufferSize(1024);
        confLocal.setReaderIdleWarnThresholdMillis(120000);
        confLocal.setReaderIdleErrorThresholdMillis(300000);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        long txId = 1;
        long numTrans = 0;
        long lastTxId = 5;

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLargeLogRecordInstance(txId++));
        BKContinuousLogReader reader = (BKContinuousLogReader)dlm.getInputStream(6);
        Assert.assertTrue(null == reader.readNext(true));
        LOG.info("Reading new segment");

        for (long j = 1; j < 10; j++) {
            writer.write(DLMTestUtil.getLargeLogRecordInstance(txId++));
        }
        writer.flushAndSync();

        while (true) {
            LogRecord record = reader.readNext(true);
            if (null != record) {
                DLMTestUtil.verifyLargeLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numTrans++;
                continue;
            }

            if (numTrans >= 5) {
                break;
            }
        }
    }


    @Test(timeout = 10000)
    public void nonBlockingReadNoInProgressTimeout() throws Exception {
        String name = "distrlog-non-blocking-reader-noinprogress-timeout";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(500);
        confLocal.setReaderIdleErrorThresholdMillis(30000);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();

        long txId = writeRecordsForNonBlockingReads(dlm, false);

        BKContinuousLogReader reader = readNonBlocking(dlm, false, false, false, false);
        assert(!currentThread.isInterrupted());

        int numIterations = 30;
        long attempts = reader.getOpenReaderAttemptCount();
        for (int index = 0; index < numIterations; index++) {
            LogRecord record = reader.readNext(true);
            assert (null == record);
        }
        assert (reader.getOpenReaderAttemptCount() - attempts <= numIterations / 2);

        Thread.sleep(600);

        attempts = reader.getOpenReaderAttemptCount();
        LogRecord record = reader.readNext(true);
        assert (null == record);
        assert (reader.getOpenReaderAttemptCount() > attempts);
    }

    static class ReaderThread extends Thread {

        final LogReader reader;
        final boolean nonBlockReading;
        volatile boolean running = true;
        final AtomicInteger readCount = new AtomicInteger(0);

        ReaderThread(String name, LogReader reader, boolean nonBlockReading) {
            super(name);
            this.reader = reader;
            this.nonBlockReading = nonBlockReading;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    LogRecord r = reader.readNext(nonBlockReading);
                    if (r != null) {
                        readCount.incrementAndGet();
                        if (readCount.get() % 1000 == 0) {
                            LOG.info("{} reading {}", getName(), r.getTransactionId());
                        }
                    }
                } catch (DLInterruptedException die) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    break;
                }
            }
        }

        void stopReading() {
            LOG.info("Stopping reader {}.");
            running = false;
            interrupt();
            try {
                join();
            } catch (InterruptedException e) {
                LOG.error("Interrupted on waiting reader thread {} exiting : ", getName(), e);
            }
        }

        int getReadCount() {
            return readCount.get();
        }

    }

    @Test(timeout = 10000)
    public void nonBlockingReadWithForceRead() throws Exception {
        String name = "distrlog-non-blocking-reader-force-read";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        confLocal.setReaderIdleErrorThresholdMillis(500);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(1));
        writer.setReadyToFlush();
        writer.flushAndSync();

        BKContinuousLogReader reader = (BKContinuousLogReader)dlm.getInputStream(1);
        LogRecord record = reader.readNext(true);
        Assert.assertTrue(null != record);
        Thread.sleep(150);
        record = reader.readNext(true);
        Assert.assertTrue(null == record);
        Assert.assertTrue(reader.getForceBlockingRead());
        writer.write(DLMTestUtil.getLogRecordInstance(2));
        writer.setReadyToFlush();
        writer.flushAndSync();
        record = reader.readNext(true);
        Assert.assertTrue(null != record);
        Thread.sleep(425);
        boolean exceptionEncountered = false;
        try {
            record = reader.readNext(true);
            Assert.assertTrue(null == record);
        } catch (IdleReaderException exc) {
            LOG.info("Exception encountered", exc);
            exceptionEncountered = true;
        }
        Assert.assertTrue(exceptionEncountered);
    }

    @Test(timeout = 10000)
    public void nonBlockingReadWithForceReadDisable() throws Exception {
        String name = "distrlog-non-blocking-reader-force-read-disable";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        confLocal.setReaderIdleErrorThresholdMillis(500);
        confLocal.setEnableForceRead(false);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(1));
        writer.setReadyToFlush();
        writer.flushAndSync();

        BKContinuousLogReader reader = (BKContinuousLogReader)dlm.getInputStream(1);
        LogRecord record = reader.readNext(true);
        Assert.assertTrue(null != record);
        Thread.sleep(150);
        record = reader.readNext(true);
        Assert.assertTrue(null == record);
        Assert.assertTrue(!reader.getForceBlockingRead());
        writer.write(DLMTestUtil.getLogRecordInstance(2));
        writer.setReadyToFlush();
        writer.flushAndSync();
        record = reader.readNext(true);
        Assert.assertTrue(null != record);
        Thread.sleep(425);
        boolean exceptionEncountered = false;
        try {
            record = reader.readNext(true);
            Assert.assertTrue(null == record);
        } catch (IdleReaderException exc) {
            LOG.info("Exception encountered", exc);
            exceptionEncountered = true;
        }
        Assert.assertTrue(!exceptionEncountered);
    }

    @Test
    public void testMultiReaders() throws Exception {
        String name = "distrlog-multireaders";
        final RateLimiter limiter = RateLimiter.create(1000);
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);

        final LogWriter writer = dlmwrite.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(0));
        final AtomicInteger writeCount = new AtomicInteger(1);

        DistributedLogManager dlmread = DLMTestUtil.createNewDLM(conf, name);

        LogReader reader0 = dlmread.getInputStream(0);

        ReaderThread[] readerThreads = new ReaderThread[2];
        readerThreads[0] = new ReaderThread("reader0-non-blocking", reader0, false);
        readerThreads[1] = new ReaderThread("reader1-non-blocking", reader0, false);

        final AtomicBoolean running = new AtomicBoolean(true);
        Thread writerThread = new Thread("WriteThread") {
            @Override
            public void run() {
                try {
                    long txid = 1;
                    while (running.get()) {
                        limiter.acquire();
                            long curTxId = txid++;
                            writer.write(DLMTestUtil.getLogRecordInstance(curTxId));
                            writeCount.incrementAndGet();
                            if (curTxId % 1000 == 0) {
                                LOG.info("writer write {}", curTxId);
                            }
                    }
                    writer.setReadyToFlush();
                    writer.flushAndSync();
                } catch (DLInterruptedException die) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {

                }
            }
        };

        for (ReaderThread rt : readerThreads) {
            rt.start();
        }

        writerThread.start();

        TimeUnit.SECONDS.sleep(20);

        LOG.info("Stopping writer");

        running.set(false);
        writerThread.join();

        TimeUnit.SECONDS.sleep(10);

        assertEquals(writeCount.get(),
            (readerThreads[0].getReadCount() + readerThreads[1].getReadCount()));

        writer.close();
        dlmwrite.close();
        reader0.close();
        dlmread.close();
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
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, DLMTestUtil.createDLMURI("/" + name));
        DistributedLogManager[] dlms = new DistributedLogManager[count];
        for (int s = 0; s < count; s++) {
            if (shareBK) {
                dlms[s] = factory.createDistributedLogManagerWithSharedClients(name + String.format("%d", s));
            } else {
                dlms[s] = factory.createDistributedLogManagerWithSharedZK(name + String.format("%d", s));
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
                    dlms[0] = factory.createDistributedLogManagerWithSharedClients(name + String.format("%d", 0));
                } else {
                    dlms[0] = factory.createDistributedLogManagerWithSharedZK(name + String.format("%d", 0));
                }
            }

        }

        for (int s = 0; s < count; s++) {
            dlms[s].close();
        }

        factory.close();
    }

    @Test(timeout = 10000)
    public void testMissingZKNotificationOnLogSegmentCompletion() throws Exception {
        String name = "distrlog-missing-zk-notification-on-log-segment-completion";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReaderIdleWarnThresholdMillis(500);
        confLocal.setReaderIdleErrorThresholdMillis(Integer.MAX_VALUE);
        final DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(1));
        writer.setReadyToFlush();
        writer.flushAndSync();

        BKContinuousLogReader reader = (BKContinuousLogReader)dlm.getInputStream(1);
        LogRecord record = reader.readNext(true);
        Assert.assertTrue(null != record);
        Thread.sleep(150);
        record = reader.readNext(true);
        Assert.assertTrue(null == record);

        Assert.assertNotNull(reader.getCurrentReader());
        // disable per stream reader's zk notification
        reader.getCurrentReader().disableZKNotification();

        writer.closeAndComplete();
        writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();

        writer.write(DLMTestUtil.getLogRecordInstance(2));
        writer.setReadyToFlush();
        writer.flushAndSync();

        Thread.sleep(500 * 2);

        // first read still return null since forceBlockRead is set to true when idle reader is detected.
        record = reader.readNext(true);
        Assert.assertTrue(null == record);
        record = reader.readNext(true);
        Assert.assertTrue(null != record);

        writer.close();
        reader.close();
        dlm.close();
    }

    private long createStreamWithInconsistentMetadata(String name) throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        ZooKeeperClient zkClient = ZooKeeperClientBuilder.newBuilder().zkAclId(null).zkServers("127.0.0.1:7000").sessionTimeoutMs(10000).build();
        long txid = 1;

        long numRecordsWritten = 0;
        int segmentSize = 10;
        for (long i = 0; i < 3; i++) {
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
                numRecordsWritten++;
            }
            out.closeAndComplete();
        }

        BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
        String completedZNode = blplm.completedLedgerZNode(txid - segmentSize, txid - 1);
        LogSegmentLedgerMetadata metadataToChange = LogSegmentLedgerMetadata.read(zkClient, completedZNode);
        zkClient.get().delete(completedZNode, -1);
        metadataToChange.overwriteLastTxId(metadataToChange.getLastTxId() + 100);
        metadataToChange.write(zkClient, completedZNode);

        txid += 100;


        for (long i = 0; i < 3; i++) {
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
                numRecordsWritten++;
            }
            out.closeAndComplete();
        }
        dlm.close();

        return numRecordsWritten;
    }


    @Test
    public void testHandleInconsistentMetadata() throws Exception {
        String name = "distrlog-inconsistent-metadata-blocking-read";
        long numRecordsWritten = createStreamWithInconsistentMetadata(name);

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(1);
        long numRecordsRead = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            Assert.assertTrue(lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numRecordsRead++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals(numRecordsWritten, numRecordsRead);
    }

    @Test(timeout = 15000)
    public void testHandleInconsistentMetadataNonBlocking() throws Exception {
        String name = "distrlog-inconsistent-metadata-nonblocking-read";
        long numRecordsWritten = createStreamWithInconsistentMetadata(name);

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(1);
        long numRecordsRead = 0;
        long lastTxId = -1;
        while (numRecordsRead < numRecordsWritten) {
            LogRecord record = reader.readNext(false);
            if (record != null) {
                DLMTestUtil.verifyLogRecord(record);
                Assert.assertTrue(lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numRecordsRead++;
            } else {
                Thread.sleep(1);
            }
        }
        reader.close();
    }
}
