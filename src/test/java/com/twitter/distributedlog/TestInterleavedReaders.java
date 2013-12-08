package com.twitter.distributedlog;

import com.twitter.conversions.thread;
import com.twitter.distributedlog.exceptions.RetryableReadException;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestInterleavedReaders {
    static final Log LOG = LogFactory.getLog(TestBookKeeperDistributedLogManager.class);

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration().setLockTimeout(10);
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

    private int drainStreams(LogReader reader0, LogReader reader1) throws IOException {
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
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);


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
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);


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
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
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

        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);
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
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testInterleavedReadersWithRollingEdgeUnPartitioned() throws Exception {
        String name = "distrlog-interleaved-rolling-edge-unpartitioned";
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
    public void testSimpleAsyncWrite() throws Exception {
        String name = "distrlog-simpleasyncwrite";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        final Thread currentThread = Thread.currentThread();
        final CountDownLatch syncLatch = new CountDownLatch(30);
        final AtomicReference<DLSN> maxDLSN = new AtomicReference<DLSN>(DLSN.InvalidDLSN);
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                            LOG.debug("EntryId: " + value.getLedgerSequenceNo() + ", TxId " + currentLedgerSeqNo);
                            currentThread.interrupt();
                        }

                        if(value.getEntryId() != currentEntryId) {
                            LOG.debug("EntryId: " + value.getEntryId() + ", TxId " + record.getTransactionId() + "Expected " + currentEntryId);
                            currentThread.interrupt();
                        }

                        if (value.compareTo(maxDLSN.get()) > 0) {
                            maxDLSN.set(value);
                        }

                        syncLatch.countDown();
                        LOG.debug("SyncLatch: " + syncLatch.getCount());
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        currentThread.interrupt();
                    }
                });

                boolean success = false;
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);

        LogRecordWithDLSN last = dlm.getLastLogRecord();
        assertEquals(last.getDlsn(), maxDLSN.get());
        DLMTestUtil.verifyLargeLogRecord(last);

        dlm.close();
    }

    private static void readNext(final Thread threadToInterrupt,
                                 final CountDownLatch syncLatch,
                                 final AsyncLogReader reader,
                                 final DLSN startPosition) {
        Future<LogRecordWithDLSN> record = null;
        try {
            record = reader.readNext();
        } catch (Exception exc) {
            LOG.debug("Encountered Exception");
            threadToInterrupt.interrupt();
        }
        if (null != record) {
            record.addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                @Override
                public void onSuccess(LogRecordWithDLSN value) {
                    assert(value.getDlsn().compareTo(startPosition) >= 0);
                    try {
                        LOG.debug("DLSN: " + value.getDlsn());
                        assert(!value.isControl());
                        assert(value.getDlsn().getSlotId() == 0);
                        assert(value.getDlsn().compareTo(startPosition) >= 0);
                        DLMTestUtil.verifyLargeLogRecord(value);
                    } catch (Exception exc) {
                        LOG.debug("Exception Encountered when verifying log records" + value.getDlsn(), exc);
                        threadToInterrupt.interrupt();
                    }
                    syncLatch.countDown();
                    LOG.debug("SyncLatch: " + syncLatch.getCount());
                    TestInterleavedReaders.readNext(threadToInterrupt, syncLatch, reader, value.getDlsn().getNextDLSN());
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.debug("Encountered Exception");
                    threadToInterrupt.interrupt();
                }
            });
        }
    }

    private static void readNextWithRetry(final Thread threadToInterrupt,
                                 final CountDownLatch syncLatch,
                                 final DistributedLogManager dlm,
                                 final AsyncLogReader reader,
                                 final boolean simulateErrors,
                                 final DLSN startPosition,
                                 final ScheduledThreadPoolExecutor executorService,
                                 final int delay,
                                 final AtomicInteger executionCount) {
        Future<LogRecordWithDLSN> record = null;
        try {
            record = reader.readNext();
        } catch (Exception exc) {
            LOG.debug("Encountered Exception");
            threadToInterrupt.interrupt();
        }
        if (null != record) {
            record.addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                @Override
                public void onSuccess(LogRecordWithDLSN value) {
                    assert(value.getDlsn().compareTo(startPosition) >= 0);
                    try {
                        LOG.debug("DLSN: " + value.getDlsn());
                        assert(!value.isControl());
                        assert(value.getDlsn().getSlotId() == 0);
                        assert(value.getDlsn().compareTo(startPosition) >= 0);
                        DLMTestUtil.verifyLargeLogRecord(value);
                    } catch (Exception exc) {
                        LOG.debug("Exception Encountered when verifying log records" + value.getDlsn(), exc);
                        threadToInterrupt.interrupt();
                    }
                    syncLatch.countDown();
                    LOG.debug("SyncLatch: " + syncLatch.getCount());
                    if(0 == syncLatch.getCount()) {
                        try {
                            reader.close();
                        } catch (Exception exc) {
                            //
                        }
                    } else {
                        TestInterleavedReaders.readNextWithRetry(threadToInterrupt, syncLatch,
                                dlm, reader, simulateErrors,
                                value.getDlsn().getNextDLSN(), executorService, delay, executionCount);
                    }
                }
                @Override
                public void onFailure(Throwable cause) {
                    LOG.debug("Encountered Exception", cause);
                    try {
                        reader.close();
                    } catch (Exception exc) {
                        //
                    }
                    if (cause instanceof RetryableReadException) {
                        int newDelay = Math.min(delay * 2, 500);
                        if (0 == delay) {
                            newDelay = 10;
                        }
                        positionReader(threadToInterrupt, syncLatch, dlm, simulateErrors,
                            startPosition, executorService, newDelay, executionCount);
                    }
                }
            });
        }
    }

    private static void positionReader(final Thread threadToInterrupt,
                                          final CountDownLatch syncLatch,
                                          final DistributedLogManager dlm,
                                          final boolean simulateErrors,
                                          final DLSN startPosition,
                                          final ScheduledThreadPoolExecutor executorService,
                                          final int delay,
                                          final AtomicInteger executionCount) {
        executionCount.incrementAndGet();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncLogReader reader = dlm.getAsyncLogReader(startPosition);
                    if (simulateErrors) {
                        ((BKAsyncLogReaderDLSN)reader).simulateErrors();
                    }
                    readNextWithRetry(threadToInterrupt, syncLatch, dlm, reader, simulateErrors,
                        startPosition, executorService, delay, executionCount);
                } catch (IOException exc) {
                    int newDelay = Math.min(delay * 2, 500);
                    if (0 == delay) {
                        newDelay = 10;
                    }
                    positionReader(threadToInterrupt, syncLatch, dlm,
                        simulateErrors, startPosition, executorService, newDelay, executionCount);
                }
            }
        };
        executorService.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }


    @Test
    public void testSimpleAsyncRead() throws Exception {
        String name = "distrlog-simpleasyncread";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(1024);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= 10; j++) {
                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1), false));
            blplm.close();
        }

        long start = txid;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= 5; j++) {
            writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
            if (j % 2 == 0) {
                writer.setReadyToFlush();
                writer.flushAndSync();
            }
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        final CountDownLatch syncLatch = new CountDownLatch(txid - 1);
        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        final Thread currentThread = Thread.currentThread();

        TestInterleavedReaders.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

    @Test
    public void testAsyncReadEmptyRecords() throws Exception {
        String name = "distrlog-simpleasyncreadempty";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= 10; j++) {
                writer.write(DLMTestUtil.getEmptyLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1), false));
            blplm.close();
        }

        long start = txid;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= 5; j++) {
            writer.write(DLMTestUtil.getEmptyLogRecordInstance(txid++));
            if (j % 2 == 0) {
                writer.setReadyToFlush();
                writer.flushAndSync();
            }
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        AsyncLogReader asyncReader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        long numTrans = 0;
        DLSN lastDLSN = DLSN.InvalidDLSN;
        LogRecordWithDLSN record = asyncReader.readNext().get();
        while (null != record) {
            DLMTestUtil.verifyEmptyLogRecord(record);
            assert(record.getDlsn().getSlotId() == 0);
            assert (record.getDlsn().compareTo(lastDLSN) > 0);
            lastDLSN = record.getDlsn();
            numTrans++;
            if (numTrans >= (txid - 1)) {
                break;
            }
            record = asyncReader.readNext().get();
        }
        assertEquals((txid - 1), numTrans);
        asyncReader.close();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadPosition() throws Exception {
        String name = "distrlog-simpleasyncreadpos";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(1024);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)(dlm.startLogSegmentNonPartitioned());
            for (long j = 1; j <= 10; j++) {
                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }

        long start = txid;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= 5; j++) {
            writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        final CountDownLatch syncLatch = new CountDownLatch(txid - 14);
        final AsyncLogReader reader = dlm.getAsyncLogReader(new DLSN(2, 2, 4));
        final Thread currentThread = Thread.currentThread();

        TestInterleavedReaders.readNext(currentThread, syncLatch, reader, new DLSN(2, 3, 0));

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadWrite() throws Exception {
        String name = "distrlog-simpleasyncreadwrite";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(30);
        final AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        final Thread currentThread = Thread.currentThread();

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                            LOG.debug("EntryId: " + value.getLedgerSequenceNo() + ", TxId " + currentLedgerSeqNo);
                            currentThread.interrupt();
                        }

                        if(value.getEntryId() != currentEntryId) {
                            LOG.debug("EntryId: " + value.getEntryId() + ", TxId " + record.getTransactionId() + "Expected " + currentEntryId);
                            currentThread.interrupt();
                        }
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        currentThread.interrupt();
                    }
                });
                if (i == 0 && j == 0) {
                    TestInterleavedReaders.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);
                }
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadWriteStartEmpty() throws Exception {
        String name = "distrlog-simpleasyncreadwritestartempty";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(30);
        final Thread currentThread = Thread.currentThread();
        final AtomicInteger executionCount = new AtomicInteger(0);

        positionReader(currentThread, syncLatch, dlm, false, DLSN.InvalidDLSN, new ScheduledThreadPoolExecutor(1), 0, executionCount);

        // Increase the probability of reader failure and retry
        Thread.sleep(500);
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                            LOG.debug("EntryId: " + value.getLedgerSequenceNo() + ", TxId " + currentLedgerSeqNo);
                            currentThread.interrupt();
                        }

                        if(value.getEntryId() != currentEntryId) {
                            LOG.debug("EntryId: " + value.getEntryId() + ", TxId " + record.getTransactionId() + "Expected " + currentEntryId);
                            currentThread.interrupt();
                        }
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        currentThread.interrupt();
                    }
                });
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        assertEquals(true, executionCount.get() > 1);
        dlm.close();
    }

    @Test
    public void testSimpleAsyncReadWriteSimulateErrors() throws Exception {
        String name = "distrlog-simpleasyncreadwritesimulateerrors";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        final CountDownLatch syncLatch = new CountDownLatch(200);
        final Thread currentThread = Thread.currentThread();
        final AtomicInteger executionCount = new AtomicInteger(0);

        positionReader(currentThread, syncLatch, dlm, true, DLSN.InvalidDLSN, new ScheduledThreadPoolExecutor(1), 0, executionCount);

        int txid = 1;
        for (long i = 0; i < 20; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                            LOG.debug("EntryId: " + value.getLedgerSequenceNo() + ", TxId " + currentLedgerSeqNo);
                            currentThread.interrupt();
                        }

                        if(value.getEntryId() != currentEntryId) {
                            LOG.debug("EntryId: " + value.getEntryId() + ", TxId " + record.getTransactionId() + "Expected " + currentEntryId);
                            currentThread.interrupt();
                        }
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        currentThread.interrupt();
                    }
                });
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        assertEquals(true, executionCount.get() > 1);
        dlm.close();
    }
}
