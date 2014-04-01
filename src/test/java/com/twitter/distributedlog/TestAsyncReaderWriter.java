package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.RetryableReadException;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestAsyncReaderWriter {
    static final Logger LOG = LoggerFactory.getLogger(TestAsyncReaderWriter.class);

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
        assertEquals(last.getDlsn(), dlm.getLastDLSN());
        assertEquals(last.getDlsn(), dlm.getLastDLSNAsync().get());
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
                    TestAsyncReaderWriter.readNext(threadToInterrupt, syncLatch, reader, value.getDlsn().getNextDLSN());
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
                                          final AtomicInteger readCount,
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
                    synchronized (syncLatch) {
                        syncLatch.countDown();
                        readCount.incrementAndGet();
                    }
                    LOG.debug("SyncLatch: " + syncLatch.getCount());
                    if(0 == syncLatch.getCount()) {
                        try {
                            reader.close();
                        } catch (Exception exc) {
                            //
                        }
                    } else {
                        TestAsyncReaderWriter.readNextWithRetry(threadToInterrupt, syncLatch,
                            dlm, reader, simulateErrors,
                            value.getDlsn().getNextDLSN(), executorService, delay, readCount, executionCount);
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
                    if (cause instanceof IOException) {
                        int newDelay = Math.min(delay * 2, 500);
                        if (0 == delay) {
                            newDelay = 10;
                        }
                        positionReader(threadToInterrupt, syncLatch, dlm, simulateErrors,
                            startPosition, executorService, newDelay, readCount, executionCount);
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
                                       final AtomicInteger readCount,
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
                        startPosition, executorService, delay, readCount, executionCount);
                } catch (IOException exc) {
                    int newDelay = Math.min(delay * 2, 500);
                    if (0 == delay) {
                        newDelay = 10;
                    }
                    positionReader(threadToInterrupt, syncLatch, dlm,
                        simulateErrors, startPosition, executorService,
                        newDelay, readCount, executionCount);
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
            BKPerStreamLogWriter perStreamLogWriter = writer.getCachedLogWriter(conf.getUnpartitionedStreamName());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(perStreamLogWriter.getLedgerHandle().getId(),
                start, txid - 1, perStreamLogWriter.getLedgerSequenceNumber()), false));
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

        TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);

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
            BKPerStreamLogWriter perStreamLogWriter = writer.getCachedLogWriter(conf.getUnpartitionedStreamName());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(perStreamLogWriter.getLedgerHandle().getId(),
                start, txid - 1, perStreamLogWriter.getLedgerSequenceNumber()), false));
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

        TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, new DLSN(2, 3, 0));

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
        testSimpleAsyncReadWriteInternal("distrlog-simpleasyncreadwrite", false);
    }

    @Test
    public void testSimpleAsyncReadWriteImmediateFlush() throws Exception {
        testSimpleAsyncReadWriteInternal("distrlog-simpleasyncreadwrite-imm-flush", true);
    }

    class WriteFutureEventListener implements FutureEventListener<DLSN> {
        private final LogRecord record;
        private final long currentLedgerSeqNo;
        private final long currentEntryId;
        private final Thread currentThread;

        WriteFutureEventListener(LogRecord record, long currentLedgerSeqNo, long currentEntryId, Thread currentThread) {
            this.record = record;
            this.currentLedgerSeqNo = currentLedgerSeqNo;
            this.currentEntryId = currentEntryId;
            this.currentThread = currentThread;
        }

        /**
         * Invoked if the computation completes successfully
         */
        @Override
        public void onSuccess(DLSN value) {
            if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                LOG.debug("Thread Interrupted - Ledger Seq No: " + value.getLedgerSequenceNo() + ", Expected: " + currentLedgerSeqNo);
                currentThread.interrupt();
            }

            if(value.getEntryId() != currentEntryId) {
                LOG.debug("Thread Interrupted - EntryId: " + value.getEntryId() + ", TxId " + record.getTransactionId() + "Expected " + currentEntryId);
                currentThread.interrupt();
            }
        }

        /**
         * Invoked if the computation completes unsuccessfully
         */
        @Override
        public void onFailure(Throwable cause) {
            LOG.debug("Thread Interrupted - onFailure", cause);
            currentThread.interrupt();
        }
    }


    public void testSimpleAsyncReadWriteInternal(String name, boolean immediateFlush) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        confLocal.setImmediateFlushEnabled(immediateFlush);
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
                dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
                if (i == 0 && j == 0) {
                    TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);
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
        final AtomicInteger readCount = new AtomicInteger(0);

        positionReader(currentThread, syncLatch, dlm, false, DLSN.InvalidDLSN, new ScheduledThreadPoolExecutor(1), 0, readCount, executionCount);

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
                dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
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
    public void testSimpleAsyncReadWriteStartEmptyFactory() throws Exception {
        int count = 50;
        String name = "distrlog-simpleasyncreadwritestartemptyfactory";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadWaitTime(10);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setOutputBufferSize(1024);
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(confLocal, DLMTestUtil.createDLMURI("/" + name));
        DistributedLogManager[] dlms = new DistributedLogManager[count];
        final AtomicInteger[] readCounts = new AtomicInteger[count];
        final AtomicInteger[] executionCounts = new AtomicInteger[count];
        final CountDownLatch syncLatch = new CountDownLatch(3 * count);
        final Thread currentThread = Thread.currentThread();
        for (int s = 0; s < count; s++) {
            executionCounts[s] = new AtomicInteger(0);
            readCounts[s] = new AtomicInteger(0);
            dlms[s] = factory.createDistributedLogManagerWithSharedClients(name + String.format("%d", s));
            positionReader(currentThread, syncLatch, dlms[s], false,
                DLSN.InvalidDLSN, new ScheduledThreadPoolExecutor(1), 0, readCounts[s], executionCounts[s]);
        }


        // Increase the probability of reader failure and retry
        Thread.sleep(5000);
        int txid = 1;
        for (long i = 0; i < 3; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter[] writers = new BKUnPartitionedAsyncLogWriter[count];
            for (int s = 0; s < count; s++) {
                writers[s] = (BKUnPartitionedAsyncLogWriter)(dlms[s].startAsyncLogSegmentNonPartitioned());
            }
            for (long j = 0; j < 1; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                for (int s = 0; s < count; s++) {
                    Future<DLSN> dlsnFuture = writers[s].write(record);
                    dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
                }
            }
            for (int s = 0; s < count; s++) {
                writers[s].closeAndComplete();
            }
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(90, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        if (!success) {
            for (int s = 0; s < count; s++) {
                assertEquals(String.format("%d Stream", s), 3, readCounts[s].get());
            }
        }
        for (int s = 0; s < count; s++) {
            assertEquals(true, executionCounts[s].get() > 1);
            dlms[s].close();
        }
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
        final AtomicInteger readCount = new AtomicInteger(0);

        positionReader(currentThread, syncLatch, dlm, true, DLSN.InvalidDLSN,
            new ScheduledThreadPoolExecutor(1), 0, readCount, executionCount);

        int txid = 1;
        for (long i = 0; i < 20; i++) {
            final long currentLedgerSeqNo = i + 1;
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
            for (long j = 0; j < 10; j++) {
                final long currentEntryId = j;
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new WriteFutureEventListener(record, currentLedgerSeqNo, currentEntryId, currentThread));
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

    @Test
    public void testSimpleAsyncReadWritePolling() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritepolling", 0);
    }

    @Test
    public void testSimpleAsyncReadWriteLongPoll() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritelongpoll", 1);
    }

    @Test
    public void testSimpleAsyncReadWritePiggyBack() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritepiggyback", 2);
    }

    @Test
    public void testSimpleAsyncReadWritePiggyBackSpec() throws Exception {
        testSimpleAsyncReadWriteLACOptions("distrlog-simpleasyncreadwritepiggybackspec", 3);
    }

    private void testSimpleAsyncReadWriteLACOptions(String name, int lacOption) throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setEnableReadAhead(true);
        confLocal.setReadAheadWaitTime(500);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setReadAheadMaxEntries(100);
        confLocal.setOutputBufferSize(1024);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(100);
        confLocal.setReadLACOption(lacOption);
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
                Thread.sleep(250);
                final LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsnFuture = writer.write(record);
                dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        if(value.getLedgerSequenceNo() != currentLedgerSeqNo) {
                            LOG.debug("Thread Interrupted - EntryId: " + value.getLedgerSequenceNo() + ", TxId " + currentLedgerSeqNo);
                            currentThread.interrupt();
                        }
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        LOG.debug("Thread Interrupted - onFailure", cause);
                        currentThread.interrupt();
                    }
                });
                if (i == 0 && j == 0) {
                    TestAsyncReaderWriter.readNext(currentThread, syncLatch, reader, DLSN.InvalidDLSN);
                }
            }
            writer.closeAndComplete();
        }

        boolean success = false;
        if (!(Thread.interrupted())) {
            try {
                success = syncLatch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                LOG.debug("Thread Interrupted - onFailure", exc);
                Thread.currentThread().interrupt();
            }
        }

        assert(!(Thread.interrupted()));
        assert(success);
        reader.close();
        dlm.close();
    }

}
