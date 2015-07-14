package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.RateLimiter;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.IdleReaderException;

import static org.junit.Assert.*;

public class TestNonBlockingReads extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestNonBlockingReads.class);

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    private void readNonBlocking(DistributedLogManager dlm, boolean forceBlockingRead, boolean forceStall) throws Exception {
        readNonBlocking(dlm, forceBlockingRead, forceStall, DEFAULT_SEGMENT_SIZE);
    }

    private void readNonBlocking(DistributedLogManager dlm, boolean forceBlockingRead, boolean forceStall, long segmentSize) throws Exception {
        BKContinuousLogReaderTxId reader = (BKContinuousLogReaderTxId)dlm.getInputStream(1);
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

                LogRecordWithDLSN record = reader.readNext(true);
                if (null != record) {
                    DLMTestUtil.verifyLogRecord(record);
                    assertTrue(lastTxId < record.getTransactionId());
                    assertEquals(record.getTransactionId() - 1, record.getSequenceId());
                    lastTxId = record.getTransactionId();
                    numTrans++;
                    continue;
                }

                if (numTrans >= (3 * segmentSize)) {
                    break;
                }

                TimeUnit.MILLISECONDS.sleep(2);
            }
        } catch (LogReadException readexc) {
            exceptionEncountered = true;
        } catch (LogNotFoundException exc) {
            exceptionEncountered = true;
        }
        assert(!exceptionEncountered);
        reader.close();
    }

    void writeRecordsForNonBlockingReads(DistributedLogManager dlm, boolean recover) throws Exception {
        writeRecordsForNonBlockingReads(dlm, recover, DEFAULT_SEGMENT_SIZE);
    }

    void writeRecordsForNonBlockingReads(DistributedLogManager dlm, boolean recover, long segmentSize) throws Exception {
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
            TimeUnit.MILLISECONDS.sleep(300);
        }
    }


    @Test(timeout = 60000)
    public void testNonBlockingRead() throws Exception {
        String name = "distrlog-non-blocking-reader";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
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

        readNonBlocking(dlm, false, false);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 60000)
    public void testNonBlockingReadWithForceBlockingReads() throws Exception {
        String name = "distrlog-non-blocking-reader-force-blocking";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
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

        readNonBlocking(dlm, true, false);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 60000)
    public void testNonBlockingReadRecovery() throws Exception {
        String name = "distrlog-non-blocking-reader-recovery";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(16);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setReadAheadMaxEntries(10);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
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


        readNonBlocking(dlm, false, false);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 60000)
    public void testNonBlockingReadIdleError() throws Exception {
        String name = "distrlog-non-blocking-reader-error";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setReaderIdleWarnThresholdMillis(50);
        confLocal.setReaderIdleErrorThresholdMillis(100);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
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
            readNonBlocking(dlm, false, false);
        } catch (IdleReaderException exc) {
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }

    @Test(timeout = 60000)
    public void testNonBlockingReadAheadStall() throws Exception {
        String name = "distrlog-non-blocking-reader-stall";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(3);
        confLocal.setReaderIdleWarnThresholdMillis(500);
        confLocal.setReaderIdleErrorThresholdMillis(30000);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
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
            readNonBlocking(dlm, false, true, 3);
        } catch (IdleReaderException exc) {
            LOG.info("Exception encountered", exc);
            exceptionEncountered = true;
        }
        assert(!exceptionEncountered);
        assert(!currentThread.isInterrupted());
        executor.shutdown();
    }


    @Test(timeout = 60000)
    public void testNonBlockingReadWithForceRead() throws Exception {
        String name = "distrlog-non-blocking-reader-force-read";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        confLocal.setReaderIdleErrorThresholdMillis(500);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(1));
        writer.setReadyToFlush();
        writer.flushAndSync();

        BKContinuousLogReaderTxId reader = (BKContinuousLogReaderTxId)dlm.getInputStream(1);
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

    @Test(timeout = 60000)
    public void testNonBlockingReadWithForceReadDisable() throws Exception {
        String name = "distrlog-non-blocking-reader-force-read-disable";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        confLocal.setReaderIdleErrorThresholdMillis(500);
        confLocal.setEnableForceRead(false);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(1));
        writer.setReadyToFlush();
        writer.flushAndSync();

        BKContinuousLogReaderTxId reader = (BKContinuousLogReaderTxId)dlm.getInputStream(1);
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

    @Test
    public void testMultiReaders() throws Exception {
        String name = "distrlog-multireaders";
        final RateLimiter limiter = RateLimiter.create(1000);
        DistributedLogManager dlmwrite = createNewDLM(conf, name);

        final LogWriter writer = dlmwrite.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(0));
        final AtomicInteger writeCount = new AtomicInteger(1);

        DistributedLogManager dlmread = createNewDLM(conf, name);

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

    private long createStreamWithInconsistentMetadata(String name) throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, name);
        ZooKeeperClient zkClient = ZooKeeperClientBuilder.newBuilder()
                .zkAclId(null)
                .uri(createDLMURI("/"))
                .sessionTimeoutMs(10000)
                .build();
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
        String completedZNode = blplm.completedLedgerZNode(txid - segmentSize, txid - 1, 3);
        LogSegmentLedgerMetadata metadata = LogSegmentLedgerMetadata.read(zkClient, completedZNode);
        zkClient.get().delete(completedZNode, -1);
        LogSegmentLedgerMetadata metadataToChange =
                metadata.mutator()
                        .setLastEntryId(metadata.getLastEntryId() + 100)
                        .setLastTxId(metadata.getLastTxId() + 100)
                        .build();
        metadataToChange.write(zkClient);

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

        DistributedLogManager dlm = createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(45);
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
        assertEquals(numRecordsWritten / 2, numRecordsRead);
    }

    @Test(timeout = 15000)
    public void testHandleInconsistentMetadataNonBlocking() throws Exception {
        String name = "distrlog-inconsistent-metadata-nonblocking-read";
        long numRecordsWritten = createStreamWithInconsistentMetadata(name);

        DistributedLogManager dlm = createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(45);
        long numRecordsRead = 0;
        long lastTxId = -1;
        while (numRecordsRead < (numRecordsWritten / 2)) {
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

    @Test(timeout = 15000)
    public void testHandleInconsistentMetadataDLSNNonBlocking() throws Exception {
        String name = "distrlog-inconsistent-metadata-nonblocking-read-dlsn";
        long numRecordsWritten = createStreamWithInconsistentMetadata(name);

        DistributedLogManager dlm = createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
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