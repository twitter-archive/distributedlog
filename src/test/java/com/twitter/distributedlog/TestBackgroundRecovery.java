package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.util.DistributedLogAnnotations.FlakyTest;
import com.twitter.util.Await;

import static org.junit.Assert.*;

public class TestBackgroundRecovery extends TestDistributedLogBase {

    private static Logger logger = LoggerFactory.getLogger(TestBackgroundRecovery.class);

    private static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10).setRecoverLogSegmentsInBackground(true)
            .setOutputBufferSize(0).setPeriodicFlushFrequencyMilliSeconds(20);

    static class NopExecutorService implements ExecutorService {

        @Override
        public void shutdown() {
            // nop
        }
        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }
        @Override
        public boolean isShutdown() {
            return true;
        }
        @Override
        public boolean isTerminated() {
            return true;
        }
        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }
        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return null;
        }
        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return null;
        }
        @Override
        public Future<?> submit(Runnable task) {
            return null;
        }
        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return null;
        }
        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return null;
        }
        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return null;
        }
        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
        @Override
        public void execute(Runnable command) {
            // nop
        }
    }

    private void recoveryTest(boolean background, int nameVersion) throws Exception {
        String name = "recover-test-v" + nameVersion + "-" + background;

        DistributedLogConfiguration testConf = new DistributedLogConfiguration();
        testConf.addConfiguration(conf);
        testConf.setRecoverLogSegmentsInBackground(background);
        testConf.setLogSegmentNameVersion(nameVersion);

        int numSegments = 3;

        long txid = 1L;
        for (int i = 0; i < numSegments; i++) {
            logger.info("Creating segment {}.", i);
            BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(testConf, name);
            dlm.setMetadataExecutor(new NopExecutorService());
            BKUnPartitionedAsyncLogWriter out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
            logger.info("Adding txn id {} to log segment {}.", txid, i);
            Await.result(out.write(DLMTestUtil.getLogRecordInstance(txid)));
            out.closeNoThrow();
            dlm.close();
            logger.info("Created segment {}.", i);
        }

        BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(testConf, name);
        BKUnPartitionedAsyncLogWriter out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        Await.result(out.write(DLMTestUtil.getLogRecordInstance(txid)));
        out.closeAndComplete();
        dlm.close();

        assertEquals(numSegments + 1, DLMTestUtil.getNumberofLogRecords(DLMTestUtil.createNewDLM(testConf, name), 0L));
    }

    private void backwardTest(int writeVersion, int readVersion, int numSegments, int numExpectedSegments,
                              int numExpectedCompletedSegments, int numExpectedInprocessSegments) throws Exception {
        String name = "backward-test-wv-" + writeVersion + "-rv-" + readVersion;

        DistributedLogConfiguration writeConf = new DistributedLogConfiguration();
        writeConf.addConfiguration(conf);
        writeConf.setRecoverLogSegmentsInBackground(true);
        writeConf.setLogSegmentNameVersion(writeVersion);

        long txid = 1L;
        for (int i = 0; i < numSegments; i++) {
            logger.info("Creating log segment {}.", i);
            BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(writeConf, name);
            dlm.setMetadataExecutor(new NopExecutorService());
            BKUnPartitionedAsyncLogWriter out = dlm.startAsyncLogSegmentNonPartitioned();
            long txnidToWrite = txid++;
            logger.info("Adding txn id {} to log segment {}.", txnidToWrite, i);
            Await.result(out.write(DLMTestUtil.getLogRecordInstance(txnidToWrite)));
            out.closeNoThrow();
            dlm.close();
            logger.info("Created log segment {}.", i);
        }

        DistributedLogConfiguration readConf = new DistributedLogConfiguration();
        readConf.addConfiguration(conf);
        readConf.setRecoverLogSegmentsInBackground(false);
        readConf.setLogSegmentNameVersion(readVersion);

        BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(readConf, name);
        BKUnPartitionedAsyncLogWriter out = dlm.startAsyncLogSegmentNonPartitioned();
        Await.result(out.write(DLMTestUtil.getLogRecordInstance(txid)));
        out.closeAndComplete();
        dlm.close();

        BKDistributedLogManager readDLM = (BKDistributedLogManager) DLMTestUtil.createNewDLM(readConf, name);
        BKLogPartitionReadHandler readHandler = readDLM.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        List<LogSegmentLedgerMetadata> segments = readHandler.getFullLedgerList(true, false);
        assertEquals(numExpectedSegments, segments.size());
        int numCompleted = 0;
        int numInprogress = 0;
        for (LogSegmentLedgerMetadata l : segments) {
            if (l.isInProgress()) {
                ++numInprogress;
            } else {
                ++numCompleted;
            }
        }
        assertEquals(numExpectedCompletedSegments, numCompleted);
        assertEquals(numExpectedInprocessSegments, numInprogress);

        assertEquals(numSegments + 1, DLMTestUtil.getNumberofLogRecords(DLMTestUtil.createNewDLM(readConf, name), 0L));

        readHandler.close();
        readDLM.close();
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testRecoverInForeground() throws Exception {
        recoveryTest(false, DistributedLogConstants.LOGSEGMENT_NAME_VERSION);
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testRecoverInForegroundWithOldNaming() throws Exception {
        try {
            recoveryTest(false, 0);
            fail("Should fail if use old naming mechanism.");
        } catch (IOException ioe) {
            if (!ioe.getMessage().contains("already exists but data doesn't match")) {
                fail("Should fail with 'already exists but data doesn't match'.");
            }
        }
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testRecoverInBackground() throws Exception {
        recoveryTest(true, DistributedLogConstants.LOGSEGMENT_NAME_VERSION);
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testWriteOldNameRecoverNewName() throws Exception {
        backwardTest(0, DistributedLogConstants.LOGSEGMENT_NAME_VERSION, 3, 4, 4, 0);
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testWriteNewNameRecoverOldName() throws Exception {
        backwardTest(DistributedLogConstants.LOGSEGMENT_NAME_VERSION, 0, 3, 4, 4, 0);
    }

    @Test(timeout = 60000)
    public void testPositionReaderByTxID() throws Exception {
        String name = "test-position-reader-by-txid";

        DistributedLogConfiguration localConf = new DistributedLogConfiguration();
        localConf.addConfiguration(conf);
        localConf.setRecoverLogSegmentsInBackground(true);

        int numSegments = 5;

        long txid = 1L;
        for (int i = 0; i < numSegments; i++) {
            logger.info("Creating log segment {}.", i);
            BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(localConf, name);
            dlm.setMetadataExecutor(new NopExecutorService());
            BKUnPartitionedAsyncLogWriter out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
            long txnidToWrite = txid++;
            logger.info("Adding txn id {} to log segment {}.", txnidToWrite, i);
            Await.result(out.write(DLMTestUtil.getLogRecordInstance(txnidToWrite)));
            out.closeNoThrow();
            dlm.close();
            logger.info("Created log segment {}.", i);
        }

        BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(localConf, name);
        BKLogPartitionReadHandler readHandler = dlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        for (long i = 1L; i <= numSegments; i++) {
            ResumableBKPerStreamLogReader perStreamLogReader = readHandler.getInputStream(i, false, false);
            assertNotNull(perStreamLogReader);
            assertEquals(1L, perStreamLogReader.getLogSegmentLedgerMetadata().getLedgerSequenceNumber());
        }
        readHandler.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testPositionReaderByDLSN() throws Exception {
        String name = "test-position-reader-by-dlsn";

        DistributedLogConfiguration localConf = new DistributedLogConfiguration();
        localConf.addConfiguration(conf);
        localConf.setRecoverLogSegmentsInBackground(true);

        int numSegments = 5;

        long txid = 1L;
        for (int i = 0; i < numSegments; i++) {
            logger.info("Creating log segment {}.", i);
            BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(localConf, name);
            dlm.setMetadataExecutor(new NopExecutorService());
            BKUnPartitionedAsyncLogWriter out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
            long txnidToWrite = txid++;
            logger.info("Adding txn id {} to log segment {}.", txnidToWrite, i);
            Await.result(out.write(DLMTestUtil.getLogRecordInstance(txnidToWrite)));
            out.closeNoThrow();
            dlm.close();
            logger.info("Created log segment {}.", i);
        }

        BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(localConf, name);
        BKLogPartitionReadHandler readHandler = dlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
        for (long i = 1L; i <= numSegments; i++) {
            DLSN dlsn = new DLSN(i, 0, 0);
            ResumableBKPerStreamLogReader perStreamLogReader = readHandler.getInputStream(dlsn, false, false);
            assertNotNull(perStreamLogReader);
            assertEquals(i, perStreamLogReader.getLogSegmentLedgerMetadata().getLedgerSequenceNumber());
        }
        readHandler.close();
        dlm.close();
    }

    private void writeReadWithMultipleInprogressSegments(String testName, final boolean byTxID, final long fromTxID, final DLSN fromDLSN) throws Exception {
        final String name = testName + "-writeread-" + byTxID + "-" + fromTxID + "-" + System.currentTimeMillis();

        final DistributedLogConfiguration localConf = new DistributedLogConfiguration();
        localConf.addConfiguration(conf);
        localConf.setRecoverLogSegmentsInBackground(true);

        final int numSegments = 5;
        final int numEntriesPerSegment = 10;
        long txid = 1L;

        int numEntriesToRead;
        if (fromTxID > numEntriesPerSegment) {
            if (byTxID) {
                numEntriesToRead = (numSegments - 1) * numEntriesPerSegment + 1;
            } else {
                numEntriesToRead = (numSegments - ((int)fromDLSN.getLedgerSequenceNo()) + 1) * numEntriesPerSegment;
            }
        } else {
            numEntriesToRead = numSegments * numEntriesPerSegment;
        }

        final AtomicInteger numPendingReads = new AtomicInteger(numEntriesToRead);
        final CountDownLatch readDone = new CountDownLatch(1);

        logger.info("Creating first log segment.");
        BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(localConf, name);
        dlm.setMetadataExecutor(new NopExecutorService());
        BKUnPartitionedAsyncLogWriter out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        for (int j = 0; j < numEntriesPerSegment; j++) {
            long txnidToWrite = txid++;
            logger.info("Adding txn id {} to first log segment.", txnidToWrite);
            Await.result(out.write(DLMTestUtil.getLogRecordInstance(txnidToWrite)));
        }
        out.closeNoThrow();
        dlm.close();
        logger.info("Created first log segment.");

        Thread readThread = new Thread() {

            final DistributedLogManager dlm = DLMTestUtil.createNewDLM(localConf, name);
            final LogReader reader = byTxID ? dlm.getInputStream(fromTxID) : dlm.getInputStream(fromDLSN);

            @Override
            public void run() {
                long txid;
                if (fromTxID > numEntriesPerSegment) {
                    if (byTxID) {
                        txid = numEntriesPerSegment - 1;
                    } else {
                        txid = fromTxID - 1;
                    }
                } else {
                    txid = fromTxID - 1;
                }
                while (true) {
                    LogRecord record;
                    try {
                        record = reader.readNext(false);
                    } catch (IOException e) {
                        logger.error("Failed on reading {} : ", name, e);
                        readDone.countDown();
                        break;
                    }
                    if (null != record) {
                        ++txid;
                        if (txid != record.getTransactionId()) {
                            logger.error("Expected txn id {}, but received {}.", txid, record.getTransactionId());
                            readDone.countDown();
                            break;
                        }
                        if (0 == numPendingReads.decrementAndGet()) {
                            readDone.countDown();
                            break;
                        }
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            // ignored
                        }
                    }
                }
                try {
                    reader.close();
                    dlm.close();
                } catch (IOException e) {
                    // ignored
                }
            }
        };

        readThread.start();
        logger.info("Started reading thread.");

        for (int i = 1; i <= numSegments - 1; i++) {
            logger.info("Creating log segment {}.", i);
            dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(localConf, name);
            dlm.setMetadataExecutor(new NopExecutorService());
            out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
            for (int j = 0; j < numEntriesPerSegment; j++) {
                long txnidToWrite = txid++;
                logger.info("Adding txn id {} to log segment {}.", txnidToWrite, i);
                Await.result(out.write(DLMTestUtil.getLogRecordInstance(txnidToWrite)));
            }
            out.closeNoThrow();
            dlm.close();
            logger.info("Created log segment {}.", i);
        }

        readDone.await(2, TimeUnit.SECONDS);

        // should hang on reading first log segment until it is completed
        if (byTxID) {
            assertEquals((numSegments - 1) * numEntriesPerSegment, numPendingReads.get());
        }

        DistributedLogConfiguration recoverConf = new DistributedLogConfiguration();
        recoverConf.addConfiguration(conf);
        recoverConf.setRecoverLogSegmentsInBackground(false);

        logger.info("Recovering all inprogress log segments for {}.", name);

        dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(recoverConf, name);
        out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        out.closeAndComplete();
        dlm.close();

        logger.info("Recovered all inprogress log segments for {}.", name);

        readDone.await();
        assertEquals(0, numPendingReads.get());

        readThread.join();
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testWriteReadWithMultipleInprogressSegmentsByTxID1() throws Exception {
        writeReadWithMultipleInprogressSegments("test-by-txid-1", true, 1L, DLSN.InvalidDLSN);
    }

    @Test(timeout = 60000)
    public void testWriteReadWithMultipleInprogressSegmentsByTxID2() throws Exception {
        writeReadWithMultipleInprogressSegments("test-by-txid-2", true, 13L, DLSN.InvalidDLSN);
    }

    @Test(timeout = 60000)
    public void testWriteReadWithMultipleInprogressSegmentsByTxID3() throws Exception {
        writeReadWithMultipleInprogressSegments("test-by-txid-3", true, 44L, DLSN.InvalidDLSN);
    }

    @FlakyTest
    @Test(timeout = 60000)
    public void testWriteReadWithMultipleInprogressSegmentsByDLSN1() throws Exception {
        writeReadWithMultipleInprogressSegments("test-by-dlsn-1", false, 1L, new DLSN(1L, 0L, 0L));
    }

    @Test(timeout = 60000)
    public void testWriteReadWithMultipleInprogressSegmentsByDLSN2() throws Exception {
        writeReadWithMultipleInprogressSegments("test-by-dlsn-2", false, 11L, new DLSN(2L, 0L, 0L));
    }

    @Test(timeout = 60000)
    public void testWriteReadWithMultipleInprogressSegmentsByDLSN3() throws Exception {
        writeReadWithMultipleInprogressSegments("test-by-dlsn-3", false, 31L, new DLSN(4L, 0L, 0L));
    }

}
