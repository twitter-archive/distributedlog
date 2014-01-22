package com.twitter.distributedlog;

import com.twitter.util.Await;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class TestBackgroundRecovery {

    private static Logger logger = LoggerFactory.getLogger(TestBackgroundRecovery.class);

    private static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10).setRecoverLogSegmentsInBackground(true)
            .setOutputBufferSize(0).setPeriodicFlushFrequencyMilliSeconds(20);
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    private static int numBookies = 3;

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
            out.close();
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
            BKUnPartitionedAsyncLogWriter out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
            long txnidToWrite = txid++;
            logger.info("Adding txn id {} to log segment {}.", txnidToWrite, i);
            Await.result(out.write(DLMTestUtil.getLogRecordInstance(txnidToWrite)));
            out.close();
            dlm.close();
            logger.info("Created log segment {].", i);
        }

        DistributedLogConfiguration readConf = new DistributedLogConfiguration();
        readConf.addConfiguration(conf);
        readConf.setRecoverLogSegmentsInBackground(false);
        readConf.setLogSegmentNameVersion(readVersion);

        BKDistributedLogManager dlm = (BKDistributedLogManager) DLMTestUtil.createNewDLM(readConf, name);
        BKUnPartitionedAsyncLogWriter out = (BKUnPartitionedAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        Await.result(out.write(DLMTestUtil.getLogRecordInstance(txid)));
        out.closeAndComplete();
        dlm.close();

        BKLogPartitionReadHandler readHandler = dlm.createReadLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
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
    }

    @Test(timeout = 60000)
    public void testRecoverInForeground() throws Exception {
        recoveryTest(false, DistributedLogConstants.LOGSEGMENT_NAME_VERSION);
    }

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

    @Test
    public void testRecoverInBackground() throws Exception {
        recoveryTest(true, DistributedLogConstants.LOGSEGMENT_NAME_VERSION);
    }

    @Test
    public void testWriteOldNameRecoverNewName() throws Exception {
        backwardTest(0, DistributedLogConstants.LOGSEGMENT_NAME_VERSION, 3, 4, 4, 0);
    }

    @Test
    public void testWriteNewNameRecoverOldName() throws Exception {
        backwardTest(DistributedLogConstants.LOGSEGMENT_NAME_VERSION, 0, 3, 4, 4, 0);
    }

}
