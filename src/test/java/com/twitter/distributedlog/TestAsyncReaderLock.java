package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.ReadCancelledException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.DistributedReentrantLock.LockClosedException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Stopwatch;

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function0;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TestAsyncReaderLock extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestAsyncReaderLock.class);

    @Rule
    public TestName runtime = new TestName();

    void assertAcquiredFlagsSet(boolean[] acquiredFlags, int endIndex) {
        for (int i = 0; i < endIndex; i++) {
            assertTrue("reader " + i + " should have acquired lock", acquiredFlags[i]);
        }
        for (int i = endIndex; i < acquiredFlags.length; i++) {
            assertFalse("reader " + i + " should not have acquired lock", acquiredFlags[i]);
        }
    }

    @Test(timeout = 60000)
    public void testReaderLockIfLockPathDoesntExist() throws Exception {
        final String name = runtime.getMethodName();
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.closeAndComplete();

        Future<AsyncLogReader> futureReader1 = dlm.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        BKAsyncLogReaderDLSN reader1 = (BKAsyncLogReaderDLSN) Await.result(futureReader1);
        LogRecord record = Await.result(reader1.readNext());
        assertEquals(1L, record.getTransactionId());
        DLMTestUtil.verifyLogRecord(record);

        String readLockPath = reader1.bkLedgerManager.getReadLockPath();
        reader1.close();

        // simulate a old stream created without readlock path
        writer.bkDistributedLogManager.getWriterZKC().get().delete(readLockPath, -1);
        Future<AsyncLogReader> futureReader2 = dlm.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader2 = Await.result(futureReader2);
        record = Await.result(reader2.readNext());
        assertEquals(1L, record.getTransactionId());
        DLMTestUtil.verifyLogRecord(record);
    }

    @Test(timeout = 60000)
    public void testReaderLockBackgroundReaderLockAcquire() throws Exception {
        final String name = runtime.getMethodName();
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.closeAndComplete();

        Future<AsyncLogReader> futureReader1 = dlm.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Await.result(futureReader1);
        reader1.readNext();

        final CountDownLatch acquiredLatch = new CountDownLatch(1);
        final AtomicBoolean acquired = new AtomicBoolean(false);
        Thread acquireThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Future<AsyncLogReader> futureReader2 = null;
                DistributedLogManager dlm2 = null;
                try {
                    dlm2 = DLMTestUtil.createNewDLM(conf, name);
                    futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
                    AsyncLogReader reader2 = Await.result(futureReader2);
                    acquired.set(true);
                    acquiredLatch.countDown();
                } catch (Exception ex) {
                    fail("shouldn't reach here");
                } finally {
                    try {
                        dlm2.close();
                    } catch (Exception ex) {
                        fail("shouldn't reach here");
                    }
                }
            }
        }, "acquire-thread");
        acquireThread.start();

        Thread.sleep(1000);
        assertEquals(false, acquired.get());
        reader1.close();

        acquiredLatch.await();
        assertEquals(true, acquired.get());
        dlm.close();
    }

    int countDefined(ArrayList<Future<AsyncLogReader>> readers) {
        int done = 0;
        for (Future<AsyncLogReader> futureReader : readers) {
            if (futureReader.isDefined()) {
                done++;
            }
        } 
        return done;
    }

    @Test(timeout = 60000)
    public void testReaderLockManyLocks() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        int count = 5;
        final CountDownLatch acquiredLatch = new CountDownLatch(count);
        final ArrayList<Future<AsyncLogReader>> readers = new ArrayList<Future<AsyncLogReader>>(5);
        for (int i = 0; i < count; i++) {
            readers.add(null);
        }
        final DistributedLogManager[] dlms = new DistributedLogManager[count];
        final AtomicInteger acquired = new AtomicInteger(0);

        for (int i = 0; i < count; i++) {
            final int index = i;

            dlms[i] = DLMTestUtil.createNewDLM(conf, name);
            readers.set(i, dlms[i].getAsyncLogReaderWithLock(DLSN.InitialDLSN));
            readers.get(i).addEventListener(new FutureEventListener<AsyncLogReader>() {
                @Override
                public void onSuccess(AsyncLogReader reader) {
                    assertEquals(countDefined(readers), acquired.incrementAndGet());
                    acquiredLatch.countDown();
                    try {
                        reader.close();
                    } catch (IOException ioe) {
                        fail("unexpected exception on reader close");
                    }
                }
                @Override
                public void onFailure(Throwable cause) {
                    fail("acquire shouldnt have failed");
                }
            });
        }

        acquiredLatch.await();
        for (int i = 0; i < count; i++) {
            dlms[i].close();
        }

        dlm.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockDlmClosed() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = DLMTestUtil.createNewDLM(conf, name);
        Future<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        Await.result(futureReader1);

        BKDistributedLogManager dlm2 = (BKDistributedLogManager) DLMTestUtil.createNewDLM(conf, name);
        Future<AsyncLogReader> futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        final CountDownLatch latch = new CountDownLatch(1);
        dlm2.getReaderFuturePool().apply(new Function0<Void>() {
            @Override
            public Void apply() {
                latch.countDown();
                return null;
            }
        });
        latch.await();

        dlm2.close();
        try {
            Await.result(futureReader2);
            fail("should have thrown exception!");
        } catch (LockClosedException ex) {
        }

        dlm0.close();
        dlm1.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockSessionExpires() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = DLMTestUtil.createNewDLM(conf, name);
        Future<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Await.result(futureReader1);
        ZooKeeperClientUtils.expireSession(((BKDistributedLogManager)dlm1).getWriterZKC(), zkServers, 1000);

        // The result of expireSession is somewhat non-deterministic with this lock.
        // It may fail with LockingException or it may succesfully reacquire, so for
        // the moment rather than make it deterministic we accept either result.
        boolean success = false;
        try {
            Await.result(reader1.readNext());
            success = true;
        } catch (LockingException ex) {
        }
        if (success) {
            Await.result(reader1.readNext());
        }

        reader1.close();
        dlm0.close();
        dlm1.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockFutureCancelledWhileWaiting() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = DLMTestUtil.createNewDLM(conf, name);
        Future<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Await.result(futureReader1);

        DistributedLogManager dlm2 = DLMTestUtil.createNewDLM(conf, name);
        Future<AsyncLogReader> futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        try {
            futureReader2.cancel();
            Await.result(futureReader2);
        } catch (LockClosedException ex) {
        }

        futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        reader1.close();

        Await.result(futureReader2);

        dlm0.close();
        dlm1.close();
        dlm2.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockFutureCancelledWhileLocked() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = DLMTestUtil.createNewDLM(conf, name);
        Future<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        // Must not throw or cancel or do anything bad, future already completed.
        Await.result(futureReader1);
        futureReader1.cancel();
        AsyncLogReader reader1 = Await.result(futureReader1);
        Await.result(reader1.readNext());

        dlm0.close();
        dlm1.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockSharedDlmDoesNotConflict() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = DLMTestUtil.createNewDLM(conf, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter)(dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = DLMTestUtil.createNewDLM(conf, name);
        Future<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        Future<AsyncLogReader> futureReader2 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        // Both use the same client id, so there's no lock conflict. Not necessarily ideal, but how the
        // system currently works.
        Await.result(futureReader1);
        Await.result(futureReader2);

        dlm0.close();
        dlm1.close();
    }

    static class ReadRecordsListener implements FutureEventListener<AsyncLogReader> {
        CountDownLatch latch = new CountDownLatch(1);
        boolean failed = false;
        AtomicReference<DLSN> currentDLSN;
        String name;

        public ReadRecordsListener(AtomicReference<DLSN> currentDLSN, String name) {
            this.currentDLSN = currentDLSN;
            this.name = name;
        }
        public CountDownLatch getLatch() {
            return latch;
        }
        public boolean failed() {
            return failed;
        }
        public boolean done() {
            return latch.getCount() == 0;
        }

        @Override
        public void onSuccess(AsyncLogReader reader) {
            try {
                for (int i = 0; i < 300; i++) {
                    LogRecordWithDLSN record = Await.result(reader.readNext());
                    currentDLSN.set(record.getDlsn());
                }
            } catch (Exception ex) {
                failed = true;
            } finally {
                latch.countDown();
            }
        }
        @Override
        public void onFailure(Throwable cause) {
            failed = true;
            latch.countDown();
        }
    }

    @Test(timeout = 60000)
    public void testReaderLockMultiReadersScenario() throws Exception {
        final String name = runtime.getMethodName();

        // Force immediate flush to make dlsn counting easy.
        DistributedLogConfiguration localConf = new DistributedLogConfiguration();
        localConf.addConfiguration(conf);
        localConf.setImmediateFlushEnabled(true);
        localConf.setOutputBufferSize(0);

        DistributedLogManager dlm0 = DLMTestUtil.createNewDLM(localConf, name);
        DLMTestUtil.generateCompletedLogSegments(dlm0, localConf, 9, 100);
        dlm0.close();

        int recordCount = 0;
        AtomicReference<DLSN> currentDLSN = new AtomicReference<DLSN>(DLSN.InitialDLSN);

        DistributedLogManager dlm1 = DLMTestUtil.createNewDLM(localConf, name);
        DistributedLogManager dlm2 = DLMTestUtil.createNewDLM(localConf, name);
        DistributedLogManager dlm3 = DLMTestUtil.createNewDLM(localConf, name);

        Future<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Await.result(futureReader1);
        
        Future<AsyncLogReader> futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        Future<AsyncLogReader> futureReader3 = dlm3.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        ReadRecordsListener listener2 = new ReadRecordsListener(currentDLSN, "reader-2");
        ReadRecordsListener listener3 = new ReadRecordsListener(currentDLSN, "reader-3");
        futureReader2.addEventListener(listener2);
        futureReader3.addEventListener(listener3);

        // Get reader1 and start reading.
        for ( ; recordCount < 200; recordCount++) {
            LogRecordWithDLSN record = Await.result(reader1.readNext());
            currentDLSN.set(record.getDlsn());
        }

        // Take a break, reader2 decides to stop waiting and cancels.
        assertFalse(listener2.done());
        futureReader2.cancel();
        listener2.getLatch().await();
        assertTrue(listener2.done());
        assertTrue(listener2.failed());

        // Reader1 starts reading again.
        for (; recordCount < 300; recordCount++) {
            LogRecordWithDLSN record = Await.result(reader1.readNext());
            currentDLSN.set(record.getDlsn());
        }

        // Reader1 is done, someone else can take over. Since reader2 was
        // aborted, reader3 should take its place.
        assertFalse(listener3.done());
        reader1.close();
        listener3.getLatch().await();
        assertTrue(listener3.done());
        assertFalse(listener3.failed());

        assertEquals(new DLSN(3, 99, 0), currentDLSN.get());

        try {
            Await.result(futureReader2);
        } catch (Exception ex) {
            // Can't get this one to close it--the dlm will take care of it.
        }

        Await.result(futureReader3).close();

        dlm1.close();
        dlm2.close();
        dlm3.close();
    }
}
