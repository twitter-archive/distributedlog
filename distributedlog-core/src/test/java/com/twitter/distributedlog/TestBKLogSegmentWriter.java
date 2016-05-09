/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.BKTransmitException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.impl.BKLogSegmentEntryWriter;
import com.twitter.distributedlog.io.Abortables;
import com.twitter.distributedlog.lock.SessionLockFactory;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.lock.ZKSessionLockFactory;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import scala.runtime.AbstractFunction0;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test Case for BookKeeper Based Log Segment Writer
 */
public class TestBKLogSegmentWriter extends TestDistributedLogBase {

    @Rule
    public TestName runtime = new TestName();

    private OrderedScheduler scheduler;
    private OrderedScheduler lockStateExecutor;
    private ZooKeeperClient zkc;
    private ZooKeeperClient zkc0;
    private BookKeeperClient bkc;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        scheduler = OrderedScheduler.newBuilder().corePoolSize(1).build();
        lockStateExecutor = OrderedScheduler.newBuilder().corePoolSize(1).build();
        // build zookeeper client
        URI uri = createDLMURI("");
        zkc = ZooKeeperClientBuilder.newBuilder()
                .name("test-zkc")
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .zkAclId(conf.getZkAclId())
                .uri(uri)
                .build();
        zkc0 = ZooKeeperClientBuilder.newBuilder()
                .name("test-zkc0")
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .zkAclId(conf.getZkAclId())
                .uri(uri)
                .build();
        // build bookkeeper client
        BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, uri);
        bkc = BookKeeperClientBuilder.newBuilder()
                .dlConfig(conf)
                .name("test-bkc")
                .ledgersPath(bkdlConfig.getBkLedgersPath())
                .zkServers(DLUtils.getZKServersFromDLUri(uri))
                .build();
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (null != bkc) {
            bkc.close();
        }
        if (null != zkc) {
            zkc.close();
        }
        if (null != lockStateExecutor) {
            lockStateExecutor.shutdown();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
        super.teardown();
    }

    private DistributedLogConfiguration newLocalConf() {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        return confLocal;
    }

    private DistributedLock createLock(String path,
                                       ZooKeeperClient zkClient,
                                       boolean acquireLock)
            throws Exception {
        try {
            Await.result(Utils.zkAsyncCreateFullPathOptimistic(zkClient, path, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        } catch (KeeperException.NodeExistsException nee) {
            // node already exists
        }
        SessionLockFactory lockFactory = new ZKSessionLockFactory(
                zkClient,
                "test-lock",
                lockStateExecutor,
                0,
                Long.MAX_VALUE,
                conf.getZKSessionTimeoutMilliseconds(),
                NullStatsLogger.INSTANCE
        );
        DistributedLock lock = new DistributedLock(
                lockStateExecutor,
                lockFactory,
                path,
                Long.MAX_VALUE,
                NullStatsLogger.INSTANCE);
        if (acquireLock) {
            return FutureUtils.result(lock.asyncAcquire());
        } else {
            return lock;
        }
    }

    private void closeWriterAndLock(BKLogSegmentWriter writer,
                                    DistributedLock lock)
            throws IOException {
        try {
            FutureUtils.result(writer.asyncClose());
        } finally {
            Utils.closeQuietly(lock);
        }
    }

    private void abortWriterAndLock(BKLogSegmentWriter writer,
                                    DistributedLock lock)
            throws IOException {
        try {
            Abortables.abort(writer, false);
        } finally {
            Utils.closeQuietly(lock);
        }
    }

    private BKLogSegmentWriter createLogSegmentWriter(DistributedLogConfiguration conf,
                                                      long logSegmentSequenceNumber,
                                                      long startTxId,
                                                      DistributedLock lock) throws Exception {
        LedgerHandle lh = bkc.get().createLedger(3, 2, 2,
                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
        return new BKLogSegmentWriter(
                runtime.getMethodName(),
                runtime.getMethodName(),
                conf,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION,
                new BKLogSegmentEntryWriter(lh),
                lock,
                startTxId,
                logSegmentSequenceNumber,
                scheduler,
                NullStatsLogger.INSTANCE,
                NullStatsLogger.INSTANCE,
                new AlertStatsLogger(NullStatsLogger.INSTANCE, "test"),
                PermitLimiter.NULL_PERMIT_LIMITER,
                new SettableFeatureProvider("", 0),
                ConfUtils.getConstDynConf(conf));
    }

    private LedgerHandle openLedgerNoRecovery(LedgerHandle lh) throws Exception {
        return bkc.get().openLedgerNoRecovery(lh.getId(),
                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
    }

    private LedgerHandle openLedger(LedgerHandle lh) throws Exception {
        return bkc.get().openLedger(lh.getId(),
                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
    }

    private void fenceLedger(LedgerHandle lh) throws Exception {
        bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                conf.getBKDigestPW().getBytes(UTF_8));
    }

    /**
     * Close a segment log writer should flush buffered data.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testCloseShouldFlush() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);
        // Use another lock to wait for writer releasing lock
        DistributedLock lock0 = createLock("/test/lock-" + runtime.getMethodName(), zkc0, false);
        Future<DistributedLock> lockFuture0 = lock0.asyncAcquire();
        // add 10 records
        int numRecords = 10;
        List<Future<DLSN>> futureList = new ArrayList<Future<DLSN>>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            futureList.add(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(i)));
        }
        assertEquals("Last tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should be -1",
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should be " + numRecords,
                10, writer.getPositionWithinLogSegment());
        // close the writer should flush buffered data and release lock
        closeWriterAndLock(writer, lock);
        Await.result(lockFuture0);
        lock0.checkOwnership();
        assertEquals("Last tx id should still be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should become " + (numRecords - 1),
                numRecords - 1, writer.getLastTxIdAcknowledged());
        assertEquals("Position should still be " + numRecords,
                10, writer.getPositionWithinLogSegment());
        List<DLSN> dlsns = Await.result(Future.collect(futureList));
        assertEquals("All records should be written",
                numRecords, dlsns.size());
        for (int i = 0; i < numRecords; i++) {
            DLSN dlsn = dlsns.get(i);
            assertEquals("Incorrent ledger sequence number",
                    0L, dlsn.getLogSegmentSequenceNo());
            assertEquals("Incorrent entry id",
                    0L, dlsn.getEntryId());
            assertEquals("Inconsistent slot id",
                    i, dlsn.getSlotId());
        }
        assertEquals("Last DLSN should be " + dlsns.get(dlsns.size() - 1),
                dlsns.get(dlsns.size() - 1), writer.getLastDLSN());
        LedgerHandle lh = getLedgerHandle(writer);
        LedgerHandle readLh = openLedgerNoRecovery(lh);
        assertTrue("Ledger " + lh.getId() + " should be closed", readLh.isClosed());
        assertEquals("There should be two entries in ledger " + lh.getId(),
                1L, readLh.getLastAddConfirmed());
    }

    /**
     * Abort a segment log writer should just abort pending writes and not flush buffered data.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAbortShouldNotFlush() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);
        // Use another lock to wait for writer releasing lock
        DistributedLock lock0 = createLock("/test/lock-" + runtime.getMethodName(), zkc0, false);
        Future<DistributedLock> lockFuture0 = lock0.asyncAcquire();
        // add 10 records
        int numRecords = 10;
        List<Future<DLSN>> futureList = new ArrayList<Future<DLSN>>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            futureList.add(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(i)));
        }
        assertEquals("Last tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should be -1",
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should be " + numRecords,
                10, writer.getPositionWithinLogSegment());
        // close the writer should flush buffered data and release lock
        abortWriterAndLock(writer, lock);
        Await.result(lockFuture0);
        lock0.checkOwnership();
        assertEquals("Last tx id should still be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should still be " + (numRecords - 1),
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should still be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should still be " + numRecords,
                10, writer.getPositionWithinLogSegment());

        for (int i = 0; i < numRecords; i++) {
            try {
                Await.result(futureList.get(i));
                fail("Should be aborted record " + i + " with transmit exception");
            } catch (WriteCancelledException wce) {
                assertTrue("Record " + i + " should be aborted because of ledger fenced",
                        wce.getCause() instanceof BKTransmitException);
                BKTransmitException bkte = (BKTransmitException) wce.getCause();
                assertEquals("Record " + i + " should be aborted",
                        BKException.Code.InterruptedException, bkte.getBKResultCode());
            }
        }

        // check no entries were written
        LedgerHandle lh = getLedgerHandle(writer);
        LedgerHandle readLh = openLedgerNoRecovery(lh);
        assertTrue("Ledger " + lh.getId() + " should not be closed", readLh.isClosed());
        assertEquals("There should be no entries in ledger " + lh.getId(),
                LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
    }


    /**
     * Close a log segment writer that already detect ledger fenced, should not flush buffered data.
     * And should throw exception on closing.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testCloseShouldNotFlushIfLedgerFenced() throws Exception {
        testCloseShouldNotFlushIfInErrorState(BKException.Code.LedgerFencedException);
    }

    /**
     * Close a log segment writer that is already in error state, should not flush buffered data.
     *
     * @throws Exception
     */
    void testCloseShouldNotFlushIfInErrorState(int rcToFailComplete) throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);
        // Use another lock to wait for writer releasing lock
        DistributedLock lock0 = createLock("/test/lock-" + runtime.getMethodName(), zkc0, false);
        Future<DistributedLock> lockFuture0 = lock0.asyncAcquire();
        // add 10 records
        int numRecords = 10;
        List<Future<DLSN>> futureList = new ArrayList<Future<DLSN>>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            futureList.add(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(i)));
        }
        assertEquals("Last tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should be -1",
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should be " + numRecords,
                10, writer.getPositionWithinLogSegment());
        writer.setTransmitResult(rcToFailComplete);
        // close the writer should release lock but not flush data
        try {
            closeWriterAndLock(writer, lock);
            fail("Close a log segment writer in error state should throw exception");
        } catch (BKTransmitException bkte) {
            assertEquals("Inconsistent rc is thrown",
                    rcToFailComplete, bkte.getBKResultCode());
        }
        Await.result(lockFuture0);
        lock0.checkOwnership();
        assertEquals("Last tx id should still be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should still be " + (numRecords - 1),
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should still be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should still be " + numRecords,
                10, writer.getPositionWithinLogSegment());

        for (int i = 0; i < numRecords; i++) {
            try {
                Await.result(futureList.get(i));
                fail("Should be aborted record " + i + " with transmit exception");
            } catch (WriteCancelledException wce) {
                assertTrue("Record " + i + " should be aborted because of ledger fenced",
                        wce.getCause() instanceof BKTransmitException);
                BKTransmitException bkte = (BKTransmitException) wce.getCause();
                assertEquals("Record " + i + " should be aborted",
                        rcToFailComplete, bkte.getBKResultCode());
            }
        }

        // check no entries were written
        LedgerHandle lh = getLedgerHandle(writer);
        LedgerHandle readLh = openLedgerNoRecovery(lh);
        assertFalse("Ledger " + lh.getId() + " should not be closed", readLh.isClosed());
        assertEquals("There should be no entries in ledger " + lh.getId(),
                LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
    }

    /**
     * Close the writer when ledger is fenced: it should release the lock, fail on flushing data and throw exception
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testCloseShouldFailIfLedgerFenced() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);
        // Use another lock to wait for writer releasing lock
        DistributedLock lock0 = createLock("/test/lock-" + runtime.getMethodName(), zkc0, false);
        Future<DistributedLock> lockFuture0 = lock0.asyncAcquire();
        // add 10 records
        int numRecords = 10;
        List<Future<DLSN>> futureList = new ArrayList<Future<DLSN>>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            futureList.add(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(i)));
        }
        assertEquals("Last tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should be -1",
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should be " + numRecords,
                10, writer.getPositionWithinLogSegment());
        // fence the ledger
        fenceLedger(getLedgerHandle(writer));
        // close the writer: it should release the lock, fail on flushing data and throw exception
        try {
            closeWriterAndLock(writer, lock);
            fail("Close a log segment writer when ledger is fenced should throw exception");
        } catch (BKTransmitException bkte) {
            assertEquals("Inconsistent rc is thrown",
                    BKException.Code.LedgerFencedException, bkte.getBKResultCode());
        }

        Await.result(lockFuture0);
        lock0.checkOwnership();

        assertEquals("Last tx id should still be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should still be " + (numRecords - 1),
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should still be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should still be " + numRecords,
                10, writer.getPositionWithinLogSegment());

        for (int i = 0; i < numRecords; i++) {
            try {
                Await.result(futureList.get(i));
                fail("Should be aborted record " + i + " with transmit exception");
            } catch (BKTransmitException bkte) {
                assertEquals("Record " + i + " should be aborted",
                        BKException.Code.LedgerFencedException, bkte.getBKResultCode());
            }
        }

        // check no entries were written
        LedgerHandle lh = getLedgerHandle(writer);
        LedgerHandle readLh = openLedgerNoRecovery(lh);
        assertTrue("Ledger " + lh.getId() + " should be closed", readLh.isClosed());
        assertEquals("There should be no entries in ledger " + lh.getId(),
                LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
    }

    /**
     * Abort should wait for outstanding transmits to be completed and cancel buffered data.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAbortShouldFailAllWrites() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);
        // Use another lock to wait for writer releasing lock
        DistributedLock lock0 = createLock("/test/lock-" + runtime.getMethodName(), zkc0, false);
        Future<DistributedLock> lockFuture0 = lock0.asyncAcquire();
        // add 10 records
        int numRecords = 10;
        List<Future<DLSN>> futureList = new ArrayList<Future<DLSN>>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            futureList.add(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(i)));
        }
        assertEquals("Last tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should be -1",
                -1L, writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should be " + numRecords,
                numRecords, writer.getPositionWithinLogSegment());

        final CountDownLatch deferLatch = new CountDownLatch(1);
        writer.getFuturePool().apply(new AbstractFunction0<Object>() {
            @Override
            public Object apply() {
                try {
                    deferLatch.await();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted on deferring completion : ", e);
                }
                return null;
            }
        });

        // transmit the buffered data
        FutureUtils.result(writer.flush());

        // add another 10 records
        List<Future<DLSN>> anotherFutureList = new ArrayList<Future<DLSN>>(numRecords);
        for (int i = numRecords; i < 2 * numRecords; i++) {
            anotherFutureList.add(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(i)));
        }
        assertEquals("Last tx id should become " + (2 * numRecords - 1),
                2 * numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should become " + (numRecords - 1),
                (long) (numRecords - 1), writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should still be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should become " + (2 * numRecords),
                2 * numRecords, writer.getPositionWithinLogSegment());

        // abort the writer: it waits for outstanding transmits and abort buffered data
        abortWriterAndLock(writer, lock);

        Await.result(lockFuture0);
        lock0.checkOwnership();

        // release defer latch so completion would go through
        deferLatch.countDown();

        List<DLSN> dlsns = Await.result(Future.collect(futureList));
        assertEquals("All first 10 records should be written",
                numRecords, dlsns.size());
        for (int i = 0; i < numRecords; i++) {
            DLSN dlsn = dlsns.get(i);
            assertEquals("Incorrent ledger sequence number",
                    0L, dlsn.getLogSegmentSequenceNo());
            assertEquals("Incorrent entry id",
                    0L, dlsn.getEntryId());
            assertEquals("Inconsistent slot id",
                    i, dlsn.getSlotId());
        }
        for (int i = 0; i < numRecords; i++) {
            try {
                Await.result(anotherFutureList.get(i));
                fail("Should be aborted record " + (numRecords + i) + " with transmit exception");
            } catch (WriteCancelledException wce) {
                // writes should be cancelled.
            }
        }

        assertEquals("Last tx id should still be " + (2 * numRecords - 1),
                2 * numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should be still " + (numRecords - 1),
                (long) (numRecords - 1), writer.getLastTxIdAcknowledged());
        assertEquals("Last DLSN should become " + futureList.get(futureList.size() - 1),
                dlsns.get(futureList.size() - 1), writer.getLastDLSN());
        assertEquals("Position should become " + 2 * numRecords,
                2 * numRecords, writer.getPositionWithinLogSegment());

        // check only 1 entry were written
        LedgerHandle lh = getLedgerHandle(writer);
        LedgerHandle readLh = openLedgerNoRecovery(lh);
        assertTrue("Ledger " + lh.getId() + " should not be closed", readLh.isClosed());
        assertEquals("Only one entry is written for ledger " + lh.getId(),
                0L, lh.getLastAddPushed());
        assertEquals("Only one entry is written for ledger " + lh.getId(),
                0L, readLh.getLastAddConfirmed());
    }

    /**
     * Log Segment Writer should only update last tx id only for user records.
     */
    @Test(timeout = 60000)
    public void testUpdateLastTxIdForUserRecords() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);
        // add 10 records
        int numRecords = 10;
        List<Future<DLSN>> futureList = new ArrayList<Future<DLSN>>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            futureList.add(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(i)));
        }
        LogRecord controlRecord = DLMTestUtil.getLogRecordInstance(9999L);
        controlRecord.setControl();
        futureList.add(writer.asyncWrite(controlRecord));
        assertEquals("Last tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last DLSN should be " + DLSN.InvalidDLSN,
                DLSN.InvalidDLSN, writer.getLastDLSN());
        assertEquals("Position should be " + numRecords,
                numRecords, writer.getPositionWithinLogSegment());

        // close the writer to flush the output buffer
        closeWriterAndLock(writer, lock);

        List<DLSN> dlsns = Await.result(Future.collect(futureList));
        assertEquals("All 11 records should be written",
                numRecords + 1, dlsns.size());
        for (int i = 0; i < numRecords; i++) {
            DLSN dlsn = dlsns.get(i);
            assertEquals("Incorrent ledger sequence number",
                    0L, dlsn.getLogSegmentSequenceNo());
            assertEquals("Incorrent entry id",
                    0L, dlsn.getEntryId());
            assertEquals("Inconsistent slot id",
                    i, dlsn.getSlotId());
        }
        DLSN dlsn = dlsns.get(numRecords);
        assertEquals("Incorrent ledger sequence number",
                0L, dlsn.getLogSegmentSequenceNo());
        assertEquals("Incorrent entry id",
                1L, dlsn.getEntryId());
        assertEquals("Inconsistent slot id",
                0L, dlsn.getSlotId());

        assertEquals("Last tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxId());
        assertEquals("Last acked tx id should be " + (numRecords - 1),
                numRecords - 1, writer.getLastTxIdAcknowledged());
        assertEquals("Position should be " + numRecords,
                numRecords, writer.getPositionWithinLogSegment());
        assertEquals("Last DLSN should be " + dlsn,
                dlsns.get(numRecords - 1), writer.getLastDLSN());
    }

    /**
     * Non durable write should fail if writer is closed.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testNondurableWriteAfterWriterIsClosed() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setDurableWriteEnabled(false);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);

        // close the writer
        closeWriterAndLock(writer, lock);
        FutureUtils.result(writer.asyncClose());

        try {
            Await.result(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(1)));
            fail("Should fail the write if the writer is closed");
        } catch (WriteException we) {
            // expected
        }
    }

    /**
     * Non durable write should fail if writer is marked as end of stream.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testNondurableWriteAfterEndOfStream() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setDurableWriteEnabled(false);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);

        FutureUtils.result(writer.markEndOfStream());

        try {
            Await.result(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(1)));
            fail("Should fail the write if the writer is marked as end of stream");
        } catch (EndOfStreamException we) {
            // expected
        }

        closeWriterAndLock(writer, lock);
    }

    /**
     * Non durable write should fail if the log segment is fenced.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testNondurableWriteAfterLedgerIsFenced() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setDurableWriteEnabled(false);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);

        // fence the ledger
        fenceLedger(getLedgerHandle(writer));

        LogRecord record = DLMTestUtil.getLogRecordInstance(1);
        record.setControl();
        try {
            Await.result(writer.asyncWrite(record));
            fail("Should fail the writer if the log segment is already fenced");
        } catch (BKTransmitException bkte) {
            // expected
            assertEquals(BKException.Code.LedgerFencedException, bkte.getBKResultCode());
        }

        try {
            Await.result(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(2)));
            fail("Should fail the writer if the log segment is already fenced");
        } catch (WriteException we) {
            // expected
        }

        abortWriterAndLock(writer, lock);
    }

    /**
     * Non durable write should fail if writer is marked as end of stream.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testNondurableWrite() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(Integer.MAX_VALUE);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setDurableWriteEnabled(false);
        DistributedLock lock = createLock("/test/lock-" + runtime.getMethodName(), zkc, true);
        BKLogSegmentWriter writer =
                createLogSegmentWriter(confLocal, 0L, -1L, lock);

        assertEquals(DLSN.InvalidDLSN,
                Await.result(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(2))));
        assertEquals(-1L, ((BKLogSegmentEntryWriter) writer.getEntryWriter())
                .getLedgerHandle().getLastAddPushed());

        closeWriterAndLock(writer, lock);
    }
}
