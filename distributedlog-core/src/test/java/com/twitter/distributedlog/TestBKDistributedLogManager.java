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

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.twitter.distributedlog.impl.ZKLogSegmentMetadataStore;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadata;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.subscription.SubscriptionStateStore;
import com.twitter.distributedlog.subscription.SubscriptionsStore;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestBKDistributedLogManager extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestBKDistributedLogManager.class);

    @Rule
    public TestName testNames = new TestName();

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    private void testNonPartitionedWritesInternal(String name, DistributedLogConfiguration conf) throws Exception {
        BKDistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter();
            writer.closeAndComplete();
            BKLogWriteHandler blplm = dlm.createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                                perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            blplm.close();
        }

        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
    }

    @Test(timeout = 60000)
    public void testSimpleWrite() throws Exception {
        BKDistributedLogManager dlm = createNewDLM(conf, "distrlog-simplewrite");
        BKSyncLogWriter out = dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
        out.closeAndComplete();

        BKLogWriteHandler blplm = dlm.createWriteLedgerHandler(conf.getUnpartitionedStreamName());
        assertNotNull(zkc.exists(blplm.completedLedgerZNode(1, 100,
                perStreamLogWriter.getLogSegmentSequenceNumber()), false));
        blplm.close();
    }

    @Test(timeout = 60000)
    public void testNumberOfTransactions() throws Exception {
        String name = "distrlog-txncount";
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        long numTrans = DLMTestUtil.getNumberofLogRecords(createNewDLM(conf, name), 1);
        assertEquals(100, numTrans);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testSanityCheckTxnID() throws Exception {
        String name = "distrlog-sanity-check-txnid";
        BKDistributedLogManager dlm = createNewDLM(conf, name);
        BKSyncLogWriter out = dlm.startLogSegmentNonPartitioned();
        long txid = 1;
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        BKSyncLogWriter out1 = dlm.startLogSegmentNonPartitioned();
        LogRecord op1 = DLMTestUtil.getLogRecordInstance(1);
        try {
            out1.write(op1);
            fail("Should fail writing lower txn id if sanityCheckTxnID is enabled.");
        } catch (TransactionIdOutOfOrderException tioooe) {
            // expected
        }
        out1.closeAndComplete();
        dlm.close();

        DLMTestUtil.updateBKDLConfig(bkutil.getUri(), bkutil.getZkServers(), bkutil.getBkLedgerPath(), false);
        LOG.info("Disable sanity check txn id.");
        BKDLConfig.clearCachedDLConfigs();

        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        BKDistributedLogManager newDLM = createNewDLM(newConf, name);
        BKSyncLogWriter out2 = newDLM.startLogSegmentNonPartitioned();
        LogRecord op2 = DLMTestUtil.getLogRecordInstance(1);
        out2.write(op2);
        out2.closeAndComplete();
        newDLM.close();

        DLMTestUtil.updateBKDLConfig(bkutil.getUri(), bkutil.getZkServers(), bkutil.getBkLedgerPath(), true);
        LOG.info("Enable sanity check txn id.");
        BKDLConfig.clearCachedDLConfigs();

        DistributedLogConfiguration conf3 = new DistributedLogConfiguration();
        conf3.addConfiguration(conf);
        BKDistributedLogManager dlm3 = createNewDLM(newConf, name);
        BKSyncLogWriter out3 = dlm3.startLogSegmentNonPartitioned();
        LogRecord op3 = DLMTestUtil.getLogRecordInstance(1);
        try {
            out3.write(op3);
            fail("Should fail writing lower txn id if sanityCheckTxnID is enabled.");
        } catch (TransactionIdOutOfOrderException tioooe) {
            // expected
        }
        out3.closeAndComplete();
        dlm3.close();
    }

    @Test(timeout = 60000)
    public void testContinuousReaders() throws Exception {
        String name = "distrlog-continuous";
        BKDistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = dlm.createWriteLedgerHandler(conf.getUnpartitionedStreamName());

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            blplm.close();
        }

        BKSyncLogWriter out = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.close();
        dlm.close();

        dlm = createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals((txid - 1), numTrans);
        assertEquals(txid - 1, dlm.getLogRecordCount());
        reader.close();
        dlm.close();
    }

    /**
     * Create a bkdlm namespace, write a journal from txid 1, close stream.
     * Try to create a new journal from txid 1. Should throw an exception.
     */
    @Test(timeout = 60000)
    public void testWriteRestartFrom1() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, "distrlog-restartFrom1");
        long txid = 1;
        BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        txid = 1;
        try {
            out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(txid));
            fail("Shouldn't be able to start another journal from " + txid
                + " when one already exists");
        } catch (Exception ioe) {
            LOG.info("Caught exception as expected", ioe);
        } finally {
            out.close();
        }

        // test border case
        txid = DEFAULT_SEGMENT_SIZE - 1;
        try {
            out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(txid));
            fail("Shouldn't be able to start another journal from " + txid
                + " when one already exists");
        } catch (TransactionIdOutOfOrderException rste) {
            LOG.info("Caught exception as expected", rste);
        } finally {
            out.close();
        }

        // open journal continuing from before
        txid = DEFAULT_SEGMENT_SIZE + 1;
        out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        assertNotNull(out);

        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        // open journal arbitarily far in the future
        txid = DEFAULT_SEGMENT_SIZE * 4;
        out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        out.write(DLMTestUtil.getLogRecordInstance(txid));
        out.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testTwoWriters() throws Exception {
        long start = 1;
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlm1 =
                createNewBKDLM(conf, "distrlog-dualWriter");
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlm2 = createNewBKDLM(conf, "distrlog-dualWriter");

        bkdlm1.getWriteHandler().startLogSegment(start);
        try {
            bkdlm2.getWriteHandler().startLogSegment(start);
            fail("Shouldn't have been able to open the second writer");
        } catch (OwnershipAcquireFailedException ioe) {
            assertEquals(ioe.getCurrentOwner(), DistributedLogConstants.UNKNOWN_CLIENT_ID);
        }

        bkdlm1.close();
        bkdlm2.close();
    }

    @Test(timeout = 60000)
    public void testSimpleRead() throws Exception {
        String name = "distrlog-simpleread";
        DistributedLogManager dlm = createNewDLM(conf, name);
        final long numTransactions = 10000;
        BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= numTransactions; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        assertEquals(numTransactions, DLMTestUtil.getNumberofLogRecords(createNewDLM(conf, name), 1));
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testNumberOfTransactionsWithInprogressAtEnd() throws Exception {
        String name = "distrlog-inprogressAtEnd";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            blplm.close();
        }
        BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.close();

        long numTrans = DLMTestUtil.getNumberofLogRecords(createNewDLM(conf, name), 1);
        assertEquals((txid - 1), numTrans);
    }

    @Test(timeout = 60000)
    public void testContinuousReaderBulk() throws Exception {
        String name = "distrlog-continuous-bulk";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.closeAndComplete();
        }

        BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.close();
        dlm.close();

        dlm = createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        List<LogRecordWithDLSN> recordList = reader.readBulk(false, 13);
        long lastTxId = -1;
        while (!recordList.isEmpty()) {
            for (LogRecord record : recordList) {
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                DLMTestUtil.verifyLogRecord(record);
                numTrans++;
            }
            recordList = reader.readBulk(false, 13);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
    }

    @Test(timeout = 60000)
    public void testContinuousReadersWithEmptyLedgers() throws Exception {
        String name = "distrlog-continuous-emptyledgers";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter writer = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      writer.getLogSegmentSequenceNumber()), false));
            BKLogSegmentWriter perStreamLogWriter = blplm.startLogSegment(txid - 1);
            blplm.completeAndCloseLogSegment(perStreamLogWriter.getLogSegmentSequenceNumber(),
                    perStreamLogWriter.getLedgerHandle().getId(), txid - 1, txid - 1, 0);
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(txid - 1, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            blplm.close();
        }

        BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.close();
        dlm.close();

        dlm = createNewDLM(conf, name);

        AsyncLogReader asyncreader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        long numTrans = 0;
        LogRecordWithDLSN record = Await.result(asyncreader.readNext());
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            if (numTrans >= (txid - 1)) {
                break;
            }
            record = Await.result(asyncreader.readNext());
        }
        assertEquals((txid - 1), numTrans);
        asyncreader.close();

        LogReader reader = dlm.getInputStream(1);
        numTrans = 0;
        record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals((txid - 1), numTrans);
        reader.close();
        assertEquals(txid - 1, dlm.getLogRecordCount());
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testNonPartitionedWrites() throws Exception {
        String name = "distrlog-non-partitioned-bulk";
        testNonPartitionedWritesInternal(name, conf);
    }

    @Test(timeout = 60000)
    public void testCheckLogExists() throws Exception {
        String name = "distrlog-check-log-exists";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();
        dlm.createOrUpdateMetadata(name.getBytes());
        assertEquals(name, new String(dlm.getMetadata()));

        URI uri = createDLMURI("/" + name);
        BKDistributedLogNamespace namespace = BKDistributedLogNamespace.newBuilder()
                .conf(conf).uri(uri).build();
        assertTrue(namespace.logExists(name));
        assertFalse(namespace.logExists("non-existent-log"));
        URI nonExistentUri = createDLMURI("/" + "non-existent-ns");
        DistributedLogNamespace nonExistentNS = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf).uri(nonExistentUri).build();
        assertFalse(nonExistentNS.logExists(name));

        int logCount = 0;
        Iterator<String> logIter = namespace.getLogs();
        while(logIter.hasNext()) {
            String log = logIter.next();
            logCount++;
            assertEquals(name, log);
        }
        assertEquals(1, logCount);

        for(Map.Entry<String, byte[]> logEntry: namespace.enumerateLogsWithMetadataInNamespace().entrySet()) {
            assertEquals(name, new String(logEntry.getValue()));
        }
    }

    @Test(timeout = 60000)
    public void testMetadataAccessor() throws Exception {
        String name = "distrlog-metadata-accessor";
        MetadataAccessor metadata = DLMTestUtil.createNewMetadataAccessor(conf, name, createDLMURI("/" + name));
        assertEquals(name, metadata.getStreamName());
        metadata.createOrUpdateMetadata(name.getBytes());
        assertEquals(name, new String(metadata.getMetadata()));
        metadata.deleteMetadata();
        assertEquals(null, metadata.getMetadata());
    }

    @Test(timeout = 60000)
    @Deprecated
    public void testSubscriptionStateStore() throws Exception {
        String name = "distrlog-subscription-state";
        String subscriberId = "defaultSubscriber";
        DLSN commitPosition0 = new DLSN(4, 33, 5);
        DLSN commitPosition1 = new DLSN(4, 34, 5);
        DLSN commitPosition2 = new DLSN(5, 34, 5);

        DistributedLogManager dlm = createNewDLM(conf, name);
        SubscriptionStateStore store = dlm.getSubscriptionStateStore(subscriberId);
        assertEquals(Await.result(store.getLastCommitPosition()), DLSN.NonInclusiveLowerBound);
        Await.result(store.advanceCommitPosition(commitPosition1));
        assertEquals(Await.result(store.getLastCommitPosition()), commitPosition1);
        Await.result(store.advanceCommitPosition(commitPosition0));
        assertEquals(Await.result(store.getLastCommitPosition()), commitPosition1);
        Await.result(store.advanceCommitPosition(commitPosition2));
        assertEquals(Await.result(store.getLastCommitPosition()), commitPosition2);
        SubscriptionStateStore store1 = dlm.getSubscriptionStateStore(subscriberId);
        assertEquals(Await.result(store1.getLastCommitPosition()), commitPosition2);
    }

    @Test(timeout = 60000)
    public void testSubscriptionsStore() throws Exception {
        String name = "distrlog-subscriptions-store";
        String subscriber0 = "subscriber-0";
        String subscriber1 = "subscriber-1";
        String subscriber2 = "subscriber-2";

        DLSN commitPosition0 = new DLSN(4, 33, 5);
        DLSN commitPosition1 = new DLSN(4, 34, 5);
        DLSN commitPosition2 = new DLSN(5, 34, 5);
        DLSN commitPosition3 = new DLSN(6, 35, 6);

        DistributedLogManager dlm = createNewDLM(conf, name);

        SubscriptionsStore store = dlm.getSubscriptionsStore();

        // no data
        assertEquals(Await.result(store.getLastCommitPosition(subscriber0)), DLSN.NonInclusiveLowerBound);
        assertEquals(Await.result(store.getLastCommitPosition(subscriber1)), DLSN.NonInclusiveLowerBound);
        assertEquals(Await.result(store.getLastCommitPosition(subscriber2)), DLSN.NonInclusiveLowerBound);
        // empty
        assertTrue(Await.result(store.getLastCommitPositions()).isEmpty());

        // subscriber 0 advance
        Await.result(store.advanceCommitPosition(subscriber0, commitPosition0));
        assertEquals(commitPosition0, Await.result(store.getLastCommitPosition(subscriber0)));
        Map<String, DLSN> committedPositions = Await.result(store.getLastCommitPositions());
        assertEquals(1, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));

        // subscriber 1 advance
        Await.result(store.advanceCommitPosition(subscriber1, commitPosition1));
        assertEquals(commitPosition1, Await.result(store.getLastCommitPosition(subscriber1)));
        committedPositions = Await.result(store.getLastCommitPositions());
        assertEquals(2, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));
        assertEquals(commitPosition1, committedPositions.get(subscriber1));

        // subscriber 2 advance
        Await.result(store.advanceCommitPosition(subscriber2, commitPosition2));
        assertEquals(commitPosition2, Await.result(store.getLastCommitPosition(subscriber2)));
        committedPositions = Await.result(store.getLastCommitPositions());
        assertEquals(3, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));
        assertEquals(commitPosition1, committedPositions.get(subscriber1));
        assertEquals(commitPosition2, committedPositions.get(subscriber2));

        // subscriber 2 advance again
        DistributedLogManager newDLM = createNewDLM(conf, name);
        SubscriptionsStore newStore = newDLM.getSubscriptionsStore();
        Await.result(newStore.advanceCommitPosition(subscriber2, commitPosition3));
        newStore.close();
        newDLM.close();

        committedPositions = Await.result(store.getLastCommitPositions());
        assertEquals(3, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));
        assertEquals(commitPosition1, committedPositions.get(subscriber1));
        assertEquals(commitPosition3, committedPositions.get(subscriber2));

        dlm.close();

    }

    private long writeAndMarkEndOfStream(DistributedLogManager dlm, long txid) throws Exception {
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter writer = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter();

            if (i < 2) {
                writer.closeAndComplete();
                BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                                    perStreamLogWriter.getLogSegmentSequenceNumber()), false));
                blplm.close();
            } else {
                writer.markEndOfStream();
                BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, DistributedLogConstants.MAX_TXID,
                                                                    perStreamLogWriter.getLogSegmentSequenceNumber()), false));
                blplm.close();
            }
        }
        return txid;
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStream() throws Exception {
        String name = "distrlog-mark-end-of-stream";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        txid = writeAndMarkEndOfStream(dlm, txid);

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        boolean exceptionEncountered = false;
        LogRecord record = null;
        try {
            record = reader.readNext(false);
            long expectedTxId = 1;
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assertEquals(expectedTxId, record.getTransactionId());
                expectedTxId++;
                numTrans++;
                record = reader.readNext(false);
            }
        } catch (EndOfStreamException exc) {
            LOG.info("Encountered EndOfStream on reading records after {}", record);
            exceptionEncountered = true;
        }
        assertEquals((txid - 1), numTrans);
        assertTrue(exceptionEncountered);
        exceptionEncountered = false;
        try {
            reader.readNext(false);
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        assertTrue(exceptionEncountered);
        reader.close();
    }

    @Test(timeout = 60000)
    public void testWriteFailsAfterMarkEndOfStream() throws Exception {
        String name = "distrlog-mark-end-failure";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        txid = writeAndMarkEndOfStream(dlm, txid);

        assertEquals(txid - 1, dlm.getLastTxId());
        LogRecord last = dlm.getLastLogRecord();
        assertEquals(txid - 1, last.getTransactionId());
        DLMTestUtil.verifyLogRecord(last);
        assert(dlm.isEndOfStreamMarked());

        LogWriter writer = null;
        boolean exceptionEncountered = false;
        try {
            writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        writer.close();
        assert(exceptionEncountered);
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStreamOnEmptyStream() throws Exception {
        markEndOfStreamOnEmptyLogSegment(0);
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStreamOnClosedStream() throws Exception {
        markEndOfStreamOnEmptyLogSegment(3);
    }

    private void markEndOfStreamOnEmptyLogSegment(int numCompletedSegments) throws Exception {
        String name = "distrlog-mark-end-empty-" + numCompletedSegments;

        DistributedLogManager dlm = createNewDLM(conf, name);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, numCompletedSegments, DEFAULT_SEGMENT_SIZE);

        BKSyncLogWriter writer = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        writer.markEndOfStream();

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        boolean exceptionEncountered = false;
        try {
            LogRecord record = reader.readNext(false);
            long lastTxId = -1;
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numTrans++;
                record = reader.readNext(false);
            }
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        assertEquals(numCompletedSegments * DEFAULT_SEGMENT_SIZE, numTrans);
        assertTrue(exceptionEncountered);
        exceptionEncountered = false;
        try {
            reader.readNext(false);
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        assertTrue(exceptionEncountered);
        reader.close();
    }

    @Test(timeout = 60000)
    public void testMaxLogRecSize() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients =
                createNewBKDLM(conf, "distrlog-maxlogRecSize");
        long txid = 1;
        BKLogSegmentWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
        boolean exceptionEncountered = false;
        try {
            LogRecord op = new LogRecord(txid, DLMTestUtil.repeatString(
                                DLMTestUtil.repeatString("abcdefgh", 256), 512).getBytes());
            out.write(op);
        } catch (LogRecordTooLongException exc) {
            exceptionEncountered = true;
        } finally {
            out.close();
        }
        bkdlmAndClients.close();
        assert(exceptionEncountered);
    }

    @Test(timeout = 60000)
    public void testMaxTransmissionSize() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(1024 * 1024);
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients =
                createNewBKDLM(confLocal, "distrlog-transmissionSize");
        long txid = 1;
        BKLogSegmentWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
        boolean exceptionEncountered = false;
        byte[] largePayload = DLMTestUtil.repeatString(DLMTestUtil.repeatString("abcdefgh", 256), 256).getBytes();
        try {
            while (txid < 3) {
                LogRecord op = new LogRecord(txid, largePayload);
                out.write(op);
                txid++;
            }
        } catch (LogRecordTooLongException exc) {
            exceptionEncountered = true;
        } finally {
            out.close();
        }
        bkdlmAndClients.close();
        assert(!exceptionEncountered);
    }

    @Test(timeout = 60000)
    public void deleteDuringRead() throws Exception {
        String name = "distrlog-delete-with-reader";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter writer = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter();

            writer.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                                perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            blplm.close();
        }

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 1;
        LogRecord record = reader.readNext(false);
        assert (null != record);
        DLMTestUtil.verifyLogRecord(record);
        long lastTxId = record.getTransactionId();

        dlm.delete();

        boolean exceptionEncountered = false;
        try {
            record = reader.readNext(false);
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numTrans++;
                record = reader.readNext(false);
            }
            // make sure the exception is thrown from readahead
            while (true) {
                reader.readNext(false);
            }
        } catch (LogReadException readexc) {
            exceptionEncountered = true;
        } catch (LogNotFoundException exc) {
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        reader.close();
    }

    @Test(timeout = 60000)
    public void testImmediateFlush() throws Exception {
        String name = "distrlog-immediate-flush";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(0);
        testNonPartitionedWritesInternal(name, confLocal);
    }

    @Test(timeout = 60000)
    public void testLastLogRecordWithEmptyLedgers() throws Exception {
        String name = "distrlog-lastLogRec-emptyledgers";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            BKLogSegmentWriter writer = blplm.startLogSegment(txid - 1);
            blplm.completeAndCloseLogSegment(writer.getLogSegmentSequenceNumber(),
                    writer.getLedgerHandle().getId(), txid - 1, txid - 1, 0);
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(txid - 1, txid - 1,
                                                      writer.getLogSegmentSequenceNumber()), false));
            blplm.close();
        }

        BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        LogRecord op = DLMTestUtil.getLogRecordInstance(txid);
        op.setControl();
        out.write(op);
        out.setReadyToFlush();
        out.flushAndSync();
        out.abort();
        dlm.close();

        dlm = createNewDLM(conf, name);

        assertEquals(txid - 1, dlm.getLastTxId());
        LogRecord last = dlm.getLastLogRecord();
        assertEquals(txid - 1, last.getTransactionId());
        DLMTestUtil.verifyLogRecord(last);
        assertEquals(txid - 1, dlm.getLogRecordCount());

        dlm.close();
    }

    @Test(timeout = 60000)
    public void testLogSegmentListener() throws Exception {
        String name = "distrlog-logsegment-listener";
        int numSegments = 3;
        final CountDownLatch[] latches = new CountDownLatch[numSegments + 1];
        for (int i = 0; i < numSegments + 1; i++) {
            latches[i] = new CountDownLatch(1);
        }

        final AtomicInteger numFailures = new AtomicInteger(0);
        final AtomicReference<Collection<LogSegmentMetadata>> receivedStreams =
                new AtomicReference<Collection<LogSegmentMetadata>>();

        DistributedLogManager dlm = createNewDLM(conf, name);
        ZooKeeperClient zkClient = ZooKeeperClientBuilder.newBuilder()
                .uri(createDLMURI("/"))
                .sessionTimeoutMs(10000)
                .zkAclId(null)
                .build();

        BKDistributedLogManager.createLog(conf, zkClient, ((BKDistributedLogManager) dlm).uri, name);
        dlm.registerListener(new LogSegmentListener() {
            @Override
            public void onSegmentsUpdated(List<LogSegmentMetadata> segments) {
                int updates = segments.size();
                boolean hasIncompletedLogSegments = false;
                for (LogSegmentMetadata l : segments) {
                    if (l.isInProgress()) {
                        hasIncompletedLogSegments = true;
                        break;
                    }
                }
                if (hasIncompletedLogSegments) {
                    return;
                }
                if (updates >= 1) {
                    if (segments.get(0).getLogSegmentSequenceNumber() != updates) {
                        numFailures.incrementAndGet();
                    }
                }
                receivedStreams.set(segments);
                latches[updates].countDown();
            }
        });
        LOG.info("Registered listener for stream {}.", name);
        long txid = 1;
        for (int i = 0; i < numSegments; i++) {
            LOG.info("Waiting for creating log segment {}.", i);
            latches[i].await();
            LOG.info("Creating log segment {}.", i);
            BKSyncLogWriter out = (BKSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            LOG.info("Created log segment {}.", i);
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.closeAndComplete();
            LOG.info("Completed log segment {}.", i);
        }
        latches[numSegments].await();
        assertEquals(0, numFailures.get());
        assertNotNull(receivedStreams.get());
        assertEquals(numSegments, receivedStreams.get().size());
        int seqno = numSegments;
        for (LogSegmentMetadata m : receivedStreams.get()) {
            assertEquals(seqno, m.getLogSegmentSequenceNumber());
            assertEquals((seqno - 1) * DEFAULT_SEGMENT_SIZE + 1, m.getFirstTxId());
            assertEquals(seqno * DEFAULT_SEGMENT_SIZE, m.getLastTxId());
            --seqno;
        }
    }

    @Test(timeout = 60000)
    public void testGetLastDLSN() throws Exception {
        String name = "distrlog-get-last-dlsn";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setFirstNumEntriesPerReadLastRecordScan(2);
        confLocal.setMaxNumEntriesPerReadLastRecordScan(4);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        long txid = 1;
        for (int i = 0; i < 10; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(txid++);
            record.setControl();
            Await.result(writer.writeControlRecord(record));
        }

        try {
            dlm.getLastDLSN();
            fail("Should fail on getting last dlsn from an empty log.");
        } catch (LogEmptyException lee) {
            // expected
        }

        writer.closeAndComplete();

        writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));

        for (int i = 1; i < 10; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(txid++);
            record.setControl();
            Await.result(writer.write(record));
        }

        assertEquals(new DLSN(2, 0, 0), dlm.getLastDLSN());

        writer.closeAndComplete();

        assertEquals(new DLSN(2, 0, 0), dlm.getLastDLSN());

        writer.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountAsync() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, testNames.getMethodName());
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 2, 10);

        Future<Long> futureCount = dlm.getLogRecordCountAsync(DLSN.InitialDLSN);
        Long count = Await.result(futureCount, Duration.fromSeconds(2));
        assertEquals(20, count.longValue());

        writer.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testInvalidStreamFromInvalidZkPath() throws Exception {
        String baseName = testNames.getMethodName();
        String streamName = "\0blah";
        URI uri = createDLMURI("/" + baseName);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();

        DistributedLogManager dlm = null;
        AsyncLogWriter writer = null;
        try {
            dlm = namespace.openLog(streamName);
            writer = dlm.startAsyncLogSegmentNonPartitioned();
            fail("should have thrown");
        } catch (InvalidStreamNameException e) {
        } finally {
            if (null != writer) {
                writer.close();
            }
            if (null != dlm) {
                dlm.close();
            }
            namespace.close();
        }
    }

    @Test(timeout = 60000)
    public void testTruncationValidation() throws Exception {
        String name = "distrlog-truncation-validation";
        URI uri = createDLMURI("/" + name);
        ZooKeeperClient zookeeperClient = ZooKeeperClientBuilder.newBuilder()
            .uri(uri)
            .zkAclId(null)
            .sessionTimeoutMs(10000).build();
        OrderedScheduler scheduler = OrderedScheduler.newBuilder()
                .name("test-truncation-validation")
                .corePoolSize(1)
                .build();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setDLLedgerMetadataLayoutVersion(LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
        confLocal.setOutputBufferSize(0);

        LogSegmentMetadataStore metadataStore = new ZKLogSegmentMetadataStore(confLocal, zookeeperClient, scheduler);

        BKDistributedLogManager dlm = createNewDLM(confLocal, name);
        DLSN truncDLSN = DLSN.InitialDLSN;
        DLSN beyondTruncDLSN = DLSN.InitialDLSN;
        long beyondTruncTxId = 1;
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (long j = 1; j <= 10; j++) {
                LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                Future<DLSN> dlsn = writer.write(record);

                if (i == 1 && j == 2) {
                    truncDLSN = Await.result(dlsn);
                } else if (i == 2 && j == 3) {
                    beyondTruncDLSN = Await.result(dlsn);
                    beyondTruncTxId = record.getTransactionId();
                } else if (j == 10) {
                    Await.ready(dlsn);
                }
            }

            writer.close();
        }

        {
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue((record != null) && (record.getDlsn().compareTo(DLSN.InitialDLSN) == 0));
            reader.close();
        }

        Map<Long, LogSegmentMetadata> segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                ZKLogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments before truncating first segment : {}", segmentList);

        MetadataUpdater updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(
                confLocal, metadataStore);
        FutureUtils.result(updater.setLogSegmentTruncated(segmentList.get(1L)));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                ZKLogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments after truncated first segment : {}", segmentList);

        {
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue((record != null) && (record.getDlsn().compareTo(new DLSN(2, 0, 0)) == 0));
            reader.close();
        }

        {
            LogReader reader = dlm.getInputStream(1);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue((record != null) && (record.getDlsn().compareTo(new DLSN(2, 0, 0)) == 0));
            reader.close();
        }

        updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(confLocal, metadataStore);
        FutureUtils.result(updater.setLogSegmentActive(segmentList.get(1L)));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                ZKLogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments after marked first segment as active : {}", segmentList);

        updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(confLocal, metadataStore);
        FutureUtils.result(updater.setLogSegmentTruncated(segmentList.get(2L)));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                ZKLogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments after truncated second segment : {}", segmentList);

        {
            AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
            long expectedTxId = 1L;
            boolean exceptionEncountered = false;
            try {
                for (int i = 0; i < 3 * 10; i++) {
                    LogRecordWithDLSN record = Await.result(reader.readNext());
                    DLMTestUtil.verifyLargeLogRecord(record);
                    assertEquals(expectedTxId, record.getTransactionId());
                    expectedTxId++;
                }
            } catch (AlreadyTruncatedTransactionException exc) {
                exceptionEncountered = true;
            }
            assertTrue(exceptionEncountered);
            reader.close();
        }

        updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(conf, metadataStore);
        FutureUtils.result(updater.setLogSegmentActive(segmentList.get(2L)));

        BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        Assert.assertTrue(Await.result(writer.truncate(truncDLSN)));
        BKLogWriteHandler handler = writer.getCachedWriteHandler();
        List<LogSegmentMetadata> cachedSegments = handler.getFullLedgerList(false, false);
        for (LogSegmentMetadata segment: cachedSegments) {
            if (segment.getLastDLSN().compareTo(truncDLSN) < 0) {
                Assert.assertTrue(segment.isTruncated());
                Assert.assertTrue(!segment.isPartiallyTruncated());
            } else if (segment.getFirstDLSN().compareTo(truncDLSN) < 0) {
                Assert.assertTrue(!segment.isTruncated());
                Assert.assertTrue(segment.isPartiallyTruncated());
            } else {
                Assert.assertTrue(!segment.isTruncated());
                Assert.assertTrue(!segment.isPartiallyTruncated());
            }
        }

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                ZKLogMetadata.getLogSegmentsPath(uri, name, conf.getUnpartitionedStreamName()));

        Assert.assertTrue(segmentList.get(truncDLSN.getLogSegmentSequenceNo()).getMinActiveDLSN().compareTo(truncDLSN) == 0);

        {
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(truncDLSN, record.getDlsn());
            reader.close();
        }

        {
            LogReader reader = dlm.getInputStream(1);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(truncDLSN, record.getDlsn());
            reader.close();
        }

        {
            AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
            LogRecordWithDLSN record = Await.result(reader.readNext());
            assertTrue(record != null);
            assertEquals(truncDLSN, record.getDlsn());
            reader.close();
        }


        {
            LogReader reader = dlm.getInputStream(beyondTruncDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(beyondTruncDLSN, record.getDlsn());
            reader.close();
        }

        {
            LogReader reader = dlm.getInputStream(beyondTruncTxId);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(beyondTruncDLSN, record.getDlsn());
            assertEquals(beyondTruncTxId, record.getTransactionId());
            reader.close();
        }

        {
            AsyncLogReader reader = dlm.getAsyncLogReader(beyondTruncDLSN);
            LogRecordWithDLSN record = Await.result(reader.readNext());
            assertTrue(record != null);
            assertEquals(beyondTruncDLSN, record.getDlsn());
            reader.close();
        }


        zookeeperClient.close();
    }
}
