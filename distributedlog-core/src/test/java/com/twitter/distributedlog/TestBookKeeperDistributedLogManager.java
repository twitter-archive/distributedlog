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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.subscription.SubscriptionStateStore;
import com.twitter.distributedlog.subscription.SubscriptionsStore;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestBookKeeperDistributedLogManager extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestBookKeeperDistributedLogManager.class);

    @Rule
    public TestName testNames = new TestName();

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    private void testNonPartitionedWritesInternal(String name, DistributedLogConfiguration conf) throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter(conf.getUnpartitionedStreamName());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                                perStreamLogWriter.getLedgerSequenceNumber()), false));
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

    @Test
    public void testSimpleWrite() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, "distrlog-simplewrite");
        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter(conf.getUnpartitionedStreamName());
        out.closeAndComplete();

        BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
        assertNotNull(zkc.exists(blplm.completedLedgerZNode(1, 100,
                                                            perStreamLogWriter.getLedgerSequenceNumber()), false));
        blplm.close();
    }

    @Test
    public void testNumberOfTransactions() throws Exception {
        String name = "distrlog-txncount";
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        long numTrans = DLMTestUtil.getNumberofLogRecords(createNewDLM(conf, name), 1);
        assertEquals(100, numTrans);
        dlm.close();
    }

    @Test
    public void testSanityCheckTxnID() throws Exception {
        String name = "distrlog-sanity-check-txnid";
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        long txid = 1;
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        BKUnPartitionedSyncLogWriter out1 = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
        DistributedLogManager newDLM = createNewDLM(newConf, name);
        BKUnPartitionedSyncLogWriter out2 = (BKUnPartitionedSyncLogWriter) newDLM.startLogSegmentNonPartitioned();
        LogRecord op2 = DLMTestUtil.getLogRecordInstance(1);
        out2.write(op2);
        out2.closeAndComplete();
        newDLM.close();

        DLMTestUtil.updateBKDLConfig(bkutil.getUri(), bkutil.getZkServers(), bkutil.getBkLedgerPath(), true);
        LOG.info("Enable sanity check txn id.");
        BKDLConfig.clearCachedDLConfigs();

        DistributedLogConfiguration conf3 = new DistributedLogConfiguration();
        conf3.addConfiguration(conf);
        DistributedLogManager dlm3 = createNewDLM(newConf, name);
        BKUnPartitionedSyncLogWriter out3 = (BKUnPartitionedSyncLogWriter)dlm3.startLogSegmentNonPartitioned();
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

    @Test
    public void testContinuousReaders() throws Exception {
        String name = "distrlog-continuous";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter(conf.getUnpartitionedStreamName());
            out.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLedgerSequenceNumber()), false));
            blplm.close();
        }

        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
    @Test
    public void testWriteRestartFrom1() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, "distrlog-restartFrom1");
        long txid = 1;
        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        txid = 1;
        try {
            out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
            out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
        out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        assertNotNull(out);

        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        // open journal arbitarily far in the future
        txid = DEFAULT_SEGMENT_SIZE * 4;
        out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        out.write(DLMTestUtil.getLogRecordInstance(txid));
        out.close();
        dlm.close();
    }

    @Test
    public void testTwoWriters() throws Exception {
        long start = 1;
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlm1 = createNewBKDLM(conf, "distrlog-dualWriter");
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlm2 = createNewBKDLM(conf, "distrlog-dualWriter");

        bkdlm1.getWriteHandler().startLogSegment(start);
        try {
            bkdlm2.getWriteHandler().startLogSegment(start);
            fail("Shouldn't have been able to open the second writer");
        } catch (OwnershipAcquireFailedException ioe) {
            assertEquals(ioe.getCurrentOwner(), "localhost");
        }

        bkdlm1.close();
        bkdlm2.close();
    }

    @Test
    public void testSimpleRead() throws Exception {
        String name = "distrlog-simpleread";
        DistributedLogManager dlm = createNewDLM(conf, name);
        final long numTransactions = 10000;
        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= numTransactions; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        assertEquals(numTransactions, DLMTestUtil.getNumberofLogRecords(createNewDLM(conf, name), 1));
        dlm.close();
    }

    @Test
    public void testNumberOfTransactionsWithInprogressAtEnd() throws Exception {
        String name = "distrlog-inprogressAtEnd";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter(conf.getUnpartitionedStreamName());
            out.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLedgerSequenceNumber()), false));
            blplm.close();
        }
        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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

    @Test
    public void testPartitionedWrites() throws Exception {
        String name = "distrlog-partitioned";
        DistributedLogManager dlm = createNewDLM(conf, name);

        PartitionId pid0 = new PartitionId(0);
        PartitionId pid1 = new PartitionId(1);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKPartitionAwareLogWriter writer = (BKPartitionAwareLogWriter)dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), pid1);
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), pid0);
            }
            BKLogSegmentWriter writer1 = writer.getCachedLogWriter(pid1.toString());
            BKLogSegmentWriter writer0 = writer.getCachedLogWriter(pid0.toString());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid1);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2,
                                                                 writer1.getLedgerSequenceNumber()), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid0);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1,
                                                                 writer0.getLedgerSequenceNumber()), false));
            blplm2.close();
        }

        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        assertEquals(2, dlm.getFirstTxId(new PartitionId(0)));
        assertEquals(txid - 1, dlm.getLastTxId(new PartitionId(0)));
        assertEquals(txid - 1, dlm.getLastTxIdAsync(new PartitionId(0)).get().longValue());
        assertEquals(1, dlm.getFirstTxId(new PartitionId(1)));
        assertEquals(txid - 2, dlm.getLastTxId(new PartitionId(1)));
        assertEquals(txid - 2, dlm.getLastTxIdAsync(new PartitionId(1)).get().longValue());

        LogReader reader = dlm.getInputStream(new PartitionId(0), 2);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        reader = dlm.getInputStream(new PartitionId(1), 1);
        record = reader.readNext(false);
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
    }

    @Test
    public void testPartitionedWritesBulk() throws Exception {
        String name = "distrlog-partitioned-bulk";
        DistributedLogManager dlm = createNewDLM(conf, name);

        PartitionId pid0 = new PartitionId(0);
        PartitionId pid1 = new PartitionId(1);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKPartitionAwareLogWriter writer = (BKPartitionAwareLogWriter)dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(pid1, part1);
                mapRecords.put(pid0, part0);
                writer.writeBulk(mapRecords);
            }

            BKLogSegmentWriter writer1 = writer.getCachedLogWriter(pid1.toString());
            BKLogSegmentWriter writer0 = writer.getCachedLogWriter(pid0.toString());

            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid1);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2,
                                                                 writer1.getLedgerSequenceNumber()), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid0);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1,
                                                                 writer0.getLedgerSequenceNumber()), false));
            blplm2.close();
        }

        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        LogReader reader = dlm.getInputStream(new PartitionId(0), 2);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);

        }
        reader.close();
        reader = dlm.getInputStream(new PartitionId(1), 1);
        record = reader.readNext(false);
        lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
        dlm.close();
    }

    @Test
    public void testPartitionedWritesBulkSeparateReader() throws Exception {
        String name = "distrlog-partitioned-bulk-separate-reader";
        DistributedLogManager dlm = createNewDLM(conf, name);

        PartitionId pid0 = new PartitionId(0);
        PartitionId pid1 = new PartitionId(1);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKPartitionAwareLogWriter writer = (BKPartitionAwareLogWriter)dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(pid1, part1);
                mapRecords.put(pid0, part0);
                writer.writeBulk(mapRecords);
            }
            BKLogSegmentWriter writer1 = writer.getCachedLogWriter(pid1.toString());
            BKLogSegmentWriter writer0 = writer.getCachedLogWriter(pid0.toString());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid1);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2,
                                                                 writer1.getLedgerSequenceNumber()), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid0);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1,
                                                                 writer0.getLedgerSequenceNumber()), false));
            blplm2.close();
        }

        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();
        dlm.close();

        dlm = createNewDLM(conf, name);
        LogReader reader = dlm.getInputStream(new PartitionId(0), 2);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);

        }
        reader.close();
        reader = dlm.getInputStream(new PartitionId(1), 1);
        record = reader.readNext(false);
        lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid-1), numTrans);
        dlm.close();
    }

    @Test
    public void testPartitionedWritesBulkSeparateReaderWriterOpen() throws Exception {
        String name = "distrlog-partitioned-bulk-separate-reader-writer-open";
        DistributedLogManager dlm = createNewDLM(conf, name);

        PartitionId pid0 = new PartitionId(0);
        PartitionId pid1 = new PartitionId(1);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKPartitionAwareLogWriter writer = (BKPartitionAwareLogWriter)dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(pid1, part1);
                mapRecords.put(pid0, part0);
                writer.writeBulk(mapRecords);
            }
            BKLogSegmentWriter writer1 = writer.getCachedLogWriter(pid1.toString());
            BKLogSegmentWriter writer0 = writer.getCachedLogWriter(pid0.toString());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid1);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2,
                                                                 writer1.getLedgerSequenceNumber()), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid0);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1,
                                                                 writer0.getLedgerSequenceNumber()), false));
            blplm2.close();
        }

        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        DistributedLogManager dlm2 = createNewDLM(conf, name);
        LogReader reader = dlm2.getInputStream(new PartitionId(0), 2);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);

        }
        reader.close();
        reader = dlm2.getInputStream(new PartitionId(1), 1);
        record = reader.readNext(false);
        lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
        dlm.close();
        dlm2.close();
    }


    @Test
    public void testContinuousReaderBulk() throws Exception {
        String name = "distrlog-continuous-bulk";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.closeAndComplete();
        }

        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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

    @Test
    public void testContinuousReadersWithEmptyLedgers() throws Exception {
        String name = "distrlog-continuous-emptyledgers";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter writer = out.getCachedLogWriter(conf.getUnpartitionedStreamName());
            out.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      writer.getLedgerSequenceNumber()), false));
            BKLogSegmentWriter perStreamLogWriter = blplm.startLogSegment(txid - 1);
            blplm.completeAndCloseLogSegment(perStreamLogWriter.getLedgerSequenceNumber(),
                    perStreamLogWriter.getLedgerHandle().getId(), txid - 1, txid - 1, 0);
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(txid - 1, txid - 1,
                                                      perStreamLogWriter.getLedgerSequenceNumber()), false));
            blplm.close();
        }

        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
        LogRecordWithDLSN record = asyncreader.readNext().get();
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            if (numTrans >= (txid - 1)) {
                break;
            }
            record = asyncreader.readNext().get();
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

    @Test
    public void deletePartitionsTest() throws Exception {
        String name = "distrlog-deletepartitions";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);

        long txid = 701;
        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
        }
        writer.close();
        dlmwrite.close();

        DistributedLogManager dlmdelete = createNewDLM(conf, name);
        dlmdelete.deletePartition(new PartitionId(0));
        dlmdelete.deletePartition(new PartitionId(1));

    }

    @Test
    public void deleteLogTest() throws Exception {
        String name = "distrlog-deletelog";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);

        long txid = 701;
        long numTrans;
        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
        }
        writer.close();
        dlmwrite.close();

        DistributedLogManager dlmdelete = createNewDLM(conf, name);

        dlmdelete.delete();
        dlmdelete.close();

        URI uri = createDLMURI("/" + name);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();
        assertFalse(namespace.logExists(name));

        DistributedLogManager dlmwrite2 = createNewDLM(conf, name);

        DistributedLogManager dlmreader = createNewDLM(conf, name);
        LogReader reader0 = dlmwrite2.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmwrite2.getInputStream(new PartitionId(1), 1);

        txid = 701;
        numTrans = txid - 1;
        writer = dlmwrite2.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
        }
        writer.close();

        assertTrue(namespace.logExists(name));

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
        assertEquals((txid - 1), numTrans);
        reader0.close();
        reader1.close();
        dlmwrite2.close();
        dlmreader.close();
    }

    @Test
    public void testPartitionedWritesBulkOutputBufferSize() throws Exception {
        String name = "distrlog-partitioned-bulk-buffer-size";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(128);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        PartitionId pid0 = new PartitionId(0);
        PartitionId pid1 = new PartitionId(1);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKPartitionAwareLogWriter writer = (BKPartitionAwareLogWriter)dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(pid1, part1);
                mapRecords.put(pid0, part0);
                writer.writeBulk(mapRecords);
            }
            BKLogSegmentWriter writer1 = writer.getCachedLogWriter(pid1.toString());
            BKLogSegmentWriter writer0 = writer.getCachedLogWriter(pid0.toString());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid1);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2,
                                                                 writer1.getLedgerSequenceNumber()), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid0);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1,
                                                                 writer0.getLedgerSequenceNumber()), false));
            blplm2.close();
        }

        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        LogReader reader = dlm.getInputStream(new PartitionId(0), 2);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);

        }
        reader.close();
        reader = dlm.getInputStream(new PartitionId(1), 1);
        record = reader.readNext(false);
        lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals((txid - 1), numTrans);
        reader.close();
        dlm.close();
    }

    @Test
    public void testNonDefaultSeparateBKSetting() throws Exception {
        String name = "distrlog-separate-bk-setting";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        PartitionId pid0 = new PartitionId(0);
        PartitionId pid1 = new PartitionId(1);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKPartitionAwareLogWriter writer = (BKPartitionAwareLogWriter)dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(pid1, part1);
                mapRecords.put(pid0, part0);
                writer.writeBulk(mapRecords);
            }
            BKLogSegmentWriter writer1 = writer.getCachedLogWriter(pid1.toString());
            BKLogSegmentWriter writer0 = writer.getCachedLogWriter(pid0.toString());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid1);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2,
                                                                 writer1.getLedgerSequenceNumber()), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid0);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1,
                                                                 writer0.getLedgerSequenceNumber()), false));
            blplm2.close();
        }

        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        LogReader reader = dlm.getInputStream(new PartitionId(0), 2);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);

        }
        reader.close();
        reader = dlm.getInputStream(new PartitionId(1), 1);
        record = reader.readNext(false);
        lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
    }

    @Test
    public void testGetTxId() throws Exception {
        String name = "distrlog-getTxId";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);

        DistributedLogManager dlmreader = createNewDLM(conf, name);

        try {
            dlmreader.getTxIdNotLaterThan(new PartitionId(0), 70L);
            assert (false);
        } catch (LogNotFoundException exc) {
            // expected
        }
        try {
            dlmreader.getTxIdNotLaterThan(new PartitionId(1), 70L);
            assert (false);
        } catch (LogNotFoundException exc) {
            // expected
        }

        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);

        long txid = 701;
        long numTrans = txid - 1;
        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 2; j++) {
            for (int k = 1; k <= 6; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
            }
            writer.setReadyToFlush();
            writer.flushAndSync();

            assertEquals((txid - 1), dlmreader.getTxIdNotLaterThan(new PartitionId(0), txid));
            assertEquals((txid - 4), dlmreader.getTxIdNotLaterThan(new PartitionId(1), txid - 3));

            try {
                dlmreader.getTxIdNotLaterThan(new PartitionId(0), 70L);
                assert (false);
            } catch (AlreadyTruncatedTransactionException exc) {
                // expected
            }
            try {
                dlmreader.getTxIdNotLaterThan(new PartitionId(1), 70L);
                assert (false);
            } catch (AlreadyTruncatedTransactionException exc) {
                // expected
            }

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
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testToggleDefaultReadAhead() throws Exception {
        String name = "distrlog-toggle-readahead";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setEnableReadAhead(!conf.getEnableReadAhead());

        DistributedLogManager dlmwrite = createNewDLM(confLocal, name);
        DistributedLogManager dlmreader = createNewDLM(confLocal, name);


        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
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
            record = reader0.readNext(false);
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
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testReadAheadMaxEntries() throws Exception {
        String name = "distrlog-maxentries-readahead";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadLACOption(0);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setRetentionPeriodHours(24 * 7);

        DistributedLogManager dlmwrite = createNewDLM(confLocal, name);
        DistributedLogManager dlmreader = createNewDLM(confLocal, name);


        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
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
            record = reader0.readNext(false);
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
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testFlushedTxId() throws Exception {
        String name = "distrlog-flushed-txId";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 1);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 1);
        long txid = 1;
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
        assertEquals((txid - 1), numTrans);
        long syncFlushTxId = 2;

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 2; j++) {
            for (int k = 1; k <= 6; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
            }
            long lastAcked = writer.setReadyToFlush();
            assertEquals(lastAcked, syncFlushTxId);
            syncFlushTxId = writer.flushAndSync();
            assertEquals(syncFlushTxId, txid - 1);

            record = reader0.readNext(false);
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
            assertEquals((txid - 1), numTrans);
        }
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testPartitionedWritesBulkFlush() throws Exception {
        String name = "distrlog-partitioned-bulk-flush";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(64);
        DistributedLogManager dlm = createNewDLM(confLocal, name);

        PartitionId pid0 = new PartitionId(0);
        PartitionId pid1 = new PartitionId(1);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKPartitionAwareLogWriter writer = (BKPartitionAwareLogWriter)dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(pid1, part1);
                mapRecords.put(pid0, part0);
                writer.writeBulk(mapRecords);
            }
            BKLogSegmentWriter writer1 = writer.getCachedLogWriter(pid1.toString());
            BKLogSegmentWriter writer0 = writer.getCachedLogWriter(pid0.toString());
            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid1);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2,
                                                                 writer1.getLedgerSequenceNumber()), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(pid0);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1,
                                                                 writer0.getLedgerSequenceNumber()), false));
            blplm2.close();
        }

        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync(false, true);

        LogReader reader = dlm.getInputStream(new PartitionId(0), 2);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);

        }
        reader.close();
        reader = dlm.getInputStream(new PartitionId(1), 1);
        record = reader.readNext(false);
        lastTxId = -1;
        while (null != record) {
            assert ((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
    }

    @Test
    public void testNonPartitionedWrites() throws Exception {
        String name = "distrlog-non-partitioned-bulk";
        testNonPartitionedWritesInternal(name, conf);
    }

    @Test
    public void testCheckLogExists() throws Exception {
        String name = "distrlog-check-log-exists";
        DistributedLogManager dlm = createNewDLM(conf, name);

        dlm.createOrUpdateMetadata(name.getBytes());
        long txid = 1;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();
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

    @Test
    public void positionReader() throws Exception {
        String name = "distrlog-position-reader";
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        long txid = 1;
        long numTrans = txid - 1;
        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1 ; j <= 4; j++) {
            for (int k = 1; k <= 6; k++ ) {
                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLargeLogRecordInstance(txid++), new PartitionId(0));
            }
            writer.setReadyToFlush();
            writer.flushAndSync();
        }
        LogReader reader0 = dlmreader.getInputStream(new PartitionId(0), 9);
        LogReader reader1 = dlmreader.getInputStream(new PartitionId(1), 9);

        LogRecord record = reader0.readNext(false);
        while (null != record) {
            assert((record.getTransactionId() % 2 == 0));
            DLMTestUtil.verifyLargeLogRecord(record);
            numTrans++;
            record = reader0.readNext(false);
        }
        record = reader1.readNext(false);
        while (null != record) {
            assert((record.getTransactionId() % 2 == 1));
            DLMTestUtil.verifyLargeLogRecord(record);
            numTrans++;
            record = reader1.readNext(false);
        }
        assertEquals((txid - 9), numTrans);
        assertEquals(txid - 1,
            dlmreader.getLogRecordCount(new PartitionId(0)) + dlmreader.getLogRecordCount(new PartitionId(1)));
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testMetadataAccessor() throws Exception {
        String name = "distrlog-metadata-accessor";
        MetadataAccessor metadata = DLMTestUtil.createNewMetadataAccessor(conf, name, createDLMURI("/" + name));
        assertEquals(name, metadata.getStreamName());
        metadata.createOrUpdateMetadata(name.getBytes());
        assertEquals(name, new String(metadata.getMetadata()));
        metadata.deleteMetadata();
        assertEquals(null, metadata.getMetadata());
    }

    @Test
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
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter(conf.getUnpartitionedStreamName());

            if (i < 2) {
                writer.closeAndComplete();
                BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                                    perStreamLogWriter.getLedgerSequenceNumber()), false));
                blplm.close();
            } else {
                writer.markEndOfStream();
                BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, DistributedLogConstants.MAX_TXID,
                                                                    perStreamLogWriter.getLedgerSequenceNumber()), false));
                blplm.close();
            }
        }
        return txid;
    }

    @Test
    public void testMarkEndOfStream() throws Exception {
        String name = "distrlog-mark-end-of-stream";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        txid = writeAndMarkEndOfStream(dlm, txid);

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
        assertEquals((txid - 1), numTrans);
        assert(exceptionEncountered);
        exceptionEncountered = false;
        try {
            reader.readNext(false);
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        reader.close();
    }

    @Test
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

    @Test
    public void testMarkEndOfStreamOnEmptyStream() throws Exception {
        markEndOfStreamOnEmptyLogSegment(0);
    }

    @Test
    public void testMarkEndOfStreamOnClosedStream() throws Exception {
        markEndOfStreamOnEmptyLogSegment(3);
    }

    private void markEndOfStreamOnEmptyLogSegment(int numCompletedSegments) throws Exception {
        String name = "distrlog-mark-end-empty-" + numCompletedSegments;

        DistributedLogManager dlm = createNewDLM(conf, name);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, numCompletedSegments, DEFAULT_SEGMENT_SIZE);

        BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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

    @Test
    public void testMaxLogRecSize() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = createNewBKDLM(conf, "distrlog-maxlogRecSize");
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

    @Test
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

    @Test
    public void deleteDuringRead() throws Exception {
        String name = "distrlog-delete-with-reader";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter(conf.getUnpartitionedStreamName());

            writer.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                                perStreamLogWriter.getLedgerSequenceNumber()), false));
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
        } catch (LogReadException readexc) {
            exceptionEncountered = true;
        } catch (LogNotFoundException exc) {
            exceptionEncountered = true;
        }
        assert(exceptionEncountered);
        reader.close();
    }

    @Test
    public void testImmediateFlush() throws Exception {
        String name = "distrlog-immediate-flush";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(0);
        testNonPartitionedWritesInternal(name, confLocal);
    }

    @Test
    public void testLastLogRecordWithEmptyLedgers() throws Exception {
        String name = "distrlog-lastLogRec-emptyledgers";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter(conf.getUnpartitionedStreamName());
            out.closeAndComplete();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLedgerSequenceNumber()), false));
            BKLogSegmentWriter writer = blplm.startLogSegment(txid - 1);
            blplm.completeAndCloseLogSegment(writer.getLedgerSequenceNumber(),
                    writer.getLedgerHandle().getId(), txid - 1, txid - 1, 0);
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(txid - 1, txid - 1,
                                                      writer.getLedgerSequenceNumber()), false));
            blplm.close();
        }

        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
        final AtomicReference<Collection<LogSegmentLedgerMetadata>> receivedStreams =
                new AtomicReference<Collection<LogSegmentLedgerMetadata>>();

        DistributedLogManager dlm = createNewDLM(conf, name);
        ZooKeeperClient zkClient = ZooKeeperClientBuilder.newBuilder()
                .uri(createDLMURI("/"))
                .sessionTimeoutMs(10000)
                .zkAclId(null)
                .build();

        BKDistributedLogManager.createUnpartitionedStream(conf, zkClient, ((BKDistributedLogManager) dlm).uri, name);
        dlm.registerListener(new LogSegmentListener() {
            @Override
            public void onSegmentsUpdated(List<LogSegmentLedgerMetadata> segments) {
                int updates = segments.size();
                boolean hasIncompletedLogSegments = false;
                for (LogSegmentLedgerMetadata l : segments) {
                    if (l.isInProgress()) {
                        hasIncompletedLogSegments = true;
                        break;
                    }
                }
                if (hasIncompletedLogSegments) {
                    return;
                }
                if (updates >= 1) {
                    if (segments.get(0).getLedgerSequenceNumber() != updates) {
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
            BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter)dlm.startLogSegmentNonPartitioned();
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
        for (LogSegmentLedgerMetadata m : receivedStreams.get()) {
            assertEquals(seqno, m.getLedgerSequenceNumber());
            assertEquals((seqno - 1) * DEFAULT_SEGMENT_SIZE + 1, m.getFirstTxId());
            assertEquals(seqno * DEFAULT_SEGMENT_SIZE, m.getLastTxId());
            --seqno;
        }
    }

    @Test
    public void testGetLastDLSN() throws Exception {
        String name = "distrlog-get-last-dlsn";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setFirstNumEntriesPerReadLastRecordScan(2);
        confLocal.setMaxNumEntriesPerReadLastRecordScan(4);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        long txid = 1;
        for (int i = 0; i < 10; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(txid++);
            record.setControl();
            writer.writeControlRecord(record).get();
        }

        try {
            dlm.getLastDLSN();
            fail("Should fail on getting last dlsn from an empty log.");
        } catch (LogEmptyException lee) {
            // expected
        }

        writer.closeAndComplete();

        writer = (BKUnPartitionedAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(txid++)).get();

        for (int i = 1; i < 10; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(txid++);
            record.setControl();
            writer.write(record).get();
        }

        assertEquals(new DLSN(2, 0, 0), dlm.getLastDLSN());

        writer.closeAndComplete();

        assertEquals(new DLSN(2, 0, 0), dlm.getLastDLSN());

        writer.close();
        dlm.close();
    }

    @Test
    public void testGetLogRecordCountAsync() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, testNames.getMethodName());
        BKUnPartitionedAsyncLogWriter writer = (BKUnPartitionedAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 2, 10);

        Future<Long> futureCount = dlm.getLogRecordCountAsync(DLSN.InitialDLSN);
        Long count = Await.result(futureCount, Duration.fromSeconds(2));
        assertEquals(20, count.longValue());

        writer.close();
        dlm.close();
    }

    @Test
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

    @Test
    public void testTruncationValidation() throws Exception {
        String name = "distrlog-truncation-validation";
        URI uri = createDLMURI("/" + name);
        ZooKeeperClient zookeeperClient = ZooKeeperClientBuilder.newBuilder()
            .uri(uri)
            .zkAclId(null)
            .sessionTimeoutMs(10000).build();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setDLLedgerMetadataLayoutVersion(LogSegmentLedgerMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
        confLocal.setOutputBufferSize(0);

        BKDistributedLogManager dlm = (BKDistributedLogManager)createNewDLM(confLocal, name);
        DLSN truncDLSN = DLSN.InitialDLSN;
        DLSN beyondTruncDLSN = DLSN.InitialDLSN;
        long beyondTruncTxId = 1;
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKUnPartitionedAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
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

        Map<Long, LogSegmentLedgerMetadata> segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
            String.format("%s/ledgers", BKDistributedLogManager.getPartitionPath(uri,
                name, confLocal.getUnpartitionedStreamName())));

        LOG.info("Read segments before truncating first segment : {}", segmentList);

        MetadataUpdater updater = ZkMetadataUpdater.createMetadataUpdater(confLocal, zookeeperClient);
        updater.setLogSegmentTruncated(segmentList.get(1L));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
            String.format("%s/ledgers", BKDistributedLogManager.getPartitionPath(uri,
                name, confLocal.getUnpartitionedStreamName())));

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

        updater = ZkMetadataUpdater.createMetadataUpdater(confLocal, zookeeperClient);
        updater.setLogSegmentActive(segmentList.get(1L));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
            String.format("%s/ledgers", BKDistributedLogManager.getPartitionPath(uri,
                name, confLocal.getUnpartitionedStreamName())));

        LOG.info("Read segments after marked first segment as active : {}", segmentList);

        updater = ZkMetadataUpdater.createMetadataUpdater(confLocal, zookeeperClient);
        updater.setLogSegmentTruncated(segmentList.get(2L));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                String.format("%s/ledgers", BKDistributedLogManager.getPartitionPath(uri,
                        name, confLocal.getUnpartitionedStreamName())));

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

        updater = ZkMetadataUpdater.createMetadataUpdater(conf, zookeeperClient);
        updater.setLogSegmentActive(segmentList.get(2L));

        BKUnPartitionedAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        Assert.assertTrue(Await.result(writer.truncate(truncDLSN)));
        BKLogPartitionWriteHandler handler = writer.getCachedPartitionHandler(conf.getUnpartitionedStreamName());
        List<LogSegmentLedgerMetadata> cachedSegments = handler.getFullLedgerList(false, false);
        for (LogSegmentLedgerMetadata segment: cachedSegments) {
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
            String.format("%s/ledgers", BKDistributedLogManager.getPartitionPath(uri,
                name, conf.getUnpartitionedStreamName())));

        Assert.assertTrue(segmentList.get(truncDLSN.getLedgerSequenceNo()).getMinActiveDLSN().compareTo(truncDLSN) == 0);

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
