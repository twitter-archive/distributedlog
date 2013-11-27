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

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;

import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBookKeeperDistributedLogManager {
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


    private void testNonPartitionedWritesInternal(String name, DistributedLogConfiguration conf) throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            writer.close();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
            blplm.completeAndCloseLogSegment(start, txid - 1);
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1), false));
            blplm.close();
        }

        long start = txid;
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
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-simplewrite");
        long txid = 1;
        LogWriter out = bkdlm.startLogSegment(1);
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.close();
        bkdlm.completeAndCloseLogSegment(1, 100);

        String zkpath = bkdlm.completedLedgerZNode(1, 100);

        assertNotNull(zkc.exists(zkpath, false));
        assertNull(zkc.exists(bkdlm.inprogressZNode(1), false));
        bkdlm.close();
    }

    @Test
    public void testNumberOfTransactions() throws Exception {
        String name = "distrlog-txncount";
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        LogWriter out = bkdlm.startLogSegment(1);
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.close();
        bkdlm.completeAndCloseLogSegment(1, 100);

        long numTrans = DLMTestUtil.getNumberofLogRecords(DLMTestUtil.createNewDLM(conf, name), new PartitionId(0), 1);
        assertEquals(100, numTrans);
        bkdlm.close();
    }

    @Test
    public void testContinuousReaders() throws Exception {
        String name = "distrlog-continuous";
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter out = bkdlm.startLogSegment(start);
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.close();
            bkdlm.completeAndCloseLogSegment(start, txid - 1);
            assertNotNull(
                zkc.exists(bkdlm.completedLedgerZNode(start, txid - 1), false));
        }

        long start = txid;
        LogWriter out = bkdlm.startLogSegment(start);
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.abort();
        out.close();

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(new PartitionId(0), 1);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals((txid - 1), numTrans);
        reader.close();
        bkdlm.close();
        dlm.close();
    }

    /**
     * Create a bkdlm namespace, write a journal from txid 1, close stream.
     * Try to create a new journal from txid 1. Should throw an exception.
     */
    @Test
    public void testWriteRestartFrom1() throws Exception {
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-restartFrom1");
        long txid = 1;
        long start = txid;
        LogWriter out = bkdlm.startLogSegment(txid);
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.close();
        bkdlm.completeAndCloseLogSegment(start, (txid - 1));

        txid = 1;
        try {
            out = bkdlm.startLogSegment(txid);
            fail("Shouldn't be able to start another journal from " + txid
                + " when one already exists");
        } catch (Exception ioe) {
            LOG.info("Caught exception as expected", ioe);
        }

        // test border case
        txid = DEFAULT_SEGMENT_SIZE - 1;
        try {
            out = bkdlm.startLogSegment(txid);
            fail("Shouldn't be able to start another journal from " + txid
                + " when one already exists");
        } catch (TransactionIdOutOfOrderException rste) {
            LOG.info("Caught exception as expected", rste);
        }

        // open journal continuing from before
        txid = DEFAULT_SEGMENT_SIZE + 1;
        start = txid;
        out = bkdlm.startLogSegment(start);
        assertNotNull(out);

        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.close();
        bkdlm.completeAndCloseLogSegment(start, (txid - 1));

        // open journal arbitarily far in the future
        txid = DEFAULT_SEGMENT_SIZE * 4;
        out = bkdlm.startLogSegment(txid);
        assertNotNull(out);
        bkdlm.close();
    }

    @Test
    public void testTwoWriters() throws Exception {
        long start = 1;
        BKLogPartitionWriteHandler bkdlm1 = DLMTestUtil.createNewBKDLM(conf, "distrlog-dualWriter");
        BKLogPartitionWriteHandler bkdlm2 = DLMTestUtil.createNewBKDLM(conf, "distrlog-dualWriter");

        LogWriter out1 = bkdlm1.startLogSegment(start);
        try {
            LogWriter out2 = bkdlm2.startLogSegment(start);
            fail("Shouldn't have been able to open the second writer");
        } catch (OwnershipAcquireFailedException ioe) {
            assertEquals(ioe.getCurrentOwner(),"localhost");
        }
    }

    @Test
    public void testSimpleRead() throws Exception {
        String name = "distrlog-simpleread";
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        final long numTransactions = 10000;
        LogWriter out = bkdlm.startLogSegment(1);
        for (long i = 1; i <= numTransactions; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.close();
        bkdlm.completeAndCloseLogSegment(1, numTransactions);

        assertEquals(numTransactions, DLMTestUtil.getNumberofLogRecords(DLMTestUtil.createNewDLM(conf, name), new PartitionId(0), 1));
        bkdlm.close();
    }

    @Test
    public void testNumberOfTransactionsWithInprogressAtEnd() throws Exception {
        String name = "distrlog-inprogressAtEnd";
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter out = bkdlm.startLogSegment(start);
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }

            out.close();
            bkdlm.completeAndCloseLogSegment(start, (txid - 1));
            assertNotNull(
                zkc.exists(bkdlm.completedLedgerZNode(start, (txid - 1)), false));
        }
        long start = txid;
        LogWriter out = bkdlm.startLogSegment(start);
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.abort();
        out.close();

        long numTrans = DLMTestUtil.getNumberofLogRecords(DLMTestUtil.createNewDLM(conf, name), new PartitionId(0), 1);
        assertEquals((txid - 1), numTrans);
    }

    @Test
    public void testPartitionedWrites() throws Exception {
        String name = "distrlog-partitioned";
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PartitionAwareLogWriter writer = dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
            }
            writer.close();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(1));
            blplm1.completeAndCloseLogSegment(start, txid - 2);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(0));
            blplm2.completeAndCloseLogSegment(start + 1, txid - 1);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1), false));
            blplm2.close();
        }

        long start = txid;
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
        assertEquals(1, dlm.getFirstTxId(new PartitionId(1)));
        assertEquals(txid - 2, dlm.getLastTxId(new PartitionId(1)));

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
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PartitionAwareLogWriter writer = dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(new PartitionId(1), part1);
                mapRecords.put(new PartitionId(0), part0);
                writer.writeBulk(mapRecords);
            }
            writer.close();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(1));
            blplm1.completeAndCloseLogSegment(start, txid - 2);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(0));
            blplm2.completeAndCloseLogSegment(start + 1, txid - 1);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1), false));
            blplm2.close();
        }

        long start = txid;
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
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PartitionAwareLogWriter writer = dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(new PartitionId(1), part1);
                mapRecords.put(new PartitionId(0), part0);
                writer.writeBulk(mapRecords);
            }
            writer.close();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(1));
            blplm1.completeAndCloseLogSegment(start, txid - 2);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(0));
            blplm2.completeAndCloseLogSegment(start + 1, txid - 1);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1), false));
            blplm2.close();
        }

        long start = txid;
        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();
        dlm.close();

        dlm = DLMTestUtil.createNewDLM(conf, name);
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
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PartitionAwareLogWriter writer = dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(new PartitionId(1), part1);
                mapRecords.put(new PartitionId(0), part0);
                writer.writeBulk(mapRecords);
            }
            writer.close();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(1));
            blplm1.completeAndCloseLogSegment(start, txid - 2);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(0));
            blplm2.completeAndCloseLogSegment(start + 1, txid - 1);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1), false));
            blplm2.close();
        }

        long start = txid;
        PartitionAwareLogWriter writer = dlm.startLogSegment();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
            writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
        }
        writer.setReadyToFlush();
        writer.flushAndSync();
        writer.close();

        DistributedLogManager dlm2 = DLMTestUtil.createNewDLM(conf, name);
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
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter out = bkdlm.startLogSegment(start);
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.close();
            bkdlm.completeAndCloseLogSegment(start, txid - 1);
            assertNotNull(
                zkc.exists(bkdlm.completedLedgerZNode(start, txid - 1), false));
        }

        long start = txid;
        LogWriter out = bkdlm.startLogSegment(start);
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.abort();
        out.close();

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(new PartitionId(0), 1);
        long numTrans = 0;
        List<LogRecord> recordList = reader.readBulk(false, 13);
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
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter out = bkdlm.startLogSegment(start);
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.close();
            bkdlm.completeAndCloseLogSegment(start, txid - 1);
            assertNotNull(
                zkc.exists(bkdlm.completedLedgerZNode(start, txid - 1), false));
            bkdlm.startLogSegment(txid - 1);
            bkdlm.completeAndCloseLogSegment(txid - 1, txid - 1);
            assertNotNull(
                zkc.exists(bkdlm.completedLedgerZNode(txid - 1, txid - 1), false));
        }

        long start = txid;
        LogWriter out = bkdlm.startLogSegment(start);
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.abort();
        out.close();

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(new PartitionId(0), 1);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals((txid - 1), numTrans);
        reader.close();
        dlm.close();
    }

    @Test
    public void deletePartitionsTest() throws Exception {
        String name = "distrlog-deletepartitions";
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

        long txid = 701;
        long numTrans = txid - 1;
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

        DistributedLogManager dlmdelete = DLMTestUtil.createNewDLM(conf, name);
        dlmdelete.deletePartition(new PartitionId(0));
        dlmdelete.deletePartition(new PartitionId(1));

    }

    @Test
    public void deleteLogTest() throws Exception {
        String name = "distrlog-deletelog";
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);

        long txid = 701;
        long numTrans = txid - 1;
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

        DistributedLogManager dlmdelete = DLMTestUtil.createNewDLM(conf, name);

        dlmdelete.delete();
        dlmdelete.close();

        assertFalse(DistributedLogManagerFactory.checkIfLogExists(conf, DLMTestUtil.createDLMURI("/" + name), name));

        DistributedLogManager dlmwrite2 = DLMTestUtil.createNewDLM(conf, name);

        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);
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

        assertTrue(DistributedLogManagerFactory.checkIfLogExists(conf, DLMTestUtil.createDLMURI("/" + name), name));

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
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PartitionAwareLogWriter writer = dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(new PartitionId(1), part1);
                mapRecords.put(new PartitionId(0), part0);
                writer.writeBulk(mapRecords);
            }
            writer.close();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(1));
            blplm1.completeAndCloseLogSegment(start, txid - 2);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(0));
            blplm2.completeAndCloseLogSegment(start + 1, txid - 1);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1), false));
            blplm2.close();
        }

        long start = txid;
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
        confLocal.setSeparateZKClients(!conf.getSeparateZKClients());
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PartitionAwareLogWriter writer = dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(new PartitionId(1), part1);
                mapRecords.put(new PartitionId(0), part0);
                writer.writeBulk(mapRecords);
            }
            writer.close();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(1));
            blplm1.completeAndCloseLogSegment(start, txid - 2);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(0));
            blplm2.completeAndCloseLogSegment(start + 1, txid - 1);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1), false));
            blplm2.close();
        }

        long start = txid;
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
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);

        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

        try {
            dlmreader.getTxIdNotLaterThan(new PartitionId(0), 70L);
            assert (false);
        } catch (LogEmptyException exc) {
            // expected
        }
        try {
            dlmreader.getTxIdNotLaterThan(new PartitionId(1), 70L);
            assert (false);
        } catch (LogEmptyException exc) {
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

        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(confLocal, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(confLocal, name);


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
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxEntries(1);
        confLocal.setRetentionPeriodHours(24 * 7);

        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(confLocal, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(confLocal, name);


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
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PartitionAwareLogWriter writer = dlm.startLogSegment();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> part1 = new LinkedList<LogRecord>();
                LinkedList<LogRecord> part0 = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    part1.add(DLMTestUtil.getLogRecordInstance(txid++));
                    part0.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                HashMap<PartitionId, List<LogRecord>> mapRecords = new HashMap<PartitionId, List<LogRecord>>();
                mapRecords.put(new PartitionId(1), part1);
                mapRecords.put(new PartitionId(0), part0);
                writer.writeBulk(mapRecords);
            }
            writer.close();
            BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(1));
            blplm1.completeAndCloseLogSegment(start, txid - 2);
            assertNotNull(zkc.exists(blplm1.completedLedgerZNode(start, txid - 2), false));
            blplm1.close();

            BKLogPartitionWriteHandler blplm2 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(new PartitionId(0));
            blplm2.completeAndCloseLogSegment(start + 1, txid - 1);
            assertNotNull(zkc.exists(blplm2.completedLedgerZNode(start + 1, txid - 1), false));
            blplm2.close();
        }

        long start = txid;
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
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

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

        assert (DistributedLogManagerFactory.checkIfLogExists(conf, DLMTestUtil.createDLMURI("/" + name), name));
        assert (!DistributedLogManagerFactory.checkIfLogExists(conf, DLMTestUtil.createDLMURI("/" + name), "non-existent-log"));
        assert (!DistributedLogManagerFactory.checkIfLogExists(conf, DLMTestUtil.createDLMURI("/" + "non-existent-ns"), name));

        int logCount = 0;
        for(String log: DistributedLogManagerFactory.enumerateAllLogsInNamespace(conf, DLMTestUtil.createDLMURI("/" + name))) {
            logCount++;
            assertEquals(name, log);
        }
        assertEquals(1, logCount);

        for(Map.Entry<String, byte[]> logEntry: DistributedLogManagerFactory.enumerateLogsWithMetadataInNamespace(conf, DLMTestUtil.createDLMURI("/" + name)).entrySet()) {
            assertEquals(name, new String(logEntry.getValue()));
        }

    }

    @Test
    public void positionReader() throws Exception {
        String name = "distrlog-position-reader";
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);

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
        reader0.close();
        reader1.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void appendOnlyStreams() throws Exception {
        String name = "distrlog-append-only-streams";
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 51).getBytes();

        long txid = 1;
        long numTrans = txid - 1;
        AppendOnlyStreamWriter writer = dlmwrite.getAppendOnlyStreamWriter();
        writer.write(DLMTestUtil.repeatString("abc", 11).getBytes());
        writer.write(DLMTestUtil.repeatString("abc", 40).getBytes());
        writer.force(false);
        writer.close();
        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();

        byte[] bytesIn = new byte[byteStream.length];
        int read = reader.read(bytesIn, 0, 23);
        assertEquals(23, read);
        read = reader.read(bytesIn, 23, 31);
        assertEquals(read, 31);
        byte[] bytesInTemp = new byte[byteStream.length];
        read = reader.read(bytesInTemp, 0, byteStream.length);
        assertEquals(read, byteStream.length - 23 - 31);
        read = new ByteArrayInputStream(bytesInTemp).read(bytesIn, 23 + 31, byteStream.length - 23 - 31);
        assertEquals(read, byteStream.length - 23 - 31);
        assertArrayEquals(bytesIn, byteStream);
        reader.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test
    public void testMetadataAccessor() throws Exception {
        String name = "distrlog-metadata-accessor";
        MetadataAccessor metadata = DLMTestUtil.createNewMetadataAccessor(conf, name);
        metadata.createOrUpdateMetadata(name.getBytes());
        assertEquals(name, new String(metadata.getMetadata()));
        metadata.deleteMetadata();
        assertEquals(null, metadata.getMetadata());
    }

    @Test
    public void testMarkEndOfStream() throws Exception {
        String name = "distrlog-mark-end-of-stream";
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            if (i < 2) {
                writer.close();
                BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
                blplm.completeAndCloseLogSegment(start, txid - 1);
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1), false));
                blplm.close();
            } else {
                writer.markEndOfStream();
                BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, DistributedLogConstants.MAX_TXID), false));
                blplm.close();
            }
        }

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
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            if (i < 2) {
                writer.close();
                BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
                blplm.completeAndCloseLogSegment(start, txid - 1);
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1), false));
                blplm.close();
            } else {
                writer.markEndOfStream();
                BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, DistributedLogConstants.MAX_TXID), false));
            }
        }

        assert(dlm.isEndOfStreamMarked());

        long start = txid;
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
    public void testMaxLogRecSize() throws Exception {
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-maxlogRecSize");
        long txid = 1;
        LogWriter out = bkdlm.startLogSegment(1);
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
        bkdlm.close();
        assert(exceptionEncountered);
    }

    @Test
    public void testMaxTransmissionSize() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(1024 * 1024);
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(confLocal, "distrlog-transmissionSize");
        long txid = 1;
        LogWriter out = bkdlm.startLogSegment(1);
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
        bkdlm.close();
        assert(!exceptionEncountered);
    }

    @Test
    public void deleteDuringRead() throws Exception {
        String name = "distrlog-delete-with-reader";
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            writer.close();
            BKLogPartitionWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
            blplm.completeAndCloseLogSegment(start, txid - 1);
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1), false));
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
}
