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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBookKeeperDistributedLogManager {
    static final Log LOG = LogFactory.getLog(TestBookKeeperDistributedLogManager.class);

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    protected static DistributedLogConfiguration conf = new DistributedLogConfiguration().setLockTimeout(10);
    private ZooKeeper zkc;
    private static LocalDLMEmulator bkutil;
    private static LocalBookKeeper.ConnectedZKServer zks;
    static int numBookies = 3;

    @BeforeClass
    public static void setupBookkeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownBookkeeper() throws Exception {
        bkutil.teardown();
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
    public void testSimpleWrite() throws Exception {
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-simplewrite");
        long txid = 1;
        PerStreamLogWriter out = bkdlm.startLogSegment(1);
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
        PerStreamLogWriter out = bkdlm.startLogSegment(1);
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
            PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
        PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
        PerStreamLogWriter out = bkdlm.startLogSegment(txid);
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
        } catch (IOException ioe) {
            LOG.info("Caught exception as expected", ioe);
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

        PerStreamLogWriter out1 = bkdlm1.startLogSegment(start);
        try {
            PerStreamLogWriter out2 = bkdlm2.startLogSegment(start);
            fail("Shouldn't have been able to open the second writer");
        } catch (IOException ioe) {
            LOG.info("Caught exception as expected", ioe);
        }
    }

    @Test
    public void testSimpleRead() throws Exception {
        String name = "distrlog-simpleread";
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        final long numTransactions = 10000;
        PerStreamLogWriter out = bkdlm.startLogSegment(1);
        for (long i = 1; i <= numTransactions; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.close();
        bkdlm.completeAndCloseLogSegment(1, numTransactions);

        assertEquals(numTransactions, DLMTestUtil.getNumberofLogRecords(DLMTestUtil.createNewDLM(conf, name), new PartitionId(0), 1));
        bkdlm.close();
    }

    /**
     * Test that if enough bookies fail to prevent an ensemble,
     * writes the bookkeeper will fail. Test that when once again
     * an ensemble is available, it can continue to write.
     */
    @Test
    public void testAllBookieFailure() throws Exception {
        BookieServer bookieToFail = bkutil.newBookie();
        BookieServer replacementBookie = null;

        try {
            int ensembleSize = numBookies + 1;
            assertEquals("Begin: New bookie didn't start",
                ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));

            // ensure that the journal manager has to use all bookies,
            // so that a failure will fail the journal manager
            DistributedLogConfiguration conf = new DistributedLogConfiguration();
            conf.setEnsembleSize(ensembleSize);
            conf.setQuorumSize(ensembleSize);
            long txid = 1;
            BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-allbookiefailure");
            PerStreamLogWriter out = bkdlm.startLogSegment(txid);

            for (long i = 1; i <= 3; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.setReadyToFlush();
            out.flushAndSync();
            bookieToFail.shutdown();
            assertEquals("New bookie didn't die",
                numBookies, bkutil.checkBookiesUp(numBookies, 10));

            try {
                for (long i = 1; i <= 3; i++) {
                    LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                    out.write(op);
                    txid++;
                }
                out.setReadyToFlush();
                out.flushAndSync();
                fail("should not get to this stage");
            } catch (IOException ioe) {
                LOG.debug("Error writing to bookkeeper", ioe);
                assertTrue("Invalid exception message",
                    ioe.getMessage().contains("Failed to write to bookkeeper"));
            }
            replacementBookie = bkutil.newBookie();

            assertEquals("Replacement: New bookie didn't start",
                numBookies + 1, bkutil.checkBookiesUp(numBookies + 1, 10));
            out = bkdlm.startLogSegment(txid);
            for (long i = 1; i <= 3; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }

            out.setReadyToFlush();
            out.flushAndSync();

        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        } finally {
            if (replacementBookie != null) {
                replacementBookie.shutdown();
            }
            bookieToFail.shutdown();

            if (bkutil.checkBookiesUp(numBookies, 30) != numBookies) {
                LOG.warn("Not all bookies from this test shut down, expect errors");
            }
        }
    }

    /**
     * Test that a BookKeeper JM can continue to work across the
     * failure of a bookie. This should be handled transparently
     * by bookkeeper.
     */
    @Test
    public void testOneBookieFailure() throws Exception {
        BookieServer bookieToFail = bkutil.newBookie();
        BookieServer replacementBookie = null;

        try {
            int ensembleSize = numBookies + 1;
            assertEquals("New bookie didn't start",
                ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));

            // ensure that the journal manager has to use all bookies,
            // so that a failure will fail the journal manager
            DistributedLogConfiguration conf = new DistributedLogConfiguration();
            conf.setEnsembleSize(ensembleSize);
            conf.setQuorumSize(ensembleSize);
            long txid = 1;
            BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-onebookiefailure");
            PerStreamLogWriter out = bkdlm.startLogSegment(txid);
            for (long i = 1; i <= 3; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.setReadyToFlush();
            out.flushAndSync();

            replacementBookie = bkutil.newBookie();
            assertEquals("replacement bookie didn't start",
                ensembleSize + 1, bkutil.checkBookiesUp(ensembleSize + 1, 10));
            bookieToFail.shutdown();
            assertEquals("New bookie didn't die",
                ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));

            for (long i = 1; i <= 3; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.setReadyToFlush();
            out.flushAndSync();
        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        } finally {
            if (replacementBookie != null) {
                replacementBookie.shutdown();
            }
            bookieToFail.shutdown();

            if (bkutil.checkBookiesUp(numBookies, 30) != numBookies) {
                LOG.warn("Not all bookies from this test shut down, expect errors");
            }
        }
    }

    @Test
    public void testNumberOfTransactionsWithInprogressAtEnd() throws Exception {
        String name = "distrlog-inprogressAtEnd";
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
        PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
    public void testSimpleRecovery() throws Exception {
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-simplerecovery");
        PerStreamLogWriter out = bkdlm.startLogSegment(1);
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                out.setReadyToFlush();
                out.flushAndSync();
            }

        }
        out.setReadyToFlush();
        out.flushAndSync();

        out.abort();
        out.close();


        assertNull(zkc.exists(bkdlm.completedLedgerZNode(1, 100), false));
        assertNotNull(zkc.exists(bkdlm.inprogressZNode(1), false));

        bkdlm.recoverIncompleteLogSegments();

        assertNotNull(zkc.exists(bkdlm.completedLedgerZNode(1, 100), false));
        assertNull(zkc.exists(bkdlm.inprogressZNode(1), false));
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
            PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
        PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
    public void testInterleavedReaders() throws Exception {
        String name = "distrlog-interleaved";
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

        PartitionAwareLogWriter writer = dlmwrite.startLogSegment();
        for (long j = 1; j <= 4; j++) {
            for (int k = 1; k <= 10; k++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
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
    public void testContinuousReadersWithEmptyLedgers() throws Exception {
        String name = "distrlog-continuous-emptyledgers";
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
        PerStreamLogWriter out = bkdlm.startLogSegment(start);
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
    public void testRecoveryEmptyLedger() throws Exception {
        BKLogPartitionWriteHandler bkdlm = DLMTestUtil.createNewBKDLM(conf, "distrlog-recovery-empty-ledger");
        PerStreamLogWriter out = bkdlm.startLogSegment(1);
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                out.setReadyToFlush();
                out.flushAndSync();
            }

        }
        out.setReadyToFlush();
        out.flushAndSync();
        out.close();
        bkdlm.completeAndCloseLogSegment(1, 100);
        assertNotNull(zkc.exists(bkdlm.completedLedgerZNode(1, 100), false));
        PerStreamLogWriter outEmpty = bkdlm.startLogSegment(101);
        outEmpty.abort();

        assertNull(zkc.exists(bkdlm.completedLedgerZNode(101, 101), false));
        assertNotNull(zkc.exists(bkdlm.inprogressZNode(101), false));

        bkdlm.recoverIncompleteLogSegments();

        assertNull(zkc.exists(bkdlm.inprogressZNode(101), false));
        assertNotNull(zkc.exists(bkdlm.completedLedgerZNode(101, 101), false));
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
        confLocal.setShareZKClientWithBKC(!conf.getShareZKClientWithBKC());
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
    public void testInterleavedReadersWithRollingEdge() throws Exception {
        String name = "distrlog-interleaved-rolling-edge";
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
    public void testInterleavedReadersWithRolling() throws Exception {
        String name = "distrlog-interleaved-rolling";
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
    public void testInterleavedReadersWithCleanup() throws Exception {
        String name = "distrlog-interleaved-cleanup";
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        long txid = 1;
        int numTrans = 0;
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
                    ((BKPartitionAwareLogWriter) writer).setForceRecovery(true);
                }
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(1));
                writer.write(DLMTestUtil.getLogRecordInstance(txid++), new PartitionId(0));
                ((BKPartitionAwareLogWriter) writer).setForceRecovery(false);
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
        String name = "distrlog-non-partitioned";
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
    public void testNonPartitionedWritesBulk() throws Exception {
        String name = "distrlog-non-partitioned-bulk";
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            LogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 10; j++) {
                LinkedList<LogRecord> records = new LinkedList<LogRecord>();
                for (int k = 1; k <= 10; k++) {
                    records.add(DLMTestUtil.getLogRecordInstance(txid++));
                }
                writer.writeBulk(records);
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

        assertEquals(1, dlm.getFirstTxId());
        assertEquals(txid - 1, dlm.getLastTxId());

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
        writer.close();
        reader.close();
        dlm.close();
        assertEquals((txid - 1), numTrans);
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
    public void testInterleavedReadersWithRollingEdgeUnPartitioned() throws Exception {
        String name = "distrlog-interleaved-rolling-edge-unpartitioned";
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
    }

    @Test
    public void testRecoveryAPI() throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, "distrlog-recovery-api");
        LogWriter out = dlm.startLogSegmentNonPartitioned();
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                out.setReadyToFlush();
                out.flushAndSync();
            }

        }
        out.setReadyToFlush();
        out.flushAndSync();

        out.close();

        BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);

        assertNull(zkc.exists(blplm1.completedLedgerZNode(1, 100), false));
        assertNotNull(zkc.exists(blplm1.inprogressZNode(1), false));

        dlm.recover();

        assertNotNull(zkc.exists(blplm1.completedLedgerZNode(1, 100), false));
        assertNull(zkc.exists(blplm1.inprogressZNode(1), false));
        blplm1.close();
        dlm.close();
    }

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
}
