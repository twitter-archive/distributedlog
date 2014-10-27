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

import java.io.IOException;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFailureAndRecovery extends TestDistributedLogBase {
    static final Log LOG = LogFactory.getLog(TestFailureAndRecovery.class);

    @Test
    public void testSimpleRecovery() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = DLMTestUtil.createNewBKDLM(conf, "distrlog-simplerecovery");
        BKPerStreamLogWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
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


        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(out.getLedgerHandle().getId(), 1, 100, out.getLedgerSequenceNumber()), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(out.getLedgerHandle().getId(), 1, out.getLedgerSequenceNumber()), false));

        bkdlmAndClients.getWriteHandler().recoverIncompleteLogSegments();

        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(out.getLedgerHandle().getId(), 1, 100, out.getLedgerSequenceNumber()), false));
        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(out.getLedgerHandle().getId(), 1, out.getLedgerSequenceNumber()), false));
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
            conf.setWriteQuorumSize(ensembleSize);
            conf.setAckQuorumSize(ensembleSize);
            long txid = 1;
            DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = DLMTestUtil.createNewBKDLM(conf, "distrlog-allbookiefailure");
            BKPerStreamLogWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(txid);

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
            out = bkdlmAndClients.getWriteHandler().startLogSegment(txid);
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
            conf.setWriteQuorumSize(ensembleSize);
            conf.setAckQuorumSize(ensembleSize);
            long txid = 1;
            DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = DLMTestUtil.createNewBKDLM(conf, "distrlog-onebookiefailure");
            BKPerStreamLogWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(txid);
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
    public void testRecoveryEmptyLedger() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = DLMTestUtil.createNewBKDLM(conf, "distrlog-recovery-empty-ledger");
        BKPerStreamLogWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
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
        bkdlmAndClients.getWriteHandler().completeAndCloseLogSegment(out.getLedgerSequenceNumber(), out.getLedgerHandle().getId(), 1, 100, 100);
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(out.getLedgerHandle().getId(), 1, 100, out.getLedgerSequenceNumber()), false));
        BKPerStreamLogWriter outEmpty = bkdlmAndClients.getWriteHandler().startLogSegment(101);
        outEmpty.abort();

        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(outEmpty.getLedgerHandle().getId(), 101, 101, outEmpty.getLedgerSequenceNumber()), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(outEmpty.getLedgerHandle().getId(), 101, outEmpty.getLedgerSequenceNumber()), false));

        bkdlmAndClients.getWriteHandler().recoverIncompleteLogSegments();

        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(outEmpty.getLedgerHandle().getId(), outEmpty.getLedgerSequenceNumber(), 101), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(outEmpty.getLedgerHandle().getId(), 101, 101, outEmpty.getLedgerSequenceNumber()), false));
    }

    @Test
    public void testRecoveryAPI() throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, "distrlog-recovery-api");
        BKUnPartitionedSyncLogWriter out = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                out.setReadyToFlush();
                out.flushAndSync();
            }

        }
        BKPerStreamLogWriter perStreamLogWriter = out.getCachedLogWriter(conf.getUnpartitionedStreamName());
        out.setReadyToFlush();
        out.flushAndSync();

        out.abort();

        BKLogPartitionWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteLedgerHandler(conf.getUnpartitionedStreamName());

        assertNull(zkc.exists(blplm1.completedLedgerZNode(perStreamLogWriter.getLedgerHandle().getId(), 1, 100,
                                                          perStreamLogWriter.getLedgerSequenceNumber()), false));
        assertNotNull(zkc.exists(blplm1.inprogressZNode(perStreamLogWriter.getLedgerHandle().getId(), 1,
                                                        perStreamLogWriter.getLedgerSequenceNumber()), false));

        dlm.recover();

        assertNotNull(zkc.exists(blplm1.completedLedgerZNode(perStreamLogWriter.getLedgerHandle().getId(), 1, 100,
                                                             perStreamLogWriter.getLedgerSequenceNumber()), false));
        assertNull(zkc.exists(blplm1.inprogressZNode(perStreamLogWriter.getLedgerHandle().getId(), 1,
                                                     perStreamLogWriter.getLedgerSequenceNumber()), false));
        blplm1.close();
        assertEquals(100, dlm.getLogRecordCount());
        dlm.close();
    }
}
