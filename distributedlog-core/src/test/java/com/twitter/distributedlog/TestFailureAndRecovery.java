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

import com.twitter.distributedlog.util.FutureUtils;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFailureAndRecovery extends TestDistributedLogBase {
    static final Log LOG = LogFactory.getLog(TestFailureAndRecovery.class);

    @Test(timeout = 60000)
    public void testSimpleRecovery() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = createNewBKDLM(conf, "distrlog-simplerecovery");
        BKLogSegmentWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                FutureUtils.result(out.flushAndCommit());
            }

        }
        FutureUtils.result(out.flushAndCommit());

        out.abort();
        FutureUtils.result(out.close());

        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(1, 100, out.getLogSegmentSequenceNumber()), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(out.getLogSegmentId(), 1, out.getLogSegmentSequenceNumber()), false));

        FutureUtils.result(bkdlmAndClients.getWriteHandler().recoverIncompleteLogSegments());

        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(1, 100, out.getLogSegmentSequenceNumber()), false));
        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(out.getLogSegmentId(), 1, out.getLogSegmentSequenceNumber()), false));
    }

    /**
     * Test that if enough bookies fail to prevent an ensemble,
     * writes the bookkeeper will fail. Test that when once again
     * an ensemble is available, it can continue to write.
     */
    @Test(timeout = 60000)
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
            DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = createNewBKDLM(conf, "distrlog-allbookiefailure");
            BKLogSegmentWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(txid);

            for (long i = 1; i <= 3; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            FutureUtils.result(out.flushAndCommit());
            bookieToFail.shutdown();
            assertEquals("New bookie didn't die",
                numBookies, bkutil.checkBookiesUp(numBookies, 10));

            try {
                for (long i = 1; i <= 3; i++) {
                    LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                    out.write(op);
                    txid++;
                }
                FutureUtils.result(out.flushAndCommit());
                fail("should not get to this stage");
            } catch (BKTransmitException bkte) {
                LOG.debug("Error writing to bookkeeper", bkte);
                assertEquals("Invalid exception message",
                        BKException.Code.NotEnoughBookiesException, bkte.getBKResultCode());
            }
            replacementBookie = bkutil.newBookie();

            assertEquals("Replacement: New bookie didn't start",
                numBookies + 1, bkutil.checkBookiesUp(numBookies + 1, 10));
            out = bkdlmAndClients.getWriteHandler().startLogSegment(txid);
            for (long i = 1; i <= 3; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }

            FutureUtils.result(out.flushAndCommit());
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
    @Test(timeout = 60000)
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
            DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = createNewBKDLM(conf, "distrlog-onebookiefailure");
            BKLogSegmentWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(txid);
            for (long i = 1; i <= 3; i++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            FutureUtils.result(out.flushAndCommit());

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
            FutureUtils.result(out.flushAndCommit());
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

    @Test(timeout = 60000)
    public void testRecoveryEmptyLedger() throws Exception {
        DLMTestUtil.BKLogPartitionWriteHandlerAndClients bkdlmAndClients = createNewBKDLM(conf, "distrlog-recovery-empty-ledger");
        BKLogSegmentWriter out = bkdlmAndClients.getWriteHandler().startLogSegment(1);
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                FutureUtils.result(out.flushAndCommit());
            }

        }
        FutureUtils.result(out.flushAndCommit());
        FutureUtils.result(out.close());
        bkdlmAndClients.getWriteHandler().completeAndCloseLogSegment(out.getLogSegmentSequenceNumber(), out.getLogSegmentId(), 1, 100, 100);
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(1, 100, out.getLogSegmentSequenceNumber()), false));
        BKLogSegmentWriter outEmpty = bkdlmAndClients.getWriteHandler().startLogSegment(101);
        outEmpty.abort();

        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(101, 101, outEmpty.getLogSegmentSequenceNumber()), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(outEmpty.getLogSegmentId(), 101, outEmpty.getLogSegmentSequenceNumber()), false));

        FutureUtils.result(bkdlmAndClients.getWriteHandler().recoverIncompleteLogSegments());

        assertNull(zkc.exists(bkdlmAndClients.getWriteHandler().inprogressZNode(outEmpty.getLogSegmentId(), outEmpty.getLogSegmentSequenceNumber(), 101), false));
        assertNotNull(zkc.exists(bkdlmAndClients.getWriteHandler().completedLedgerZNode(101, 101, outEmpty.getLogSegmentSequenceNumber()), false));
    }

    @Test(timeout = 60000)
    public void testRecoveryAPI() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, "distrlog-recovery-api");
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        long txid = 1;
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
            if ((i % 10) == 0) {
                out.setReadyToFlush();
                out.flushAndSync();
            }

        }
        BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
        out.setReadyToFlush();
        out.flushAndSync();

        out.abort();

        BKLogWriteHandler blplm1 = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);

        assertNull(zkc.exists(blplm1.completedLedgerZNode(1, 100,
                                                          perStreamLogWriter.getLogSegmentSequenceNumber()), false));
        assertNotNull(zkc.exists(blplm1.inprogressZNode(perStreamLogWriter.getLogSegmentId(), 1,
                                                        perStreamLogWriter.getLogSegmentSequenceNumber()), false));

        dlm.recover();

        assertNotNull(zkc.exists(blplm1.completedLedgerZNode(1, 100,
                                                             perStreamLogWriter.getLogSegmentSequenceNumber()), false));
        assertNull(zkc.exists(blplm1.inprogressZNode(perStreamLogWriter.getLogSegmentId(), 1,
                                                     perStreamLogWriter.getLogSegmentSequenceNumber()), false));
        blplm1.close();
        assertEquals(100, dlm.getLogRecordCount());
        dlm.close();
    }
}
