package com.twitter.distributedlog;

import com.twitter.util.Await;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestTruncate extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestTruncate.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setOutputBufferSize(0).setPeriodicFlushFrequencyMilliSeconds(10);

    static void updateCompletionTime(ZooKeeperClient zkc,
                                     LogSegmentLedgerMetadata l, long completionTime) throws Exception {
        LogSegmentLedgerMetadata newSegment = l.mutator().setCompletionTime(completionTime).build();
        DLMTestUtil.updateSegmentMetadata(zkc, newSegment);
    }


    @Test
    public void testPurgeLogs() throws Exception {
        String name = "distrlog-purge-logs";
        URI uri = DLMTestUtil.createDLMURI("/" + name);
        long txid = 1;
        for (long i = 1; i <= 10; i++) {
            LOG.info("Writing log segment {}.", i);
            DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
            AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 1; j <= 10; j++) {
                long curTxId = txid++;
                Await.result(writer.write(DLMTestUtil.getLogRecordInstance(curTxId)));
            }
            writer.close();
            dlm.close();
        }

        DistributedLogManager distributedLogManager = DLMTestUtil.createNewDLM(conf, name);

        List<LogSegmentLedgerMetadata> segments = distributedLogManager.getLogSegments();
        LOG.info("Segments before modifying completion time : {}", segments);

        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder().zkAclId(null).uri(uri)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .connectionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .build();
        // Update completion time of first 5 segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentLedgerMetadata segment = segments.get(i);
            updateCompletionTime(zkc, segment, i);
        }
        zkc.close();

        segments = distributedLogManager.getLogSegments();
        LOG.info("Segments after modifying completion time : {}", segments);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setRetentionPeriodHours(1);
        confLocal.setSanityCheckDeletes(false);

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        for (int j = 1; j <= 10; j++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
        }

        // to make sure the truncation task is executed
        DLSN lastDLSN = dlm.getLastDLSNAsync().get();
        LOG.info("Get last dlsn of stream {} : {}", name, lastDLSN);

        segments = distributedLogManager.getLogSegments();
        assertEquals(6, segments.size());

        writer.close();
        dlm.close();

        distributedLogManager.close();
    }

    @Test
    public void testTruncation() throws Exception {
        String name = "distrlog-truncation";

        long txid = 1;
        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        Pair<DistributedLogManager, AsyncLogWriter> pair = populateData(txid2DLSN, conf, name);

        Thread.sleep(1000);

        // delete invalid dlsn
        assertFalse(pair.getRight().truncate(DLSN.InvalidDLSN).get());
        verifyEntries(name, 1, 1, 5 * 10);

        for (int i = 1; i <= 4; i++) {
            int txn = (i-1) * 10 + i;
            DLSN dlsn = txid2DLSN.get((long)txn);
            assertTrue(pair.getRight().truncate(dlsn).get());
            verifyEntries(name, 1, (i - 1) * 10 + 1, (5 - i + 1) * 10);
        }

        // Delete higher dlsn
        int txn = 43;
        DLSN dlsn = txid2DLSN.get((long) txn);
        assertTrue(pair.getRight().truncate(dlsn).get());
        verifyEntries(name, 1, 41, 10);

        pair.getRight().close();
        pair.getLeft().close();
    }

    @Test
    public void testExplicitTruncation() throws Exception {
        String name = "distrlog-truncation-explicit";

        DistributedLogConfiguration confLocal = conf.setExplicitTruncationByApplication(true);

        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        Pair<DistributedLogManager, AsyncLogWriter> pair = populateData(txid2DLSN, confLocal, name);

        Thread.sleep(1000);

        for (int i = 1; i <= 4; i++) {
            int txn = (i-1) * 10 + i;
            DLSN dlsn = txid2DLSN.get((long)txn);
            assertTrue(pair.getRight().truncate(dlsn).get());
            verifyEntries(name, 1, (i - 1) * 10 + 1, (5 - i + 1) * 10);
        }

        // Delete higher dlsn
        int txn = 43;
        DLSN dlsn = txid2DLSN.get((long) txn);
        assertTrue(pair.getRight().truncate(dlsn).get());
        verifyEntries(name, 1, 41, 10);

        pair.getRight().close();
        pair.getLeft().close();

        // Try force truncation
        BKDistributedLogManager dlm = (BKDistributedLogManager)DLMTestUtil.createNewDLM(confLocal, name);
        BKLogPartitionWriteHandler handler = dlm.createWriteLedgerHandler(conf.getUnpartitionedStreamName());
        handler.purgeLogsOlderThan(Integer.MAX_VALUE);

        verifyEntries(name, 1, 41, 10);
    }

    private Pair<DistributedLogManager, AsyncLogWriter> populateData(Map<Long, DLSN> txid2DLSN, DistributedLogConfiguration confLocal, String name) throws Exception {
        long txid = 1;
        for (long i = 1; i <= 4; i++) {
            LOG.info("Writing Log Segment {}.", i);
            DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
            AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 1; j <= 10; j++) {
                long curTxId = txid++;
                DLSN dlsn = writer.write(DLMTestUtil.getLogRecordInstance(curTxId)).get();
                txid2DLSN.put(curTxId, dlsn);
            }
            writer.close();
            dlm.close();
        }

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(confLocal, name);
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        for (int j = 1; j <= 10; j++) {
            long curTxId = txid++;
            DLSN dlsn = writer.write(DLMTestUtil.getLogRecordInstance(curTxId)).get();
            txid2DLSN.put(curTxId, dlsn);
        }
        return new ImmutablePair<DistributedLogManager, AsyncLogWriter>(dlm, writer);
    }
    private void verifyEntries(String name, long readFromTxId, long startTxId, int numEntries) throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        LogReader reader = dlm.getInputStream(readFromTxId);

        long txid = startTxId;
        int numRead = 0;
        LogRecord r = reader.readNext(false);
        while (null != r) {
            DLMTestUtil.verifyLogRecord(r);
            LOG.trace("Read entry {}.", r.getTransactionId());
            assertEquals(txid++, r.getTransactionId());
            ++numRead;
            r = reader.readNext(false);
        }
        assertEquals(numEntries, numRead);
        reader.close();
        dlm.close();
    }

}
