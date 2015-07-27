package com.twitter.distributedlog;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.LogSegmentMetadata.TruncationStatus;
import com.twitter.util.Await;

import static org.junit.Assert.*;

public class TestTruncate extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestTruncate.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setOutputBufferSize(0).setPeriodicFlushFrequencyMilliSeconds(10);

    static void updateCompletionTime(ZooKeeperClient zkc,
                                     LogSegmentMetadata l, long completionTime) throws Exception {
        LogSegmentMetadata newSegment = l.mutator().setCompletionTime(completionTime).build();
        DLMTestUtil.updateSegmentMetadata(zkc, newSegment);
    }

    static void setTruncationStatus(ZooKeeperClient zkc,
                                    LogSegmentMetadata l,
                                    TruncationStatus status) throws Exception {
        LogSegmentMetadata newSegment =
                l.mutator().setTruncationStatus(status).build();
        DLMTestUtil.updateSegmentMetadata(zkc, newSegment);
    }

    @Test(timeout = 60000)
    public void testPurgeLogs() throws Exception {
        String name = "distrlog-purge-logs";
        URI uri = createDLMURI("/" + name);

        populateData(new HashMap<Long, DLSN>(), conf, name, 10, 10, false);

        DistributedLogManager distributedLogManager = createNewDLM(conf, name);

        List<LogSegmentMetadata> segments = distributedLogManager.getLogSegments();
        LOG.info("Segments before modifying completion time : {}", segments);

        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder().zkAclId(null).uri(uri)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .connectionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .build();

        // Update completion time of first 5 segments
        long newTimeMs = System.currentTimeMillis() - 60*60*1000*2;
        for (int i = 0; i < 5; i++) {
            LogSegmentMetadata segment = segments.get(i);
            updateCompletionTime(zkc, segment, newTimeMs + i);
        }
        zkc.close();

        segments = distributedLogManager.getLogSegments();
        LOG.info("Segments after modifying completion time : {}", segments);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setRetentionPeriodHours(1);
        confLocal.setExplicitTruncationByApplication(false);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        long txid = 1 + 10 * 10;
        for (int j = 1; j <= 10; j++) {
            Await.result(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
        }

        // to make sure the truncation task is executed
        DLSN lastDLSN = Await.result(dlm.getLastDLSNAsync());
        LOG.info("Get last dlsn of stream {} : {}", name, lastDLSN);

        assertEquals(6, distributedLogManager.getLogSegments().size());

        writer.close();
        dlm.close();

        distributedLogManager.close();
    }

    @Test
    public void testTruncation() throws Exception {
        String name = "distrlog-truncation";

        long txid = 1;
        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        Pair<DistributedLogManager, AsyncLogWriter> pair =
                populateData(txid2DLSN, conf, name, 4, 10, true);

        Thread.sleep(1000);

        // delete invalid dlsn
        assertFalse(Await.result(pair.getRight().truncate(DLSN.InvalidDLSN)));
        verifyEntries(name, 1, 1, 5 * 10);

        for (int i = 1; i <= 4; i++) {
            int txn = (i-1) * 10 + i;
            DLSN dlsn = txid2DLSN.get((long)txn);
            assertTrue(Await.result(pair.getRight().truncate(dlsn)));
            verifyEntries(name, 1, (i - 1) * 10 + 1, (5 - i + 1) * 10);
        }

        // Delete higher dlsn
        int txn = 43;
        DLSN dlsn = txid2DLSN.get((long) txn);
        assertTrue(Await.result(pair.getRight().truncate(dlsn)));
        verifyEntries(name, 1, 31, 20);

        pair.getRight().close();
        pair.getLeft().close();
    }

    @Test
    public void testExplicitTruncation() throws Exception {
        String name = "distrlog-truncation-explicit";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setExplicitTruncationByApplication(true);

        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        Pair<DistributedLogManager, AsyncLogWriter> pair =
                populateData(txid2DLSN, confLocal, name, 4, 10, true);

        Thread.sleep(1000);

        for (int i = 1; i <= 4; i++) {
            int txn = (i-1) * 10 + i;
            DLSN dlsn = txid2DLSN.get((long)txn);
            assertTrue(Await.result(pair.getRight().truncate(dlsn)));
            verifyEntries(name, 1, (i - 1) * 10 + 1, (5 - i + 1) * 10);
        }

        // Delete higher dlsn
        int txn = 43;
        DLSN dlsn = txid2DLSN.get((long) txn);
        assertTrue(Await.result(pair.getRight().truncate(dlsn)));
        verifyEntries(name, 1, 31, 20);

        pair.getRight().close();
        pair.getLeft().close();

        // Try force truncation
        BKDistributedLogManager dlm = (BKDistributedLogManager)createNewDLM(confLocal, name);
        BKLogPartitionWriteHandler handler = dlm.createWriteLedgerHandler(conf.getUnpartitionedStreamName());
        handler.purgeLogsOlderThan(Integer.MAX_VALUE);

        verifyEntries(name, 1, 31, 20);
    }

    @Test(timeout = 60000)
    public void testOnlyPurgeSegmentsBeforeNoneFullyTruncatedSegment() throws Exception {
        String name = "distrlog-only-purge-segments-before-none-fully-truncated-segment";
        URI uri = createDLMURI("/" + name);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setExplicitTruncationByApplication(true);

        // populate data
        populateData(new HashMap<Long, DLSN>(), confLocal, name, 4, 10, false);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        LOG.info("Segments before modifying segment status : {}", segments);

        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .zkAclId(null)
                .uri(uri)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .connectionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .build();
        setTruncationStatus(zkc, segments.get(0), TruncationStatus.PARTIALLY_TRUNCATED);
        for (int i = 1; i < 4; i++) {
            LogSegmentMetadata segment = segments.get(i);
            setTruncationStatus(zkc, segment, TruncationStatus.TRUNCATED);
        }
        List<LogSegmentMetadata> segmentsAfterTruncated = dlm.getLogSegments();

        dlm.purgeLogsOlderThan(999999);
        List<LogSegmentMetadata> newSegments = dlm.getLogSegments();
        LOG.info("Segments after purge segments older than 999999 : {}", newSegments);
        assertArrayEquals(segmentsAfterTruncated.toArray(new LogSegmentMetadata[segmentsAfterTruncated.size()]),
                          newSegments.toArray(new LogSegmentMetadata[newSegments.size()]));

        dlm.close();

        // Update completion time of all 4 segments
        long newTimeMs = System.currentTimeMillis() - 60 * 60 * 1000 * 10;
        for (int i = 0; i < 4; i++) {
            LogSegmentMetadata segment = newSegments.get(i);
            updateCompletionTime(zkc, segment, newTimeMs + i);
        }

        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(confLocal);
        newConf.setRetentionPeriodHours(1);

        DistributedLogManager newDLM = createNewDLM(newConf, name);
        AsyncLogWriter newWriter = newDLM.startAsyncLogSegmentNonPartitioned();
        long txid = 1 + 4 * 10;
        for (int j = 1; j <= 10; j++) {
            Await.result(newWriter.write(DLMTestUtil.getLogRecordInstance(txid++)));
        }

        // to make sure the truncation task is executed
        DLSN lastDLSN = Await.result(newDLM.getLastDLSNAsync());
        LOG.info("Get last dlsn of stream {} : {}", name, lastDLSN);

        assertEquals(5, newDLM.getLogSegments().size());

        newWriter.close();
        newDLM.close();

        zkc.close();
    }

    @Test(timeout = 60000)
    public void testPartiallyTruncateTruncatedSegments() throws Exception {
        String name = "distrlog-partially-truncate-truncated-segments";
        URI uri = createDLMURI("/" + name);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setExplicitTruncationByApplication(true);

        // populate
        Map<Long, DLSN> dlsnMap = new HashMap<Long, DLSN>();
        populateData(dlsnMap, confLocal, name, 4, 10, false);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        LOG.info("Segments before modifying segment status : {}", segments);

        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .zkAclId(null)
                .uri(uri)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .connectionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .build();
        for (int i = 0; i < 4; i++) {
            LogSegmentMetadata segment = segments.get(i);
            setTruncationStatus(zkc, segment, TruncationStatus.TRUNCATED);
        }

        List<LogSegmentMetadata> newSegments = dlm.getLogSegments();
        LOG.info("Segments after changing truncation status : {}", newSegments);

        dlm.close();

        DistributedLogManager newDLM = createNewDLM(confLocal, name);
        AsyncLogWriter newWriter = newDLM.startAsyncLogSegmentNonPartitioned();
        Await.result(newWriter.truncate(dlsnMap.get(15L)));

        List<LogSegmentMetadata> newSegments2 = newDLM.getLogSegments();
        assertArrayEquals(newSegments.toArray(new LogSegmentMetadata[4]),
                          newSegments2.toArray(new LogSegmentMetadata[4]));

        newWriter.close();
        newDLM.close();
        zkc.close();
    }

    private Pair<DistributedLogManager, AsyncLogWriter> populateData(
            Map<Long, DLSN> txid2DLSN, DistributedLogConfiguration confLocal,
            String name, int numLogSegments, int numEntriesPerLogSegment,
            boolean createInprogressLogSegment) throws Exception {
        long txid = 1;
        for (long i = 1; i <= numLogSegments; i++) {
            LOG.info("Writing Log Segment {}.", i);
            DistributedLogManager dlm = createNewDLM(confLocal, name);
            AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 1; j <= numEntriesPerLogSegment; j++) {
                long curTxId = txid++;
                DLSN dlsn = Await.result(writer.write(DLMTestUtil.getLogRecordInstance(curTxId)));
                txid2DLSN.put(curTxId, dlsn);
            }
            writer.close();
            dlm.close();
        }

        if (createInprogressLogSegment) {
            DistributedLogManager dlm = createNewDLM(confLocal, name);
            AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 1; j <= 10; j++) {
                long curTxId = txid++;
                DLSN dlsn = Await.result(writer.write(DLMTestUtil.getLogRecordInstance(curTxId)));
                txid2DLSN.put(curTxId, dlsn);
            }
            return new ImmutablePair<DistributedLogManager, AsyncLogWriter>(dlm, writer);
        } else {
            return null;
        }
    }

    private void verifyEntries(String name, long readFromTxId, long startTxId, int numEntries) throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, name);
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
