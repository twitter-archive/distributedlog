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

import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for setting up bookkeeper ensembles
 * and bringing individual bookies up and down
 */
public class DLMTestUtil {
    protected static final Logger LOG = LoggerFactory.getLogger(DLMTestUtil.class);
    private final static byte[] payloadStatic = repeatString("abc", 512).getBytes();

    static String repeatString(String s, int n) {
        String ret = s;
        for(int i = 1; i < n; i++) {
            ret += s;
        }
        return ret;
    }

    public static Map<Long, LogSegmentLedgerMetadata> readLogSegments(ZooKeeperClient zkc, String ledgerPath) throws Exception {
        List<String> children = zkc.get().getChildren(ledgerPath, false);
        LOG.info("Children under {} : {}", ledgerPath, children);
        Map<Long, LogSegmentLedgerMetadata> segments =
            new HashMap<Long, LogSegmentLedgerMetadata>(children.size());
        for (String child : children) {
            LogSegmentLedgerMetadata segment = LogSegmentLedgerMetadata.read(zkc, ledgerPath + "/" + child,
                DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
            LOG.info("Read segment {} : {}", child, segment);
            segments.put(segment.getLedgerSequenceNumber(), segment);
        }
        return segments;
    }

    static void updateBKDLConfig(URI uri, String zkServers, String ledgersPath, boolean sanityCheckTxnID) throws Exception {
        BKDLConfig bkdlConfig = new BKDLConfig(zkServers, ledgersPath).setSanityCheckTxnID(sanityCheckTxnID);
        DLMetadata.create(bkdlConfig).update(uri);
    }

    public static URI createDLMURI(String path) throws Exception {
        return LocalDLMEmulator.createDLMURI("127.0.0.1:7000", path);
    }

    static BKLogPartitionWriteHandler createNewBKDLM(DistributedLogConfiguration conf,
                                                     String path) throws Exception {
        return createNewBKDLM(new PartitionId(0), conf, path);
    }

    static DistributedLogManager createNewDLM(DistributedLogConfiguration conf,
                                              String name) throws Exception {
        return DistributedLogManagerFactory.createDistributedLogManager(name, conf, createDLMURI("/" + name));
    }

    public static DistributedLogManager createNewDLM(String name, DistributedLogConfiguration conf,
                                                     URI uri) throws Exception {
        return DistributedLogManagerFactory.createDistributedLogManager(name, conf, uri);
    }

    static MetadataAccessor createNewMetadataAccessor(DistributedLogConfiguration conf,
                                              String name) throws Exception {
        return DistributedLogManagerFactory.createMetadataAccessor(name, createDLMURI("/" + name), conf);
    }

    static BKLogPartitionWriteHandler createNewBKDLM(PartitionId p,
                                                     DistributedLogConfiguration conf, String path) throws Exception {
        return BKLogPartitionWriteHandler.createBKLogPartitionWriteHandler(
                path, p.toString(), conf, createDLMURI("/" + path), null, null, null, null, null, null,
                NullStatsLogger.INSTANCE, "localhost", DistributedLogConstants.LOCAL_REGION_ID);
    }

    public static void fenceStream(DistributedLogConfiguration conf, URI uri, String name) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(name, conf, uri);
        try {
            BKLogPartitionReadHandler readHandler = dlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
            List<LogSegmentLedgerMetadata> ledgerList = readHandler.getFullLedgerList(true, true);
            LogSegmentLedgerMetadata lastSegment = ledgerList.get(ledgerList.size() - 1);
            BookKeeperClient bkc = dlm.getWriterBKCBuilder().build();
            try {
                LedgerHandle lh = bkc.get().openLedger(lastSegment.getLedgerId(),
                        BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
                lh.close();
            } finally {
                bkc.release();
            }
        } finally {
            dlm.close();
        }
    }

    static long getNumberofLogRecords(DistributedLogManager bkdlm, PartitionId partition, long startTxId) throws IOException {
        long numLogRecs = 0;
        LogReader reader = bkdlm.getInputStream(partition, startTxId);
        LogRecord record = reader.readNext(false);
        while (null != record) {
            numLogRecs++;
            verifyLogRecord(record);
            record = reader.readNext(false);
        }
        reader.close();
        return numLogRecs;
    }

    static long getNumberofLogRecords(DistributedLogManager bkdlm, long startTxId) throws IOException {
        long numLogRecs = 0;
        LogReader reader = bkdlm.getInputStream(startTxId);
        LogRecord record = reader.readNext(false);
        while (null != record) {
            numLogRecs++;
            verifyLogRecord(record);
            record = reader.readNext(false);
        }
        reader.close();
        return numLogRecs;
    }

    static LogRecord getLogRecordInstance(long txId) {
        return new LogRecord(txId, generatePayload(txId));
    }

    static LogRecord getLogRecordInstance(long txId, int size) {
        ByteBuffer buf = ByteBuffer.allocate(size);
        return new LogRecord(txId, buf.array());
    }

    public static void verifyLogRecord(LogRecord record) {
        assertEquals(generatePayload(record.getTransactionId()).length, record.getPayload().length);
        assertArrayEquals(generatePayload(record.getTransactionId()), record.getPayload());
        assert(!record.isControl());
        verifyPayload(record.getTransactionId(), record.getPayload());
    }

    static byte[] generatePayload(long txId) {
        return String.format("%d;%d", txId, txId).getBytes();
    }

    static void verifyPayload(long txId, byte[] payload) {
        String[] txIds = new String(payload).split(";");
        assertEquals(Long.valueOf(txIds[0]), Long.valueOf(txIds[0]));
    }

    static LogRecord getLargeLogRecordInstance(long txId) {
        return new LogRecord(txId, payloadStatic);
    }

    static List<LogRecord> getLargeLogRecordInstanceList(long firstTxId, int count) {
        List<LogRecord> logrecs = new ArrayList<LogRecord>(count);
        for (long i = 0; i < count; i++) {
            logrecs.add(getLargeLogRecordInstance(firstTxId + i));
        }
        return logrecs;
    }

    static List<LogRecord> getLogRecordInstanceList(long firstTxId, int count, int size) {
        List<LogRecord> logrecs = new ArrayList<LogRecord>(count);
        for (long i = 0; i < count; i++) {
            logrecs.add(getLogRecordInstance(firstTxId + i, size));
        }
        return logrecs;
    }

    static void verifyLargeLogRecord(LogRecord record) {
        verifyLargeLogRecord(record.getPayload());
    }

    static void verifyLargeLogRecord(byte[] payload) {
        assertArrayEquals(payloadStatic, payload);
    }

    static LogRecord getEmptyLogRecordInstance(long txId) {
        return new LogRecord(txId, new byte[0]);
    }

    static void verifyEmptyLogRecord(LogRecord record) {
        assert(record.getPayload().length == 0);
    }

    public static String inprogressZNodeName(long ledgerSeqNo) {
        return String.format("%s_%018d", DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX, ledgerSeqNo);
    }

    public static String completedLedgerZNodeNameWithVersion(long ledgerId, long firstTxId, long lastTxId, long ledgerSeqNo) {
        return String.format("%s_%018d_%018d_%018d_v%dl%d_%04d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                             firstTxId, lastTxId, ledgerSeqNo, DistributedLogConstants.LOGSEGMENT_NAME_VERSION, ledgerId,
                             DistributedLogConstants.LOCAL_REGION_ID);
    }

    public static String completedLedgerZNodeNameWithTxID(long firstTxId, long lastTxId) {
        return String.format("%s_%018d_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, firstTxId, lastTxId);
    }

    public static String completedLedgerZNodeNameWithLedgerSequenceNumber(long ledgerSeqNo) {
        return String.format("%s_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, ledgerSeqNo);
    }

    public static LogSegmentLedgerMetadata inprogressLogSegment(String ledgerPath, long ledgerId, long firstTxId, long ledgerSeqNo) {
        return new LogSegmentLedgerMetadata(ledgerPath + "/" + inprogressZNodeName(ledgerSeqNo),
                                            DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION,
                                            ledgerId, firstTxId, ledgerSeqNo, DistributedLogConstants.LOCAL_REGION_ID,
                                            DistributedLogConstants.LOGSEGMENT_DEFAULT_STATUS);
    }

    public static LogSegmentLedgerMetadata completedLogSegment(String ledgerPath, long ledgerId, long firstTxId,
                                                               long lastTxId, int recordCount, long ledgerSeqNo,
                                                               long lastEntryId, long lastSlotId) {
        LogSegmentLedgerMetadata metadata =
                new LogSegmentLedgerMetadata(ledgerPath + "/" + completedLedgerZNodeNameWithLedgerSequenceNumber(ledgerSeqNo),
                                             DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION,
                                             ledgerId, firstTxId, ledgerSeqNo, DistributedLogConstants.LOCAL_REGION_ID,
                                             DistributedLogConstants.LOGSEGMENT_DEFAULT_STATUS);
        metadata.finalizeLedger(lastTxId, recordCount, lastEntryId, lastSlotId);
        return metadata;
    }

    public static void generateCompletedLogSegments(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                    long numCompletedSegments, long segmentSize) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        long txid = 1L;
        for (long i = 0; i < numCompletedSegments; i++) {
            BKUnPartitionedSyncLogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }
    }

    public static void injectLogSegmentWithGivenLedgerSeqNo(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                            long ledgerSeqNo, long startTxID, boolean writeEntries, long segmentSize,
                                                            boolean completeLogSegment)
            throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        BKLogPartitionWriteHandler writeHandler = dlm.createWriteLedgerHandler(conf.getUnpartitionedStreamName());
        // Start a log segment with a given ledger seq number.
        writeHandler.lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        writeHandler.startLogSegmentCount.incrementAndGet();
        BookKeeperClient bkc = dlm.getWriterBKCBuilder().build();
        try {
            LedgerHandle lh = bkc.get().createLedger(conf.getEnsembleSize(), conf.getWriteQuorumSize(),
                    conf.getAckQuorumSize(), BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes());
            String inprogressZnodeName = writeHandler.inprogressZNodeName(lh.getId(), startTxID, ledgerSeqNo);
            String znodePath = writeHandler.inprogressZNode(lh.getId(), startTxID, ledgerSeqNo);
            LogSegmentLedgerMetadata l = new LogSegmentLedgerMetadata(znodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), startTxID, ledgerSeqNo,
                    DistributedLogConstants.LOCAL_REGION_ID, DistributedLogConstants.LOGSEGMENT_DEFAULT_STATUS);
            l.write(dlm.writerZKC, znodePath);
            writeHandler.maxTxId.store(startTxID);
            writeHandler.addLogSegmentToCache(inprogressZnodeName, l);
            BKPerStreamLogWriter writer = new BKPerStreamLogWriter(writeHandler.getFullyQualifiedName(), inprogressZnodeName,
                    conf, lh, writeHandler.lock, startTxID, ledgerSeqNo, writeHandler.executorService,
                    writeHandler.orderedFuturePool, writeHandler.statsLogger);
            if (writeEntries) {
                long txid = startTxID;
                for (long j = 1; j <= segmentSize; j++) {
                    writer.write(DLMTestUtil.getLogRecordInstance(txid++));
                }
                writer.setReadyToFlush();
                writer.flushAndSync();
            }
            if (completeLogSegment) {
                writeHandler.completeAndCloseLogSegment(writer);
            }
        } finally {
            bkc.release();
        }
    }

}
