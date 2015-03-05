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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.twitter.distributedlog.stats.AlertStatsLogger;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.util.Await;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.net.BindException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertNotNull;

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
                LogSegmentLedgerMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
            LOG.info("Read segment {} : {}", child, segment);
            segments.put(segment.getLedgerSequenceNumber(), segment);
        }
        return segments;
    }

    static void updateBKDLConfig(URI uri, String zkServers, String ledgersPath, boolean sanityCheckTxnID) throws Exception {
        BKDLConfig bkdlConfig = new BKDLConfig(zkServers, ledgersPath).setSanityCheckTxnID(sanityCheckTxnID);
        DLMetadata.create(bkdlConfig).update(uri);
    }

    public static URI createDLMURI(int port, String path) throws Exception {
        return LocalDLMEmulator.createDLMURI("127.0.0.1:" + port, path);
    }

    static BKLogPartitionWriteHandlerAndClients createNewBKDLM(DistributedLogConfiguration conf,
                                                               String path, int port) throws Exception {
        return createNewBKDLM(new PartitionId(0), conf, path, port);
    }

    public static DistributedLogManager createNewDLM(String name, DistributedLogConfiguration conf,
                                                     URI uri) throws Exception {
        return DistributedLogManagerFactory.createDistributedLogManager(name, conf, uri);
    }

    static MetadataAccessor createNewMetadataAccessor(DistributedLogConfiguration conf,
                                                      String name, URI uri) throws Exception {
        return DistributedLogManagerFactory.createMetadataAccessor(name, uri, conf);
    }

    public static class BKLogPartitionWriteHandlerAndClients {
        private BKLogPartitionWriteHandler writeHandler;
        private ZooKeeperClient zooKeeperClient;
        private BookKeeperClient bookKeeperClient;

        public BKLogPartitionWriteHandlerAndClients(BKLogPartitionWriteHandler writeHandler, ZooKeeperClient zooKeeperClient, BookKeeperClient bookKeeperClient) {
            this.writeHandler = writeHandler;
            this.zooKeeperClient = zooKeeperClient;
            this.bookKeeperClient = bookKeeperClient;
        }

        public void close() {
            bookKeeperClient.close();
            zooKeeperClient.close();
            writeHandler.close();
        }

        public BKLogPartitionWriteHandler getWriteHandler() {
            return writeHandler;
        }
    }

    static BKLogPartitionWriteHandlerAndClients createNewBKDLM(PartitionId p,
                                                               DistributedLogConfiguration conf,
                                                               String path,
                                                               int zkPort) throws Exception {
        String name = path;
        URI uri = createDLMURI(zkPort, "/" + path);

        ZooKeeperClientBuilder zkcBuilder = ZooKeeperClientBuilder.newBuilder()
            .name(String.format("dlzk:%s:handler_dedicated", name))
            .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
            .uri(uri)
            .statsLogger(NullStatsLogger.INSTANCE.scope("dlzk_handler_dedicated"))
            .retryThreadCount(conf.getZKClientNumberRetryThreads())
            .requestRateLimit(conf.getZKRequestRateLimit())
            .zkAclId(conf.getZkAclId());

        ZooKeeperClient zkClient = zkcBuilder.build();
        // resolve uri
        BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkClient, uri);
        BKDLConfig.propagateConfiguration(bkdlConfig, conf);
        BookKeeperClientBuilder bkcBuilder = BookKeeperClientBuilder.newBuilder()
            .dlConfig(conf)
            .name(String.format("bk:%s:handler_dedicated", name))
            .zkServers(bkdlConfig.getBkZkServersForWriter())
            .ledgersPath(bkdlConfig.getBkLedgersPath())
            .statsLogger(NullStatsLogger.INSTANCE);

        BKLogPartitionWriteHandler writeHandler = BKLogPartitionWriteHandler.createBKLogPartitionWriteHandler(name,
            p.toString(),
            conf,
            uri,
            zkcBuilder,
            bkcBuilder,
            Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("Test-BKDL-" + p.toString() + "-executor-%d").build()),
            null,
            null,
            OrderedSafeExecutor.newBuilder().name("LockStateThread").numThreads(1).build(),
            null,
            NullStatsLogger.INSTANCE,
            new AlertStatsLogger(NullStatsLogger.INSTANCE, "alert"),
            "localhost",
            DistributedLogConstants.LOCAL_REGION_ID,
            PermitLimiter.NULL_PERMIT_LIMITER);

        return new BKLogPartitionWriteHandlerAndClients(writeHandler, zkClient, bkcBuilder.build());
    }

    public static void fenceStream(DistributedLogConfiguration conf, URI uri, String name) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(name, conf, uri);
        try {
            BKLogPartitionReadHandler readHandler = dlm.createReadLedgerHandler(conf.getUnpartitionedStreamName());
            List<LogSegmentLedgerMetadata> ledgerList = readHandler.getFullLedgerList(true, true);
            LogSegmentLedgerMetadata lastSegment = ledgerList.get(ledgerList.size() - 1);
            BookKeeperClient bkc = dlm.getWriterBKC();
            LedgerHandle lh = bkc.get().openLedger(lastSegment.getLedgerId(),
                    BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
            lh.close();
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

    static LogRecord getLargeLogRecordInstance(long txId, boolean control) {
        LogRecord record = new LogRecord(txId, payloadStatic);
        if (control) {
            record.setControl();
        }
        return record;
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

    public static LogRecordWithDLSN getLogRecordWithDLSNInstance(DLSN dlsn, long txId) {
        LogRecordWithDLSN record = new LogRecordWithDLSN(dlsn, txId, generatePayload(txId));
        record.setPositionWithinLogSegment((int)txId + 1);
        return record;
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
        return new LogSegmentLedgerMetadata.LogSegmentLedgerMetadataBuilder(
                    ledgerPath + "/" + inprogressZNodeName(ledgerSeqNo),
                    LogSegmentLedgerMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION,
                    ledgerId, firstTxId)
                .setLedgerSequenceNo(ledgerSeqNo)
                .build();
    }

    public static LogSegmentLedgerMetadata completedLogSegment(String ledgerPath, long ledgerId, long firstTxId,
                                                               long lastTxId, int recordCount, long ledgerSeqNo,
                                                               long lastEntryId, long lastSlotId) {
        LogSegmentLedgerMetadata metadata =
                new LogSegmentLedgerMetadata.LogSegmentLedgerMetadataBuilder(
                        ledgerPath + "/" + completedLedgerZNodeNameWithLedgerSequenceNumber(ledgerSeqNo),
                        LogSegmentLedgerMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION,
                        ledgerId, firstTxId)
                    .setInprogress(false)
                    .setLedgerSequenceNo(ledgerSeqNo)
                    .build();
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

    public static long generateLogSegmentNonPartitioned(DistributedLogManager dlm, int controlEntries, int userEntries, long startTxid) throws Exception {
        AsyncLogWriter out = dlm.startAsyncLogSegmentNonPartitioned();
        long txid = startTxid;
        for (int i = 0; i < controlEntries; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            record.setControl();
            Await.result(out.write(record));
            ++txid;
        }
        for (int i = 0; i < userEntries; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            Await.result(out.write(record));
            ++txid;
        }
        out.close();
        return txid - startTxid;
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
        BookKeeperClient bkc = dlm.getWriterBKC();
        LedgerHandle lh = bkc.get().createLedger(conf.getEnsembleSize(), conf.getWriteQuorumSize(),
                conf.getAckQuorumSize(), BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes());
        String inprogressZnodeName = writeHandler.inprogressZNodeName(lh.getId(), startTxID, ledgerSeqNo);
        String znodePath = writeHandler.inprogressZNode(lh.getId(), startTxID, ledgerSeqNo);
        LogSegmentLedgerMetadata l =
            new LogSegmentLedgerMetadata.LogSegmentLedgerMetadataBuilder(znodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), startTxID)
                .setLedgerSequenceNo(ledgerSeqNo)
                .build();
        l.write(dlm.writerZKC, znodePath);
        writeHandler.maxTxId.store(startTxID);
        writeHandler.addLogSegmentToCache(inprogressZnodeName, l);
        BKPerStreamLogWriter writer = new BKPerStreamLogWriter(writeHandler.getFullyQualifiedName(), inprogressZnodeName,
                conf, conf.getDLLedgerMetadataLayoutVersion(), lh, writeHandler.lock, startTxID, ledgerSeqNo, writeHandler.executorService,
                writeHandler.orderedFuturePool, writeHandler.statsLogger, writeHandler.alertStatsLogger, PermitLimiter.NULL_PERMIT_LIMITER);
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
        } else {
            writer.getLock().release(DistributedReentrantLock.LockReason.PERSTREAMWRITER);
            writeHandler.lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        }
    }

    public static void injectLogSegmentWithLastDLSN(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                    long ledgerSeqNo, long startTxID, long segmentSize,
                                                    boolean recordWrongLastDLSN) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        BKLogPartitionWriteHandler writeHandler = dlm.createWriteLedgerHandler(conf.getUnpartitionedStreamName());
        // Start a log segment with a given ledger seq number.
        writeHandler.lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        writeHandler.startLogSegmentCount.incrementAndGet();
        BookKeeperClient bkc = dlm.getReaderBKC();
        LedgerHandle lh = bkc.get().createLedger(conf.getEnsembleSize(), conf.getWriteQuorumSize(),
                conf.getAckQuorumSize(), BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes());
        String inprogressZnodeName = writeHandler.inprogressZNodeName(lh.getId(), startTxID, ledgerSeqNo);
        String znodePath = writeHandler.inprogressZNode(lh.getId(), startTxID, ledgerSeqNo);
        LogSegmentLedgerMetadata l =
            new LogSegmentLedgerMetadata.LogSegmentLedgerMetadataBuilder(znodePath,
                conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), startTxID)
            .setLedgerSequenceNo(ledgerSeqNo)
            .setInprogress(false)
            .build();
        l.write(dlm.writerZKC, znodePath);
        writeHandler.maxTxId.store(startTxID);
        writeHandler.addLogSegmentToCache(inprogressZnodeName, l);
        BKPerStreamLogWriter writer = new BKPerStreamLogWriter(writeHandler.getFullyQualifiedName(), inprogressZnodeName,
                conf, conf.getDLLedgerMetadataLayoutVersion(), lh, writeHandler.lock, startTxID, ledgerSeqNo, writeHandler.executorService,
                writeHandler.orderedFuturePool, writeHandler.statsLogger, writeHandler.alertStatsLogger, PermitLimiter.NULL_PERMIT_LIMITER);
        long txid = startTxID;
        DLSN wrongDLSN = null;
        for (long j = 1; j <= segmentSize; j++) {
            DLSN dlsn = writer.asyncWrite(DLMTestUtil.getLogRecordInstance(txid++)).get();
            if (j == (segmentSize - 1)) {
                wrongDLSN = dlsn;
            }
        }
        assertNotNull(wrongDLSN);
        if (recordWrongLastDLSN) {
            writer.closeToFinalize();
            writeHandler.completeAndCloseLogSegment(
                    writeHandler.inprogressZNodeName(writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLedgerSequenceNumber()),
                    writer.getLedgerSequenceNumber(), writer.getLedgerHandle().getId(), writer.getStartTxId(), startTxID + segmentSize - 2,
                    writer.getPositionWithinLogSegment() - 1, wrongDLSN.getEntryId(), wrongDLSN.getSlotId(), true);
        } else {
            writeHandler.completeAndCloseLogSegment(writer);
        }
    }

    public static void updateSegmentMetadata(ZooKeeperClient zkc, LogSegmentLedgerMetadata segment) throws Exception {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        zkc.get().setData(segment.getZkPath(), finalisedData, -1);
    }

    /**
     * Log process stdout.
     */
    private static void logOpenSockets() throws Exception {
        final String LIST_CONNS_COMMAND = "lsof -P -n -i TCP";
        Process p = Runtime.getRuntime().exec(LIST_CONNS_COMMAND);
        p.waitFor();
        BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String s = null;
        while ((s = stdout.readLine()) != null) {
            LOG.info(s);
        }
    }

    /**
     * Try to start zookkeeper locally on any port.
     */
    public static Pair<ZooKeeperServerShim, Integer> runZookeeperOnAnyPort(File zkDir) throws Exception {
        return runZookeeperOnAnyPort((int) Math.random()*10000+7000, zkDir);
    }

    /**
     * Try to start zookkeeper locally on any port beginning with some base port.
     * Dump some socket info when bind fails.
     */
    public static Pair<ZooKeeperServerShim, Integer> runZookeeperOnAnyPort(int basePort, File zkDir) throws Exception {

        final int MAX_RETRIES = 20;
        final int MIN_PORT = 1025;
        final int MAX_PORT = 65535;
        ZooKeeperServerShim zks = null;
        int zkPort = basePort;
        boolean success = false;
        int retries = 0;

        while (!success) {
            try {
                LOG.info("zk trying to bind to port " + zkPort);
                zks = LocalBookKeeper.runZookeeper(1000, zkPort, zkDir);
                success = true;
            } catch (BindException be) {
                logOpenSockets();
                retries++;
                if (retries > MAX_RETRIES) {
                    throw be;
                }
                zkPort++;
                if (zkPort > MAX_PORT) {
                    zkPort = MIN_PORT;
                }
            }
        }

        return Pair.of(zks, zkPort);
    }
}
