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

import com.twitter.distributedlog.impl.BKLogSegmentEntryWriter;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

    public static Map<Long, LogSegmentMetadata> readLogSegments(ZooKeeperClient zkc, String ledgerPath) throws Exception {
        List<String> children = zkc.get().getChildren(ledgerPath, false);
        LOG.info("Children under {} : {}", ledgerPath, children);
        Map<Long, LogSegmentMetadata> segments =
            new HashMap<Long, LogSegmentMetadata>(children.size());
        for (String child : children) {
            LogSegmentMetadata segment =
                    FutureUtils.result(LogSegmentMetadata.read(zkc, ledgerPath + "/" + child));
            LOG.info("Read segment {} : {}", child, segment);
            segments.put(segment.getLogSegmentSequenceNumber(), segment);
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

    public static DistributedLogManager createNewDLM(String name,
                                                     DistributedLogConfiguration conf,
                                                     URI uri) throws Exception {
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();
        return namespace.openLog(name);
    }

    static MetadataAccessor createNewMetadataAccessor(DistributedLogConfiguration conf,
                                                      String name,
                                                      URI uri) throws Exception {
        // TODO: Metadata Accessor seems to be a legacy object which only used by kestrel
        //       (we might consider deprecating this)
        BKDistributedLogNamespace namespace = BKDistributedLogNamespace.newBuilder()
                .conf(conf).uri(uri).build();
        return namespace.createMetadataAccessor(name);
    }

    public static class BKLogPartitionWriteHandlerAndClients {
        private BKLogWriteHandler writeHandler;
        private ZooKeeperClient zooKeeperClient;
        private BookKeeperClient bookKeeperClient;

        public BKLogPartitionWriteHandlerAndClients(BKLogWriteHandler writeHandler, ZooKeeperClient zooKeeperClient, BookKeeperClient bookKeeperClient) {
            this.writeHandler = writeHandler;
            this.zooKeeperClient = zooKeeperClient;
            this.bookKeeperClient = bookKeeperClient;
        }

        public void close() {
            bookKeeperClient.close();
            zooKeeperClient.close();
            writeHandler.close();
        }

        public BKLogWriteHandler getWriteHandler() {
            return writeHandler;
        }
    }

    static BKLogPartitionWriteHandlerAndClients createNewBKDLM(DistributedLogConfiguration conf,
                                                               String logName,
                                                               int zkPort) throws Exception {
        URI uri = createDLMURI(zkPort, "/" + logName);

        ZooKeeperClientBuilder zkcBuilder = ZooKeeperClientBuilder.newBuilder()
            .name(String.format("dlzk:%s:handler_dedicated", logName))
            .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
            .uri(uri)
            .statsLogger(NullStatsLogger.INSTANCE.scope("dlzk_handler_dedicated"))
            .retryThreadCount(conf.getZKClientNumberRetryThreads())
            .requestRateLimit(conf.getZKRequestRateLimit())
            .zkAclId(conf.getZkAclId());

        ZooKeeperClient zkClient = zkcBuilder.build();

        try {
            zkClient.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            // ignore
        }

        // resolve uri
        BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkClient, uri);
        BKDLConfig.propagateConfiguration(bkdlConfig, conf);
        BookKeeperClientBuilder bkcBuilder = BookKeeperClientBuilder.newBuilder()
            .dlConfig(conf)
            .name(String.format("bk:%s:handler_dedicated", logName))
            .zkServers(bkdlConfig.getBkZkServersForWriter())
            .ledgersPath(bkdlConfig.getBkLedgersPath())
            .statsLogger(NullStatsLogger.INSTANCE);

        BKDistributedLogManager bkdlm = new BKDistributedLogManager(
                logName,
                conf,
                uri,
                zkcBuilder,
                zkcBuilder,
                zkClient,
                zkClient,
                bkcBuilder,
                bkcBuilder,
                new SettableFeatureProvider("", 0),
                PermitLimiter.NULL_PERMIT_LIMITER,
                NullStatsLogger.INSTANCE);

        BKLogWriteHandler writeHandler = bkdlm.createWriteHandler(true);
        return new BKLogPartitionWriteHandlerAndClients(writeHandler, zkClient, bkcBuilder.build());
    }

    public static void fenceStream(DistributedLogConfiguration conf, URI uri, String name) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(name, conf, uri);
        try {
            BKLogReadHandler readHandler = dlm.createReadHandler();
            List<LogSegmentMetadata> ledgerList = readHandler.getFullLedgerList(true, true);
            LogSegmentMetadata lastSegment = ledgerList.get(ledgerList.size() - 1);
            BookKeeperClient bkc = dlm.getWriterBKC();
            LedgerHandle lh = bkc.get().openLedger(lastSegment.getLedgerId(),
                    BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
            lh.close();
        } finally {
            dlm.close();
        }
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

    public static LogRecord getLogRecordInstance(long txId) {
        return new LogRecord(txId, generatePayload(txId));
    }

    public static LogRecord getLogRecordInstance(long txId, int size) {
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
        return getLogRecordWithDLSNInstance(dlsn, txId, false);
    }

    public static LogRecordWithDLSN getLogRecordWithDLSNInstance(DLSN dlsn, long txId, boolean isControlRecord) {
        LogRecordWithDLSN record = new LogRecordWithDLSN(dlsn, txId, generatePayload(txId), 1L);
        record.setPositionWithinLogSegment((int)txId + 1);
        if (isControlRecord) {
            record.setControl();
        }
        return record;
    }

    public static String inprogressZNodeName(long logSegmentSeqNo) {
        return String.format("%s_%018d", DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX, logSegmentSeqNo);
    }

    public static String completedLedgerZNodeNameWithVersion(long ledgerId, long firstTxId, long lastTxId, long logSegmentSeqNo) {
        return String.format("%s_%018d_%018d_%018d_v%dl%d_%04d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                             firstTxId, lastTxId, logSegmentSeqNo, DistributedLogConstants.LOGSEGMENT_NAME_VERSION, ledgerId,
                             DistributedLogConstants.LOCAL_REGION_ID);
    }

    public static String completedLedgerZNodeNameWithTxID(long firstTxId, long lastTxId) {
        return String.format("%s_%018d_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, firstTxId, lastTxId);
    }

    public static String completedLedgerZNodeNameWithLogSegmentSequenceNumber(long logSegmentSeqNo) {
        return String.format("%s_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, logSegmentSeqNo);
    }

    public static LogSegmentMetadata inprogressLogSegment(String ledgerPath,
                                                          long ledgerId,
                                                          long firstTxId,
                                                          long logSegmentSeqNo) {
        return inprogressLogSegment(ledgerPath, ledgerId, firstTxId, logSegmentSeqNo,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    public static LogSegmentMetadata inprogressLogSegment(String ledgerPath,
                                                          long ledgerId,
                                                          long firstTxId,
                                                          long logSegmentSeqNo,
                                                          int version) {
        return new LogSegmentMetadata.LogSegmentMetadataBuilder(
                    ledgerPath + "/" + inprogressZNodeName(logSegmentSeqNo),
                    version,
                    ledgerId,
                    firstTxId)
                .setLogSegmentSequenceNo(logSegmentSeqNo)
                .build();
    }

    public static LogSegmentMetadata completedLogSegment(String ledgerPath,
                                                         long ledgerId,
                                                         long firstTxId,
                                                         long lastTxId,
                                                         int recordCount,
                                                         long logSegmentSeqNo,
                                                         long lastEntryId,
                                                         long lastSlotId) {
        return completedLogSegment(ledgerPath, ledgerId, firstTxId, lastTxId,
                recordCount, logSegmentSeqNo, lastEntryId, lastSlotId,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    public static LogSegmentMetadata completedLogSegment(String ledgerPath,
                                                         long ledgerId,
                                                         long firstTxId,
                                                         long lastTxId,
                                                         int recordCount,
                                                         long logSegmentSeqNo,
                                                         long lastEntryId,
                                                         long lastSlotId,
                                                         int version) {
        LogSegmentMetadata metadata =
                new LogSegmentMetadata.LogSegmentMetadataBuilder(
                        ledgerPath + "/" + inprogressZNodeName(logSegmentSeqNo),
                        version,
                        ledgerId,
                        firstTxId)
                    .setInprogress(false)
                    .setLogSegmentSequenceNo(logSegmentSeqNo)
                    .build();
        return metadata.completeLogSegment(ledgerPath + "/" + completedLedgerZNodeNameWithLogSegmentSequenceNumber(logSegmentSeqNo),
                lastTxId, recordCount, lastEntryId, lastSlotId, firstTxId);
    }

    public static void generateCompletedLogSegments(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                    long numCompletedSegments, long segmentSize) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        long txid = 1L;
        for (long i = 0; i < numCompletedSegments; i++) {
            BKSyncLogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }
    }

    public static long generateLogSegmentNonPartitioned(DistributedLogManager dlm, int controlEntries, int userEntries, long startTxid)
            throws Exception {
        return generateLogSegmentNonPartitioned(dlm, controlEntries, userEntries, startTxid, 1L);
    }

    public static long generateLogSegmentNonPartitioned(DistributedLogManager dlm, int controlEntries, int userEntries, long startTxid, long txidStep) throws Exception {
        AsyncLogWriter out = dlm.startAsyncLogSegmentNonPartitioned();
        long txid = startTxid;
        for (int i = 0; i < controlEntries; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            record.setControl();
            Await.result(out.write(record));
            txid += txidStep;
        }
        for (int i = 0; i < userEntries; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            Await.result(out.write(record));
            txid += txidStep;
        }
        out.close();
        return txid - startTxid;
    }

    public static void injectLogSegmentWithGivenLogSegmentSeqNo(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                                long logSegmentSeqNo, long startTxID, boolean writeEntries, long segmentSize,
                                                                boolean completeLogSegment)
            throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        BKLogWriteHandler writeHandler = dlm.createWriteHandler(false);
        FutureUtils.result(writeHandler.lockHandler());
        // Start a log segment with a given ledger seq number.
        BookKeeperClient bkc = dlm.getWriterBKC();
        LedgerHandle lh = bkc.get().createLedger(conf.getEnsembleSize(), conf.getWriteQuorumSize(),
                conf.getAckQuorumSize(), BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes());
        String inprogressZnodeName = writeHandler.inprogressZNodeName(lh.getId(), startTxID, logSegmentSeqNo);
        String znodePath = writeHandler.inprogressZNode(lh.getId(), startTxID, logSegmentSeqNo);
        LogSegmentMetadata l =
            new LogSegmentMetadata.LogSegmentMetadataBuilder(znodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), startTxID)
                .setLogSegmentSequenceNo(logSegmentSeqNo)
                .build();
        l.write(dlm.writerZKC);
        writeHandler.maxTxId.store(startTxID);
        writeHandler.addLogSegmentToCache(inprogressZnodeName, l);
        BKLogSegmentWriter writer = new BKLogSegmentWriter(
                writeHandler.getFullyQualifiedName(),
                inprogressZnodeName,
                conf,
                conf.getDLLedgerMetadataLayoutVersion(),
                new BKLogSegmentEntryWriter(lh),
                writeHandler.lock,
                startTxID,
                logSegmentSeqNo,
                writeHandler.scheduler,
                writeHandler.orderedFuturePool,
                writeHandler.statsLogger,
                writeHandler.statsLogger,
                writeHandler.alertStatsLogger,
                PermitLimiter.NULL_PERMIT_LIMITER,
                new SettableFeatureProvider("", 0),
                ConfUtils.getConstDynConf(conf));
        if (writeEntries) {
            long txid = startTxID;
            for (long j = 1; j <= segmentSize; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            FutureUtils.result(writer.flushAndCommit());
        }
        if (completeLogSegment) {
            writeHandler.completeAndCloseLogSegment(writer);
        }
        FutureUtils.result(writeHandler.unlockHandler());
    }

    public static void injectLogSegmentWithLastDLSN(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                    long logSegmentSeqNo, long startTxID, long segmentSize,
                                                    boolean recordWrongLastDLSN) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        BKLogWriteHandler writeHandler = dlm.createWriteHandler(false);
        FutureUtils.result(writeHandler.lockHandler());
        // Start a log segment with a given ledger seq number.
        BookKeeperClient bkc = dlm.getReaderBKC();
        LedgerHandle lh = bkc.get().createLedger(conf.getEnsembleSize(), conf.getWriteQuorumSize(),
                conf.getAckQuorumSize(), BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes());
        String inprogressZnodeName = writeHandler.inprogressZNodeName(lh.getId(), startTxID, logSegmentSeqNo);
        String znodePath = writeHandler.inprogressZNode(lh.getId(), startTxID, logSegmentSeqNo);
        LogSegmentMetadata l =
            new LogSegmentMetadata.LogSegmentMetadataBuilder(znodePath,
                conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), startTxID)
            .setLogSegmentSequenceNo(logSegmentSeqNo)
            .setInprogress(false)
            .build();
        l.write(dlm.writerZKC);
        writeHandler.maxTxId.store(startTxID);
        writeHandler.addLogSegmentToCache(inprogressZnodeName, l);
        BKLogSegmentWriter writer = new BKLogSegmentWriter(
                writeHandler.getFullyQualifiedName(),
                inprogressZnodeName,
                conf,
                conf.getDLLedgerMetadataLayoutVersion(),
                new BKLogSegmentEntryWriter(lh),
                writeHandler.lock,
                startTxID,
                logSegmentSeqNo,
                writeHandler.scheduler,
                writeHandler.orderedFuturePool,
                writeHandler.statsLogger,
                writeHandler.statsLogger,
                writeHandler.alertStatsLogger,
                PermitLimiter.NULL_PERMIT_LIMITER,
                new SettableFeatureProvider("", 0),
                ConfUtils.getConstDynConf(conf));
        long txid = startTxID;
        DLSN wrongDLSN = null;
        for (long j = 1; j <= segmentSize; j++) {
            DLSN dlsn = Await.result(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(txid++)));
            if (j == (segmentSize - 1)) {
                wrongDLSN = dlsn;
            }
        }
        assertNotNull(wrongDLSN);
        if (recordWrongLastDLSN) {
            FutureUtils.result(writer.close());
            writeHandler.completeAndCloseLogSegment(
                    writeHandler.inprogressZNodeName(writer.getLogSegmentId(), writer.getStartTxId(), writer.getLogSegmentSequenceNumber()),
                    writer.getLogSegmentSequenceNumber(),
                    writer.getLogSegmentId(),
                    writer.getStartTxId(),
                    startTxID + segmentSize - 2,
                    writer.getPositionWithinLogSegment() - 1,
                    wrongDLSN.getEntryId(),
                    wrongDLSN.getSlotId());
        } else {
            writeHandler.completeAndCloseLogSegment(writer);
        }
        FutureUtils.result(writeHandler.unlockHandler());
    }

    public static void updateSegmentMetadata(ZooKeeperClient zkc, LogSegmentMetadata segment) throws Exception {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        zkc.get().setData(segment.getZkPath(), finalisedData, -1);
    }

    public static ServerConfiguration loadTestBkConf() {
        ServerConfiguration conf = new ServerConfiguration();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL confUrl = classLoader.getResource("bk_server.conf");
        try {
            if (null != confUrl) {
                conf.loadConf(confUrl);
                LOG.info("loaded bk_server.conf from resources");
            }
        } catch (org.apache.commons.configuration.ConfigurationException ex) {
            LOG.warn("loading conf failed", ex);
        }
        return conf;
    }

    public static <T> void validateFutureFailed(Future<T> future, Class exClass) {
        try {
            Await.result(future);
        } catch (Exception ex) {
            LOG.info("Expected: {} Actual: {}", exClass.getName(), ex.getClass().getName());
            assertTrue("exceptions types equal", exClass.isInstance(ex));
        }
    }

    public static <T> T validateFutureSucceededAndGetResult(Future<T> future) throws Exception {
        try {
            return Await.result(future, Duration.fromSeconds(10));
        } catch (Exception ex) {
            fail("unexpected exception " + ex.getClass().getName());
            throw ex;
        }
    }
}
