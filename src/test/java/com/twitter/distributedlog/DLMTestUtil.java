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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

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

    static void updateBKDLConfig(URI uri, String zkServers, String ledgersPath, boolean sanityCheckTxnID) throws Exception {
        BKDLConfig bkdlConfig = new BKDLConfig(zkServers, ledgersPath).setSanityCheckTxnID(sanityCheckTxnID);
        DLMetadata.create(bkdlConfig).update(uri);
    }

    static URI createDLMURI(String path) throws Exception {
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
                path, p.toString(), conf, createDLMURI("/" + path), null, null, null, null, null,
                NullStatsLogger.INSTANCE, "localhost", DistributedLogConstants.LOCAL_REGION_ID);
    }

    public static void fenceStream(DistributedLogConfiguration conf, URI uri, String name) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(name, conf, uri);
        try {
            BKLogPartitionReadHandler readHandler = dlm.createReadLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
            List<LogSegmentLedgerMetadata> ledgerList = readHandler.getFullLedgerList(true, true);
            LogSegmentLedgerMetadata lastSegment = ledgerList.get(ledgerList.size() - 1);
            LedgerHandle lh = dlm.getBookKeeperClient().get().openLedger(lastSegment.getLedgerId(),
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

    static void verifyLogRecord(LogRecord record) {
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

}
