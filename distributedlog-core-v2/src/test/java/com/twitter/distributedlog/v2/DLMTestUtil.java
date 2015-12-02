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
package com.twitter.distributedlog.v2;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.MetadataAccessor;
import com.twitter.distributedlog.util.PermitLimiter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executors;

import static com.twitter.distributedlog.DLSNUtil.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Utility class for setting up bookkeeper ensembles
 * and bringing individual bookies up and down
 */
public class DLMTestUtil {
    protected static final Log LOG = LogFactory.getLog(DLMTestUtil.class);
    private final static byte[] payloadStatic = repeatString("abc", 512).getBytes();

    static String repeatString(String s, int n) {
        String ret = s;
        for(int i = 1; i < n; i++) {
            ret += s;
        }
        return ret;
    }

    static URI createDLMURI(String path) throws Exception {
        return LocalDLMEmulator.createDLMURI("127.0.0.1:7000", path);
    }

    static BKLogPartitionWriteHandler createNewBKDLM(DistributedLogConfiguration conf,
                                                     String path) throws Exception {
        return createNewBKDLM(new PartitionId(0), conf, path);
    }

    static BKDistributedLogManager createNewDLM(DistributedLogConfiguration conf,
                                              String name) throws Exception {
        return (BKDistributedLogManager) DistributedLogManagerFactory.createDistributedLogManager(name, conf, createDLMURI("/" + name));
    }

    static MetadataAccessor createNewMetadataAccessor(DistributedLogConfiguration conf,
                                                      String name) throws Exception {
        return DistributedLogManagerFactory.createMetadataAccessor(name, createDLMURI("/" + name), conf);
    }

    static BKLogPartitionWriteHandler createNewBKDLM(PartitionId p,
                                                     DistributedLogConfiguration conf, String path) throws Exception {
        return new BKLogPartitionWriteHandler(
                path, p.toString(), conf, createDLMURI("/" + path), null, null,
                Executors.newScheduledThreadPool(1,
                        new ThreadFactoryBuilder().setNameFormat("Test-BKDL-" + p.toString() + "-executor-%d").build()),
                OrderedSafeExecutor.newBuilder().name("LockStateThread").numThreads(1).build(),
                NullStatsLogger.INSTANCE, "localhost", PermitLimiter.NULL_PERMIT_LIMITER);
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

    public static LogRecordWithDLSN getLogRecordWithDLSNInstance(DLSN dlsn, long txId) {
        return getLogRecordWithDLSNInstance(dlsn, txId, false);
    }

    public static LogRecordWithDLSN getLogRecordWithDLSNInstance(DLSN dlsn, long txId, boolean isControlRecord) {
        LogRecordWithDLSN record = new LogRecordWithDLSN(dlsn, txId, generatePayload(txId), Long.MIN_VALUE);
        if (isControlRecord) {
            setControlRecord(record);
        }
        return record;
    }

    public static void generateCompletedLogSegments(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                    long numCompletedSegments, long segmentSize) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        long txid = 1L;
        for (long i = 0; i < numCompletedSegments; i++) {
            BKUnPartitionedSyncLogWriter writer = (BKUnPartitionedSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }
    }

}
