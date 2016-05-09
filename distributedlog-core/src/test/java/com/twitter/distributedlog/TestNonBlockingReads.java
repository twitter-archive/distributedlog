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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.twitter.distributedlog.exceptions.IdleReaderException;
import com.twitter.distributedlog.util.FutureUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.twitter.distributedlog.NonBlockingReadsTestUtil.*;
import static org.junit.Assert.*;

public class TestNonBlockingReads extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestNonBlockingReads.class);

    // TODO: investigate why long poll read makes test flaky
    static {
        conf.setOutputBufferSize(0);
        conf.setImmediateFlushEnabled(true);
    }

    @Test(timeout = 60000)
    public void testNonBlockingRead() throws Exception {
        String name = "distrlog-non-blocking-reader";
        final DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxRecords(1);
        confLocal.setReaderIdleWarnThresholdMillis(100);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        try {
            final Thread currentThread = Thread.currentThread();
            executor.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                writeRecordsForNonBlockingReads(confLocal, dlm, false);
                            } catch (Exception exc) {
                                currentThread.interrupt();
                            }

                        }
                    }, 100, TimeUnit.MILLISECONDS);

            readNonBlocking(dlm, false);
            assertFalse(currentThread.isInterrupted());
        } finally {
            executor.shutdown();
            dlm.close();
        }
    }

    @Test(timeout = 60000)
    public void testNonBlockingReadRecovery() throws Exception {
        String name = "distrlog-non-blocking-reader-recovery";
        final DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(10);
        confLocal.setReadAheadMaxRecords(10);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        try {
            final Thread currentThread = Thread.currentThread();
            executor.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                writeRecordsForNonBlockingReads(confLocal, dlm, true);
                            } catch (Exception exc) {
                                currentThread.interrupt();
                            }

                        }
                    }, 100, TimeUnit.MILLISECONDS);


            readNonBlocking(dlm, false);
            assertFalse(currentThread.isInterrupted());
        } finally {
            executor.shutdown();
            dlm.close();
        }
    }

    @Test(timeout = 60000)
    public void testNonBlockingReadIdleError() throws Exception {
        String name = "distrlog-non-blocking-reader-error";
        final DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxRecords(1);
        confLocal.setReaderIdleWarnThresholdMillis(50);
        confLocal.setReaderIdleErrorThresholdMillis(100);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        try {
            final Thread currentThread = Thread.currentThread();
            executor.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                writeRecordsForNonBlockingReads(confLocal, dlm, false);
                            } catch (Exception exc) {
                                currentThread.interrupt();
                            }

                        }
                    }, 100, TimeUnit.MILLISECONDS);

            boolean exceptionEncountered = false;
            try {
                readNonBlocking(dlm, false, DEFAULT_SEGMENT_SIZE, true);
            } catch (IdleReaderException exc) {
                exceptionEncountered = true;
            }
            assertTrue(exceptionEncountered);
            assertFalse(currentThread.isInterrupted());
        } finally {
            executor.shutdown();
            dlm.close();
        }
    }

    @Test(timeout = 60000)
    public void testNonBlockingReadAheadStall() throws Exception {
        String name = "distrlog-non-blocking-reader-stall";
        final DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setReadAheadBatchSize(1);
        confLocal.setReadAheadMaxRecords(3);
        confLocal.setReaderIdleWarnThresholdMillis(500);
        confLocal.setReaderIdleErrorThresholdMillis(30000);
        final DistributedLogManager dlm = createNewDLM(confLocal, name);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        try {
            final Thread currentThread = Thread.currentThread();
            executor.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                writeRecordsForNonBlockingReads(confLocal, dlm, false, 3);
                            } catch (Exception exc) {
                                currentThread.interrupt();
                            }

                        }
                    }, 10, TimeUnit.MILLISECONDS);

            boolean exceptionEncountered = false;
            try {
                readNonBlocking(dlm, false, 3, false);
            } catch (IdleReaderException exc) {
                LOG.info("Exception encountered", exc);
                exceptionEncountered = true;
            }
            assertFalse(exceptionEncountered);
            assertFalse(currentThread.isInterrupted());
        } finally {
            executor.shutdown();
            dlm.close();
        }
    }

    private long createStreamWithInconsistentMetadata(String name) throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, name);
        ZooKeeperClient zkClient = ZooKeeperClientBuilder.newBuilder()
                .zkAclId(null)
                .uri(createDLMURI("/"))
                .sessionTimeoutMs(10000)
                .build();
        long txid = 1;

        long numRecordsWritten = 0;
        int segmentSize = 10;
        for (long i = 0; i < 3; i++) {
            BKAsyncLogWriter out = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                FutureUtils.result(out.write(op));
                numRecordsWritten++;
            }
            out.closeAndComplete();
        }

        BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);
        String completedZNode = blplm.completedLedgerZNode(txid - segmentSize, txid - 1, 3);
        LogSegmentMetadata metadata = FutureUtils.result(LogSegmentMetadata.read(zkClient, completedZNode));
        zkClient.get().delete(completedZNode, -1);
        LogSegmentMetadata metadataToChange =
                metadata.mutator()
                        .setLastEntryId(metadata.getLastEntryId() + 100)
                        .setLastTxId(metadata.getLastTxId() + 100)
                        .build();
        metadataToChange.write(zkClient);

        txid += 100;


        for (long i = 0; i < 3; i++) {
            BKAsyncLogWriter out = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                FutureUtils.result(out.write(op));
                numRecordsWritten++;
            }
            out.closeAndComplete();
        }
        dlm.close();

        return numRecordsWritten;
    }

    @Test(timeout = 60000)
    public void testHandleInconsistentMetadata() throws Exception {
        String name = "distrlog-inconsistent-metadata-blocking-read";
        long numRecordsWritten = createStreamWithInconsistentMetadata(name);

        DistributedLogManager dlm = createNewDLM(conf, name);
        try {
            LogReader reader = dlm.getInputStream(45);
            long numRecordsRead = 0;
            LogRecord record = reader.readNext(false);
            long lastTxId = -1;
            while (numRecordsRead < numRecordsWritten / 2) {
                if (null != record) {
                    DLMTestUtil.verifyLogRecord(record);
                    Assert.assertTrue(lastTxId < record.getTransactionId());
                    lastTxId = record.getTransactionId();
                    numRecordsRead++;
                } else {
                    Thread.sleep(1);
                }
                record = reader.readNext(false);
            }
            reader.close();
            assertEquals(numRecordsWritten / 2, numRecordsRead);
        } finally {
            dlm.close();
        }
    }

    @Test(timeout = 15000)
    public void testHandleInconsistentMetadataNonBlocking() throws Exception {
        String name = "distrlog-inconsistent-metadata-nonblocking-read";
        long numRecordsWritten = createStreamWithInconsistentMetadata(name);

        DistributedLogManager dlm = createNewDLM(conf, name);
        try {
            LogReader reader = dlm.getInputStream(45);
            long numRecordsRead = 0;
            long lastTxId = -1;
            while (numRecordsRead < (numRecordsWritten / 2)) {
                LogRecord record = reader.readNext(false);
                if (record != null) {
                    DLMTestUtil.verifyLogRecord(record);
                    Assert.assertTrue(lastTxId < record.getTransactionId());
                    lastTxId = record.getTransactionId();
                    numRecordsRead++;
                } else {
                    Thread.sleep(1);
                }
            }
            reader.close();
        } finally {
            dlm.close();
        }
    }

    @Test(timeout = 15000)
    public void testHandleInconsistentMetadataDLSNNonBlocking() throws Exception {
        String name = "distrlog-inconsistent-metadata-nonblocking-read-dlsn";
        long numRecordsWritten = createStreamWithInconsistentMetadata(name);

        DistributedLogManager dlm = createNewDLM(conf, name);
        try {
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            long numRecordsRead = 0;
            long lastTxId = -1;
            while (numRecordsRead < numRecordsWritten) {
                LogRecord record = reader.readNext(false);
                if (record != null) {
                    DLMTestUtil.verifyLogRecord(record);
                    Assert.assertTrue(lastTxId < record.getTransactionId());
                    lastTxId = record.getTransactionId();
                    numRecordsRead++;
                } else {
                    Thread.sleep(1);
                }
            }
            reader.close();
        } finally {
            dlm.close();
        }
    }
}
