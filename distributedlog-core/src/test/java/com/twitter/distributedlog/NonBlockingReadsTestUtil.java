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

import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.exceptions.LogReadException;
import com.twitter.distributedlog.util.FutureUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Utils for non blocking reads tests
 */
class NonBlockingReadsTestUtil {

    static final Logger LOG = LoggerFactory.getLogger(NonBlockingReadsTestUtil.class);

    static final long DEFAULT_SEGMENT_SIZE = 1000;

    static void readNonBlocking(DistributedLogManager dlm, boolean forceStall) throws Exception {
        readNonBlocking(dlm, forceStall, DEFAULT_SEGMENT_SIZE, false);
    }

    static void readNonBlocking(DistributedLogManager dlm,
                                boolean forceStall,
                                long segmentSize,
                                boolean waitForIdle) throws Exception {
        BKSyncLogReaderDLSN reader = null;
        try {
            reader = (BKSyncLogReaderDLSN) dlm.getInputStream(1);
        } catch (LogNotFoundException lnfe) {
        }
        while (null == reader) {
            TimeUnit.MILLISECONDS.sleep(20);
            try {
                reader = (BKSyncLogReaderDLSN) dlm.getInputStream(1);
            } catch (LogNotFoundException lnfe) {
            } catch (LogEmptyException lee) {
            } catch (IOException ioe) {
                LOG.error("Failed to open reader reading from {}", dlm.getStreamName());
                throw ioe;
            }
        }
        try {
            LOG.info("Created reader reading from {}", dlm.getStreamName());
            if (forceStall) {
                reader.disableReadAheadZKNotification();
            }

            long numTrans = 0;
            long lastTxId = -1;

            boolean exceptionEncountered = false;
            try {
                while (true) {
                    LogRecordWithDLSN record = reader.readNext(true);
                    if (null != record) {
                        DLMTestUtil.verifyLogRecord(record);
                        assertTrue(lastTxId < record.getTransactionId());
                        assertEquals(record.getTransactionId() - 1, record.getSequenceId());
                        lastTxId = record.getTransactionId();
                        numTrans++;
                        continue;
                    }

                    if (numTrans >= (3 * segmentSize)) {
                        if (waitForIdle) {
                            while (true) {
                                reader.readNext(true);
                                TimeUnit.MILLISECONDS.sleep(10);
                            }
                        }
                        break;
                    }

                    TimeUnit.MILLISECONDS.sleep(2);
                }
            } catch (LogReadException readexc) {
                exceptionEncountered = true;
            } catch (LogNotFoundException exc) {
                exceptionEncountered = true;
            }
            assertFalse(exceptionEncountered);
        } finally {
            reader.close();
        }
    }

    static void writeRecordsForNonBlockingReads(DistributedLogConfiguration conf,
                                         DistributedLogManager dlm,
                                         boolean recover)
            throws Exception {
        writeRecordsForNonBlockingReads(conf, dlm, recover, DEFAULT_SEGMENT_SIZE);
    }

    static void writeRecordsForNonBlockingReads(DistributedLogConfiguration conf,
                                         DistributedLogManager dlm,
                                         boolean recover,
                                         long segmentSize)
            throws Exception {
        long txId = 1;
        for (long i = 0; i < 3; i++) {
            BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
            for (long j = 1; j < segmentSize; j++) {
                FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
            }
            if (recover) {
                FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
                TimeUnit.MILLISECONDS.sleep(300);
                writer.abort();
                LOG.debug("Recovering Segments");
                BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);
                FutureUtils.result(blplm.recoverIncompleteLogSegments());
                FutureUtils.result(blplm.asyncClose());
                LOG.debug("Recovered Segments");
            } else {
                FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(txId++)));
                writer.closeAndComplete();
            }
            TimeUnit.MILLISECONDS.sleep(300);
        }
    }

}
