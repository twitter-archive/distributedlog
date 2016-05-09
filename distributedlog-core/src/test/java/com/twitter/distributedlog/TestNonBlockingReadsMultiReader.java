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

import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestNonBlockingReadsMultiReader extends TestDistributedLogBase {

        static class ReaderThread extends Thread {

        final LogReader reader;
        final boolean nonBlockReading;
        volatile boolean running = true;
        final AtomicInteger readCount = new AtomicInteger(0);

        ReaderThread(String name, LogReader reader, boolean nonBlockReading) {
            super(name);
            this.reader = reader;
            this.nonBlockReading = nonBlockReading;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    LogRecord r = reader.readNext(nonBlockReading);
                    if (r != null) {
                        readCount.incrementAndGet();
                        if (readCount.get() % 1000 == 0) {
                            LOG.info("{} reading {}", getName(), r.getTransactionId());
                        }
                    }
                } catch (DLInterruptedException die) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    break;
                }
            }
        }

        void stopReading() {
            LOG.info("Stopping reader.");
            running = false;
            interrupt();
            try {
                join();
            } catch (InterruptedException e) {
                LOG.error("Interrupted on waiting reader thread {} exiting : ", getName(), e);
            }
        }

        int getReadCount() {
            return readCount.get();
        }

    }

    @Test(timeout = 60000)
    public void testMultiReaders() throws Exception {
        String name = "distrlog-multireaders";
        final RateLimiter limiter = RateLimiter.create(1000);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);

        DistributedLogManager dlmwrite = createNewDLM(confLocal, name);

        final AsyncLogWriter writer = dlmwrite.startAsyncLogSegmentNonPartitioned();
        FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(0)));
        FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(1)));
        final AtomicInteger writeCount = new AtomicInteger(2);

        DistributedLogManager dlmread = createNewDLM(conf, name);

        BKSyncLogReaderDLSN reader0 = (BKSyncLogReaderDLSN) dlmread.getInputStream(0);

        try {
            ReaderThread[] readerThreads = new ReaderThread[1];
            readerThreads[0] = new ReaderThread("reader0-non-blocking", reader0, false);
            // readerThreads[1] = new ReaderThread("reader1-non-blocking", reader0, false);

            final AtomicBoolean running = new AtomicBoolean(true);
            Thread writerThread = new Thread("WriteThread") {
                @Override
                public void run() {
                    try {
                        long txid = 2;
                        DLSN dlsn = DLSN.InvalidDLSN;
                        while (running.get()) {
                            limiter.acquire();
                            long curTxId = txid++;
                            dlsn = FutureUtils.result(writer.write(DLMTestUtil.getLogRecordInstance(curTxId)));
                            writeCount.incrementAndGet();
                            if (curTxId % 1000 == 0) {
                                LOG.info("writer write {}", curTxId);
                            }
                        }
                        LOG.info("Completed writing record at {}", dlsn);
                        Utils.close(writer);
                    } catch (DLInterruptedException die) {
                        Thread.currentThread().interrupt();
                    } catch (IOException e) {

                    }
                }
            };

            for (ReaderThread rt : readerThreads) {
                rt.start();
            }

            writerThread.start();

            TimeUnit.SECONDS.sleep(5);

            LOG.info("Stopping writer");

            running.set(false);
            writerThread.join();

            LOG.info("Writer stopped after writing {} records, waiting for reader to complete",
                    writeCount.get());
            while (writeCount.get() > (readerThreads[0].getReadCount())) {
                LOG.info("Write Count = {}, Read Count = {}, ReadAhead = {}",
                        new Object[] { writeCount.get(), readerThreads[0].getReadCount(),
                                        reader0.getReadAheadPosition() });
                TimeUnit.MILLISECONDS.sleep(100);
            }
            assertEquals(writeCount.get(),
                    (readerThreads[0].getReadCount()));

            for (ReaderThread readerThread : readerThreads) {
                readerThread.stopReading();
            }
        } finally {
            dlmwrite.close();
            reader0.close();
            dlmread.close();
        }
    }

}
