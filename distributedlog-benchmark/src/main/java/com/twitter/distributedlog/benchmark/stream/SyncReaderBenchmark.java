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
package com.twitter.distributedlog.benchmark.stream;

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark on {@link com.twitter.distributedlog.LogReader} reading from a stream
 */
public class SyncReaderBenchmark extends AbstractReaderBenchmark {

    public SyncReaderBenchmark() {}

    @Override
    protected void benchmark(DistributedLogNamespace namespace, String streamName, StatsLogger statsLogger) {
        DistributedLogManager dlm = null;
        while (null == dlm) {
            try {
                dlm = namespace.openLog(streamName);
            } catch (IOException ioe) {
                logger.warn("Failed to create dlm for stream {} : ", streamName, ioe);
            }
            if (null == dlm) {
                try {
                    TimeUnit.MILLISECONDS.sleep(conf.getZKSessionTimeoutMilliseconds());
                } catch (InterruptedException e) {
                }
            }
        }
        OpStatsLogger openReaderStats = statsLogger.getOpStatsLogger("open_reader");
        OpStatsLogger nonBlockingReadStats = statsLogger.getOpStatsLogger("non_blocking_read");
        OpStatsLogger blockingReadStats = statsLogger.getOpStatsLogger("blocking_read");
        Counter nullReadCounter = statsLogger.getCounter("null_read");

        logger.info("Created dlm for stream {}.", streamName);
        LogReader reader = null;
        Long lastTxId = null;
        while (null == reader) {
            // initialize the last txid
            if (null == lastTxId) {
                switch (readMode) {
                    case OLDEST:
                        lastTxId = 0L;
                        break;
                    case LATEST:
                        try {
                            lastTxId = dlm.getLastTxId();
                        } catch (IOException ioe) {
                            continue;
                        }
                        break;
                    case REWIND:
                        lastTxId = System.currentTimeMillis() - rewindMs;
                        break;
                    case POSITION:
                        lastTxId = fromTxId;
                        break;
                    default:
                        logger.warn("Unsupported mode {}", readMode);
                        printUsage();
                        System.exit(0);
                        break;
                }
                logger.info("Reading from transaction id {}", lastTxId);
            }
            // Open the reader
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                reader = dlm.getInputStream(lastTxId);
                long elapsedMs = stopwatch.elapsed(TimeUnit.MICROSECONDS);
                openReaderStats.registerSuccessfulEvent(elapsedMs);
                logger.info("It took {} ms to position the reader to transaction id {}", lastTxId);
            } catch (IOException ioe) {
                openReaderStats.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                logger.warn("Failed to create reader for stream {} reading from {}.", streamName, lastTxId);
            }
            if (null == reader) {
                try {
                    TimeUnit.MILLISECONDS.sleep(conf.getZKSessionTimeoutMilliseconds());
                } catch (InterruptedException e) {
                }
                continue;
            }

            // read loop

            LogRecord record;
            boolean nonBlocking = false;
            stopwatch = Stopwatch.createUnstarted();
            while (true) {
                try {
                    stopwatch.start();
                    record = reader.readNext(nonBlocking);
                    if (null != record) {
                        long elapsedMicros = stopwatch.stop().elapsed(TimeUnit.MICROSECONDS);
                        if (nonBlocking) {
                            nonBlockingReadStats.registerSuccessfulEvent(elapsedMicros);
                        } else {
                            blockingReadStats.registerSuccessfulEvent(elapsedMicros);
                        }
                        lastTxId = record.getTransactionId();
                    } else {
                        nullReadCounter.inc();
                    }
                    if (null == record && !nonBlocking) {
                        nonBlocking = true;
                    }
                    stopwatch.reset();
                } catch (IOException e) {
                    logger.warn("Encountered reading record from stream {} : ", streamName, e);
                    reader = null;
                    break;
                }
            }
            try {
                TimeUnit.MILLISECONDS.sleep(conf.getZKSessionTimeoutMilliseconds());
            } catch (InterruptedException e) {
            }
        }
    }
}
