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
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FutureUtils;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark on {@link com.twitter.distributedlog.AsyncLogReader} reading from a stream
 */
public class AsyncReaderBenchmark extends AbstractReaderBenchmark {

    static final Logger logger = LoggerFactory.getLogger(AsyncReaderBenchmark.class);

    @Override
    protected void benchmark(DistributedLogNamespace namespace, String logName, StatsLogger statsLogger) {
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
        logger.info("Created dlm for stream {}.", streamName);

        // Stats
        OpStatsLogger openReaderStats = statsLogger.getOpStatsLogger("open_reader");
        OpStatsLogger blockingReadStats = statsLogger.getOpStatsLogger("blocking_read");
        Counter readCounter = statsLogger.getCounter("reads");

        AsyncLogReader reader = null;
        DLSN lastDLSN = null;
        Long lastTxId = null;
        while (null == reader) {
            // initialize the last txid
            if (null == lastTxId) {
                switch (readMode) {
                    case OLDEST:
                        lastTxId = 0L;
                        lastDLSN = DLSN.InitialDLSN;
                        break;
                    case LATEST:
                        lastTxId = Long.MAX_VALUE;
                        try {
                            lastDLSN = dlm.getLastDLSN();
                        } catch (IOException ioe) {
                            continue;
                        }
                        break;
                    case REWIND:
                        lastTxId = System.currentTimeMillis() - rewindMs;
                        lastDLSN = null;
                        break;
                    case POSITION:
                        lastTxId = fromTxId;
                        lastDLSN = null;
                        break;
                    default:
                        logger.warn("Unsupported mode {}", readMode);
                        printUsage();
                        System.exit(0);
                        break;
                }
                logger.info("Reading from transaction id = {}, dlsn = {}", lastTxId, lastDLSN);
            }
            // Open the reader
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                if (null == lastDLSN) {
                    reader = FutureUtils.result(dlm.openAsyncLogReader(lastTxId));
                } else {
                    reader = FutureUtils.result(dlm.openAsyncLogReader(lastDLSN));
                }
                long elapsedMs = stopwatch.elapsed(TimeUnit.MICROSECONDS);
                openReaderStats.registerSuccessfulEvent(elapsedMs);
                logger.info("It took {} ms to position the reader to transaction id = {}, dlsn = {}",
                        lastTxId, lastDLSN);
            } catch (IOException ioe) {
                openReaderStats.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                logger.warn("Failed to create reader for stream {} reading from tx id = {}, dlsn = {}.",
                        new Object[] { streamName, lastTxId, lastDLSN });
            }
            if (null == reader) {
                try {
                    TimeUnit.MILLISECONDS.sleep(conf.getZKSessionTimeoutMilliseconds());
                } catch (InterruptedException e) {
                }
                continue;
            }
            List<LogRecordWithDLSN> records;
            stopwatch = Stopwatch.createUnstarted();
            while (true) {
                try {
                    stopwatch.start();
                    records = FutureUtils.result(reader.readBulk(batchSize));
                    long elapsedMicros = stopwatch.stop().elapsed(TimeUnit.MICROSECONDS);
                    blockingReadStats.registerSuccessfulEvent(elapsedMicros);
                    if (!records.isEmpty()) {
                        readCounter.add(records.size());
                        LogRecordWithDLSN lastRecord = records.get(records.size() - 1);
                        lastTxId = lastRecord.getTransactionId();
                        lastDLSN = lastRecord.getDlsn();
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
