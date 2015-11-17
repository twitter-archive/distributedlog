package com.twitter.distributedlog.v2.benchmark.stream;

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.v2.DistributedLogConstants;
import com.twitter.distributedlog.v2.DistributedLogManager;
import com.twitter.distributedlog.v2.DistributedLogManagerFactory;
import com.twitter.distributedlog.v2.DistributedLogManagerFactory.ClientSharingOption;
import com.twitter.distributedlog.v2.LogReader;
import com.twitter.distributedlog.LogRecord;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark on {@link com.twitter.distributedlog.LogReader} reading from a stream
 */
public class SyncReaderBenchmark extends StreamBenchmark {

    static final Logger logger = LoggerFactory.getLogger(SyncReaderBenchmark.class);

    long fromTxId = DistributedLogConstants.INVALID_TXID;
    boolean readFromLatest;

    public SyncReaderBenchmark() {
        options.addOption("t", "tx-id", true, "Transaction ID to start read from.");
        options.addOption("l", "latest", true, "Read from latest.");
    }

    @Override
    protected void parseCommandLine(CommandLine cmdline) {
        if (cmdline.hasOption("t")) {
            fromTxId = Long.parseLong(cmdline.getOptionValue("t"));
        }
        if (cmdline.hasOption("l")) {
            readFromLatest = Boolean.parseBoolean(cmdline.getOptionValue("l"));
        } else {
            readFromLatest = false;
        }
        if (readFromLatest) {
            logger.info("Start reading from latest.");
        } else {
            logger.info("Start reading from transaction id {}.", fromTxId);
        }
    }

    @Override
    protected void benchmark(DistributedLogManagerFactory factory, String streamName, StatsLogger statsLogger) {
        DistributedLogManager dlm = null;
        while (null == dlm) {
            try {
                dlm = factory.createDistributedLogManager(streamName, ClientSharingOption.SharedClients);
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
            if (null == lastTxId) {
                if (readFromLatest) {
                    try {
                        lastTxId = dlm.getLastTxId();
                    } catch (IOException e) {
                        continue;
                    }
                    logger.info("Reading from latest last tx id {}", lastTxId);
                } else {
                    lastTxId = fromTxId;
                }
            }

            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                reader = dlm.getInputStream(lastTxId);
                long elapsedMicros = stopwatch.elapsed(TimeUnit.MICROSECONDS);
                openReaderStats.registerSuccessfulEvent(elapsedMicros);
                logger.info("It took {} ms to open reader reading from {}",
                        TimeUnit.MICROSECONDS.toMillis(elapsedMicros), lastTxId);
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
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                        }
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
