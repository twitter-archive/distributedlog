package com.twitter.distributedlog.benchmark.stream;

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Benchmark ledger reading
 */
public class LedgerReadBenchmark extends AbstractReaderBenchmark {

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

        List<LogSegmentMetadata> segments = null;
        while (null == segments) {
            try {
                segments = dlm.getLogSegments();
            } catch (IOException ioe) {
                logger.warn("Failed to get log segments for stream {} : ", streamName, ioe);
            }
            if (null == segments) {
                try {
                    TimeUnit.MILLISECONDS.sleep(conf.getZKSessionTimeoutMilliseconds());
                } catch (InterruptedException e) {
                }
            }
        }

        final Counter readCounter = statsLogger.getCounter("reads");

        logger.info("Reading from log segments : {}", segments);

        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .uri(uri)
                .name("benchmark-zkc")
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .zkAclId(null)
                .build();
        BKDLConfig bkdlConfig;
        try {
            bkdlConfig = BKDLConfig.resolveDLConfig(zkc, uri);
        } catch (IOException e) {
            return;
        }

        BookKeeper bk;
        try {
            bk = BookKeeperClientBuilder.newBuilder()
                    .name("benchmark-bkc")
                    .dlConfig(conf)
                    .zkServers(bkdlConfig.getBkZkServersForReader())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .build()
                    .get();
        } catch (IOException e) {
            return;
        }

        final int readConcurrency = conf.getInt("ledger_read_concurrency", 1000);
        boolean streamRead = conf.getBoolean("ledger_stream_read", true);
        try {
            for (LogSegmentMetadata segment : segments) {
                Stopwatch stopwatch = Stopwatch.createStarted();
                long lid = segment.getLedgerId();
                LedgerHandle lh = bk.openLedgerNoRecovery(
                        lid, BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
                logger.info("It took {} ms to open log segment {}",
                        new Object[] { stopwatch.elapsed(TimeUnit.MILLISECONDS), (lh.getLastAddConfirmed() + 1), segment });
                stopwatch.reset().start();
                Runnable reader;
                if (streamRead) {
                    reader = new LedgerStreamReader(lh, new BookkeeperInternalCallbacks.ReadEntryListener() {
                        @Override
                        public void onEntryComplete(int rc, LedgerHandle lh, LedgerEntry entry, Object ctx) {
                            readCounter.inc();
                        }
                    }, readConcurrency);
                } else {
                    reader = new LedgerStreamReader(lh, new BookkeeperInternalCallbacks.ReadEntryListener() {
                        @Override
                        public void onEntryComplete(int rc, LedgerHandle lh, LedgerEntry entry, Object ctx) {
                            readCounter.inc();
                        }
                    }, readConcurrency);
                }
                reader.run();
                logger.info("It took {} ms to complete reading {} entries from log segment {}",
                        new Object[] { stopwatch.elapsed(TimeUnit.MILLISECONDS), (lh.getLastAddConfirmed() + 1), segment });
            }
        } catch (Exception e) {
            logger.error("Error on reading bk ", e);
        }
    }
}
