package com.twitter.distributedlog.benchmark;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.benchmark.utils.ShiftableRateLimiter;
import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.StatsLogger;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class TwitterBenchmarker extends Benchmarker {

    TwitterBenchmarker(String[] args) {
        super(args);
    }

    @Override
    protected WriterWorker createWriteWorker(String streamPrefix,
                                             int startStreamId,
                                             int endStreamId,
                                             ShiftableRateLimiter rateLimiter,
                                             int writeConcurrency,
                                             int messageSizeBytes,
                                             int batchSize,
                                             int hostConnectionCoreSize,
                                             int hostConnectionLimit,
                                             List<String> serverSetPaths,
                                             List<String> finagleNames,
                                             StatsReceiver statsReceiver,
                                             StatsLogger statsLogger,
                                             boolean thriftmux,
                                             boolean handshakeWithClientInfo,
                                             int sendBufferSize,
                                             int recvBufferSize) {
        return new TwitterWriterWorker(
                streamPrefix,
                startStreamId,
                endStreamId,
                rateLimiter,
                writeConcurrency,
                messageSizeBytes,
                batchSize,
                hostConnectionCoreSize,
                hostConnectionLimit,
                serverSetPaths,
                finagleNames,
                statsReceiver,
                statsLogger,
                thriftmux,
                handshakeWithClientInfo,
                sendBufferSize,
                recvBufferSize);
    }

    @Override
    protected ReaderWorker createReaderWorker(DistributedLogConfiguration conf,
                                              URI uri,
                                              String streamPrefix,
                                              int startStreamId,
                                              int endStreamId,
                                              int readThreadPoolSize,
                                              List<String> serverSetPaths,
                                              List<String> finagleNames,
                                              int truncationIntervalInSeconds,
                                              boolean readFromHead,
                                              StatsReceiver statsReceiver,
                                              StatsLogger statsLogger)
            throws IOException {
        return new TwitterReaderWorker(
                conf,
                uri,
                streamPrefix,
                startStreamId,
                endStreamId,
                readThreadPoolSize,
                serverSetPaths,
                finagleNames,
                truncationIntervalInSeconds,
                readFromHead,
                statsReceiver,
                statsLogger);
    }

    public static void main(String[] args) {
        TwitterBenchmarker benchmarker = new TwitterBenchmarker(args);
        try {
            benchmarker.run();
        } catch (Exception e) {
            logger.info("Benchmark quitted due to : ", e);
        }
    }
}
